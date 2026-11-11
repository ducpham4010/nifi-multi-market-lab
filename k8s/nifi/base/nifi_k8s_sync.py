#!/usr/bin/env python3
"""In-cluster configuration sync for one NiFi instance.

  params    Vault KV v1 secret (8 keys)            -> Parameter Context "shared-runtime"
  registry  GitLab Project Access Token from Vault -> GitLabFlowRegistryClient

Vault login uses the pod's projected ServiceAccount token (audience "vault")
and the Kubernetes auth role bound to ServiceAccount "nifi" of this namespace.
There is no static Vault token. Every setting comes from environment variables
(ConfigMap nifi-sync-env + Secret nifi-admin); see k8s/nifi/*/kustomization.yaml.

The script never writes Git and never deploys a flow. The Parameter Context is a
plain local context (no provider association), so the shared snapshot stays
portable between staging, VN and INDO. Stdlib only; runs in the NiFi image.
"""

import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REQUIRED_PARAMS = {
    "kafka.bootstrap",
    "kafka.topic",
    "kafka.group",
    "db.url",
    "db.username",
    "db.password",
    "db.schema",
    "db.table",
}
SENSITIVE_PARAMS = {"db.password"}
GITLAB_CLIENT_TYPE = "org.apache.nifi.gitlab.GitLabFlowRegistryClient"
# NiFi uses a self-signed certificate in this lab. Set NIFI_CA_FILE to verify it.
_CA = os.environ.get("NIFI_CA_FILE")
TLS = (
    ssl.create_default_context(cafile=_CA) if _CA else ssl._create_unverified_context()
)


def log(message):
    print(message, flush=True)


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing environment variable {name}")
    return value


def http(url, method="GET", data=None, headers=None, form=False, timeout=30):
    headers = dict(headers or {})
    body = None
    if data is not None:
        if form:
            body = urllib.parse.urlencode(data).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            body = json.dumps(data).encode()
            headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, context=TLS, timeout=timeout) as response:
            payload = response.read().decode()
            if not payload:
                return {}
            if "json" in response.headers.get("Content-Type", ""):
                return json.loads(payload)
            return payload
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise HttpError(error.code, f"{method} {url} -> HTTP {error.code}: {detail}")


class HttpError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class Vault:
    """Vault client authenticated with the Kubernetes auth method."""

    def __init__(self, addr, role, auth_path="kubernetes", jwt_file=None):
        self.addr = addr.rstrip("/")
        self.role = role
        self.auth_path = auth_path
        self.jwt_file = jwt_file or "/var/run/secrets/vault/token"
        self.token = None

    def login(self, jwt=None):
        if jwt is None:
            with open(self.jwt_file) as handle:
                jwt = handle.read().strip()
        result = http(
            f"{self.addr}/v1/auth/{self.auth_path}/login",
            "POST",
            {"role": self.role, "jwt": jwt},
        )
        auth = result["auth"]
        self.token = auth["client_token"]
        meta = auth.get("metadata") or {}
        log(
            "Vault login OK: role={} sa={}/{} policies={} ttl={}s".format(
                meta.get("role", self.role),
                meta.get("service_account_namespace"),
                meta.get("service_account_name"),
                ",".join(auth.get("token_policies", [])),
                auth.get("lease_duration"),
            )
        )
        return auth

    def read(self, path):
        return http(
            f"{self.addr}/v1/{path.lstrip('/')}", headers={"X-Vault-Token": self.token}
        )["data"]

    def revoke(self):
        if self.token:
            try:
                http(
                    f"{self.addr}/v1/auth/token/revoke-self",
                    "POST",
                    {},
                    {"X-Vault-Token": self.token},
                )
            finally:
                self.token = None


class NiFi:
    """Minimal NiFi REST client (Single User login, bearer token)."""

    def __init__(self, api, username, password):
        self.api = api.rstrip("/")
        self.username = username
        self.password = password
        self.headers = {}

    def login(self):
        token = http(
            self.api + "/access/token",
            "POST",
            {"username": self.username, "password": self.password},
            form=True,
        )
        self.headers = {"Authorization": "Bearer " + token}
        return self

    def call(self, path, method="GET", data=None, timeout=60):
        return http(self.api + path, method, data, self.headers, timeout=timeout)

    def get(self, path):
        return self.call(path)

    def post(self, path, data):
        return self.call(path, "POST", data)

    def put(self, path, data):
        return self.call(path, "PUT", data)

    def delete(self, path):
        return self.call(path, "DELETE")

    def wait_request(self, path, key="request", attempts=180, delay=1.0):
        """Poll an asynchronous NiFi request, then delete it."""
        for _ in range(attempts):
            status = self.get(path)[key]
            if status.get("complete") or status.get("finished"):
                self.delete(path)
                if status.get("failureReason"):
                    raise RuntimeError(
                        f"NiFi request failed: {status['failureReason']}"
                    )
                return status
            time.sleep(delay)
        raise RuntimeError(f"NiFi request timed out: {path}")

    @staticmethod
    def resolve_properties(descriptors, wanted):
        """Map {displayName or name: value} to property names; resolve allowable values."""
        resolved = {}
        for label, value in wanted.items():
            labels = label if isinstance(label, tuple) else (label,)
            match = next(
                (
                    d
                    for d in descriptors.values()
                    if d.get("displayName") in labels or d.get("name") in labels
                ),
                None,
            )
            if match is None:
                known = sorted(d.get("displayName", "") for d in descriptors.values())
                raise RuntimeError(f"Property {labels} not found; available: {known}")
            allowed = match.get("allowableValues") or []
            if value is not None and allowed:
                options = [a["allowableValue"] for a in allowed]
                chosen = [
                    o["value"]
                    for o in options
                    if value in (o["value"], o["displayName"])
                ]
                if not chosen:
                    raise RuntimeError(
                        f"Value {value!r} not allowed for {labels}: "
                        f"{[o['displayName'] for o in options]}"
                    )
                value = chosen[0]
            resolved[match["name"]] = value
        return resolved


def sync_params(vault, nifi, secret_path, context_name="shared-runtime"):
    values = vault.read(secret_path)
    if set(values) != REQUIRED_PARAMS or any(
        not isinstance(v, str) for v in values.values()
    ):
        raise SystemExit(
            f"Vault {secret_path} keys/types differ from the 8-key contract; review before apply"
        )
    log(
        f"Read {secret_path}: topic={values['kafka.topic']} group={values['kafka.group']} "
        f"schema={values['db.schema']} bootstrap={values['kafka.bootstrap']}"
    )
    contexts = nifi.get("/flow/parameter-contexts")["parameterContexts"]
    found = [c for c in contexts if c["component"]["name"] == context_name]
    if len(found) > 1:
        raise SystemExit("Ambiguous Parameter Context name " + context_name)
    params = [
        {"parameter": {"name": k, "value": v, "sensitive": k in SENSITIVE_PARAMS}}
        for k, v in sorted(values.items())
    ]
    if not found:
        result = nifi.post(
            "/parameter-contexts",
            {
                "revision": {"version": 0},
                "component": {
                    "name": context_name,
                    "description": "Local values synced from Vault by ServiceAccount; registry removes values",
                    "parameters": params,
                },
            },
        )
        log(f"Created {context_name}: {result['component']['id']}")
        return
    ident = found[0]["component"]["id"]
    current = nifi.get("/parameter-contexts/" + ident)
    if current["component"].get("parameterProviderConfiguration"):
        raise SystemExit(
            "Context is managed by a Parameter Provider; do not mix ownership"
        )
    existing = {
        p["parameter"]["name"]: p["parameter"]
        for p in current["component"].get("parameters", [])
    }
    for name in REQUIRED_PARAMS & existing.keys():
        if bool(existing[name].get("sensitive")) != (name in SENSITIVE_PARAMS):
            raise SystemExit(f"Sensitivity mismatch for {name}; resolve manually")
    result = nifi.post(
        f"/parameter-contexts/{ident}/update-requests",
        {
            # NiFi 2.6 ParameterUpdateManager builds the revision from this entity-level id.
            "id": ident,
            "revision": current["revision"],
            "component": {"id": ident, "name": context_name, "parameters": params},
        },
    )
    request_id = result["request"]["requestId"]
    nifi.wait_request(f"/parameter-contexts/{ident}/update-requests/{request_id}")
    log(f"Updated {context_name} ({ident}); NiFi restarted affected components itself")


def gitlab_registry_properties(token):
    return {
        "GitLab API URL": env("GITLAB_API_URL"),
        "GitLab API Version": "V4",
        "Repository Namespace": env("GITLAB_NAMESPACE", "data360"),
        "Repository Name": env("GITLAB_PROJECT", "flows-nifi"),
        "Repository Path": env("GITLAB_REPOSITORY_PATH", "flows"),
        ("Default Branch", "Repository Branch"): env("REGISTRY_BRANCH"),
        "Authentication Type": "Access Token",
        "Access Token": token,
        "Parameter Context Values": "Remove",
    }


def sync_registry(vault, nifi, token_path, client_name):
    secret = vault.read(token_path)
    token = secret.get("access_token")
    if not token:
        raise SystemExit(f"{token_path} has no access_token")
    log(
        "Read {}: token name={} type={} role={} scopes={} expires={}".format(
            token_path,
            secret.get("token_name"),
            secret.get("token_type", "project_access_token"),
            secret.get("access_level"),
            secret.get("scopes"),
            secret.get("expires_at"),
        )
    )
    clients = nifi.get("/controller/registry-clients")["registries"]
    found = [c for c in clients if c["component"]["name"] == client_name]
    if len(found) > 1:
        raise SystemExit("Ambiguous Registry Client name " + client_name)
    if not found:
        types = nifi.get("/controller/registry-types")["flowRegistryClientTypes"]
        bundle = next(t["bundle"] for t in types if t["type"] == GITLAB_CLIENT_TYPE)
        entity = nifi.post(
            "/controller/registry-clients",
            {
                "revision": {"version": 0},
                "component": {
                    "name": client_name,
                    "type": GITLAB_CLIENT_TYPE,
                    "bundle": bundle,
                },
            },
        )
        log(f"Created Registry Client {client_name}: {entity['id']}")
    else:
        entity = found[0]
    ident = entity["id"]
    entity = nifi.get("/controller/registry-clients/" + ident)
    if entity["component"]["type"] != GITLAB_CLIENT_TYPE:
        raise SystemExit(f"{client_name} is not a GitLabFlowRegistryClient")
    properties = NiFi.resolve_properties(
        entity["component"]["descriptors"], gitlab_registry_properties(token)
    )
    entity = nifi.put(
        "/controller/registry-clients/" + ident,
        {
            "revision": entity["revision"],
            "component": {"id": ident, "name": client_name, "properties": properties},
        },
    )
    errors = entity["component"].get("validationErrors") or []
    if errors:
        raise SystemExit(f"Registry Client invalid: {errors}")
    branch = env("REGISTRY_BRANCH")
    branches = [
        b["branch"]["name"]
        for b in nifi.get(f"/flow/registries/{ident}/branches")["branches"]
    ]
    query = urllib.parse.urlencode({"branch": branch})
    buckets = [
        b["bucket"]["name"]
        for b in nifi.get(f"/flow/registries/{ident}/buckets?{query}")["buckets"]
    ]
    if branch not in branches:
        raise SystemExit(
            f"Branch {branch} not visible through {client_name}: {branches}"
        )
    log(
        f"Registry Client {client_name} OK: branch={branch} branches={sorted(branches)} "
        f"buckets={sorted(buckets)}"
    )


def main():
    commands = sys.argv[1:] or ["params"]
    if any(c not in ("params", "registry") for c in commands):
        raise SystemExit("Usage: nifi_k8s_sync.py [params] [registry]")
    vault = Vault(
        env("VAULT_ADDR"),
        env("VAULT_ROLE"),
        os.environ.get("VAULT_AUTH_PATH", "kubernetes"),
        os.environ.get("VAULT_SA_TOKEN_FILE"),
    )
    nifi = NiFi(env("NIFI_API_URL"), env("NIFI_USERNAME"), env("NIFI_PASSWORD"))
    for attempt in range(30):
        try:
            nifi.login()
            break
        except (OSError, HttpError) as error:
            log(f"Waiting for NiFi API ({error})")
            time.sleep(10)
    else:
        raise SystemExit("NiFi API not reachable")
    vault.login()
    try:
        if "params" in commands:
            sync_params(vault, nifi, env("VAULT_PARAMS_PATH"))
        if "registry" in commands:
            sync_registry(
                vault,
                nifi,
                env("VAULT_REGISTRY_TOKEN_PATH"),
                env("REGISTRY_CLIENT_NAME"),
            )
    finally:
        vault.revoke()
    log("Done")


if __name__ == "__main__":
    main()
