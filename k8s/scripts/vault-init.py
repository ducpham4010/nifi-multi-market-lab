#!/usr/bin/env python3
"""Operator bootstrap for Vault VN / INDO on Kubernetes. Idempotent.

1. Initialize once (1 key share), unseal on every run. Keys/root token are kept
   on the lab host in .private/k8s/vault-<market>-init.json (0600), never in Git
   or in the cluster.
2. KV v1 mount "kv" and the 8-key runtime contract, seeded only when missing.
   Endpoints are Kubernetes Service DNS names.
3. Kubernetes auth method: Vault validates ServiceAccount JWTs with TokenReview
   using its own pod token (ClusterRoleBinding to system:auth-delegator).
4. One policy + one role per NiFi instance, bound to ServiceAccount "nifi" in
   that instance's namespace with audience "vault". No static NiFi token.

Usage (lab host): python3 k8s/scripts/vault-init.py [--unseal-only]
Vault is reached through the Gateway URL by default; override with
VAULT_VN_ADDR / VAULT_INDO_ADDR (e.g. a kubectl port-forward).
"""

import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if line and not line.startswith("#") and "=" in line
)
IP = ENV["HOST_IP"]
PRIVATE = ROOT / ".private" / "k8s"
PRIVATE.mkdir(mode=0o700, parents=True, exist_ok=True)

KAFKA = {
    "vn": "kafka.kafka-vn.svc.cluster.local:9092",
    "indo": "kafka.kafka-indo.svc.cluster.local:9092",
}
JDBC = {
    "vn": "jdbc:postgresql://postgres.postgres-vn.svc.cluster.local:5432/market",
    "indo": "jdbc:postgresql://postgres.postgres-indo.svc.cluster.local:5432/market",
}


def runtime_secret(db_market, topic, group, schema):
    return {
        "kafka.bootstrap": KAFKA[db_market],
        "kafka.topic": topic,
        "kafka.group": group,
        "db.url": JDBC[db_market],
        "db.username": "admin",
        "db.password": ENV["DB_PASSWORD"],
        "db.schema": schema,
        "db.table": "trip_events",
    }


# market -> secrets seeded in that Vault, and NiFi instances (role/policy) it serves
VAULTS = {
    "vn": {
        "secrets": {
            "runtime": runtime_secret("vn", "trips", "nifi-vn", "market"),
            "staging": runtime_secret("vn", "trips-staging", "nifi-staging", "staging"),
            # Separate secret for the optional native Parameter Provider exercise.
            "preview": runtime_secret("vn", "trips", "nifi-vn-preview", "market"),
        },
        "instances": {
            "nifi-stag": {
                "namespace": "nifi-stag",
                "read": ["kv/staging", "kv/gitlab-registry/staging"],
            },
            "nifi-vn": {
                "namespace": "nifi-vn",
                "read": ["kv/runtime", "kv/preview", "kv/gitlab-registry/vn"],
            },
        },
    },
    "indo": {
        "secrets": {"runtime": runtime_secret("indo", "trips", "nifi-indo", "market")},
        "instances": {
            "nifi-indo": {
                "namespace": "nifi-indo",
                "read": ["kv/runtime", "kv/gitlab-registry/indo"],
            },
        },
    },
}


def address(market):
    return os.environ.get(
        f"VAULT_{market.upper()}_ADDR", f"http://vault-{market}.{IP}.sslip.io:31080"
    ).rstrip("/")


def req(base, path, method="GET", data=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Vault-Token"] = token
    request = urllib.request.Request(
        base + "/v1/" + path,
        data=None if data is None else json.dumps(data).encode(),
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        content = response.read()
        return json.loads(content) if content else {}


def policy_text(paths):
    rules = [
        "# Names only (needed by HashiCorpVaultParameterProvider listing); values need read.",
        'path "kv/" { capabilities = ["list"] }',
    ]
    rules += [f'path "{p}" {{ capabilities = ["read"] }}' for p in paths]
    return "\n".join(rules) + "\n"


def init_unseal(market, base):
    keyfile = PRIVATE / f"vault-{market}-init.json"
    if not req(base, "sys/init")["initialized"]:
        if keyfile.exists():
            raise SystemExit(
                f"{keyfile} exists but Vault {market} is uninitialized; resolve PVC/key mismatch manually"
            )
        init = req(base, "sys/init", "PUT", {"secret_shares": 1, "secret_threshold": 1})
        fd = os.open(keyfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(init, handle)
        print(f"{market}: initialized; keys saved to {keyfile.relative_to(ROOT)}")
    if not keyfile.exists():
        raise SystemExit(
            f"Missing {keyfile}; restore it, never re-initialize a used Vault"
        )
    init = json.loads(keyfile.read_text())
    if req(base, "sys/seal-status")["sealed"]:
        req(base, "sys/unseal", "PUT", {"key": init["keys"][0]})
        print(f"{market}: unsealed")
    return init["root_token"]


def configure(market, base, token):
    spec = VAULTS[market]
    mounts = req(base, "sys/mounts", token=token)
    if "kv/" not in mounts:
        req(
            base,
            "sys/mounts/kv",
            "POST",
            {"type": "kv", "options": {"version": "1"}},
            token,
        )
    elif (
        mounts["kv/"]["type"] != "kv"
        or mounts["kv/"].get("options", {}).get("version", "1") != "1"
    ):
        raise SystemExit("kv/ must be KV v1")
    for name, values in spec["secrets"].items():
        try:
            req(base, "kv/" + name, token=token)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            req(base, "kv/" + name, "POST", values, token)
            print(f"{market}: seeded kv/{name}")

    auths = req(base, "sys/auth", token=token)
    if "kubernetes/" not in auths:
        req(base, "sys/auth/kubernetes", "POST", {"type": "kubernetes"}, token)
        print(f"{market}: enabled auth/kubernetes")
    # In-cluster defaults: Vault uses its pod ServiceAccount token as reviewer JWT
    # and the pod CA bundle; both are re-read when Kubernetes rotates them.
    req(
        base,
        "auth/kubernetes/config",
        "POST",
        {"kubernetes_host": "https://kubernetes.default.svc.cluster.local"},
        token,
    )
    for name, instance in spec["instances"].items():
        req(
            base,
            f"sys/policies/acl/{name}",
            "PUT",
            {"policy": policy_text(instance["read"])},
            token,
        )
        req(
            base,
            f"auth/kubernetes/role/{name}",
            "POST",
            {
                "bound_service_account_names": ["nifi"],
                "bound_service_account_namespaces": [instance["namespace"]],
                "audience": "vault",
                "token_policies": [name],
                "token_no_default_policy": False,
                "token_ttl": "1h",
                "token_max_ttl": "4h",
            },
            token,
        )
        print(
            f"{market}: role {name} -> ServiceAccount {instance['namespace']}/nifi, "
            f"policy {name}: read {', '.join(instance['read'])}"
        )


def main():
    unseal_only = "--unseal-only" in sys.argv[1:]
    for market in VAULTS:
        base = address(market)
        token = init_unseal(market, base)
        if not unseal_only:
            configure(market, base, token)
        health = req(base, "sys/health?standbyok=true")
        print(
            f"{market}: {base} initialized={health['initialized']} sealed={health['sealed']}"
        )


if __name__ == "__main__":
    main()
