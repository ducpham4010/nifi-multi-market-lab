#!/usr/bin/env python3
"""Read-only readiness checks for the Kubernetes lab (run on the lab host).

Covers: pods per namespace, Kafka topics, PostgreSQL tables, Vault unsealed,
Vault Kubernetes auth (positive and negative ServiceAccount cases), NiFi
contexts and Registry Clients, GitLab readiness/SSH, Project Access Tokens and
the LAN URLs published by the Gateway. Does not change flows or data.
"""

import json
import pathlib
import socket
import subprocess
import sys
import urllib.error
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "k8s/nifi/base"))
from nifi_k8s_sync import NiFi, Vault, http, HttpError  # noqa: E402

ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if line and not line.startswith("#") and "=" in line
)
IP = ENV["HOST_IP"]
PRIVATE = ROOT / ".private" / "k8s"
NAMESPACES = {
    "lab-gateway": [],
    "gitlab": ["gitlab"],
    "vault-vn": ["vault"],
    "vault-indo": ["vault"],
    "kafka-vn": ["kafka"],
    "kafka-indo": ["kafka"],
    "postgres-vn": ["postgres"],
    "postgres-indo": ["postgres"],
    "nifi-stag": ["nifi"],
    "nifi-vn": ["nifi"],
    "nifi-indo": ["nifi"],
}
NIFI = {
    # market: (vault, role, params path, registry client, branch)
    "stag": ("vn", "nifi-stag", "kv/staging", "gitlab-staging", "staging"),
    "vn": ("vn", "nifi-vn", "kv/runtime", "gitlab-main", "main"),
    "indo": ("indo", "nifi-indo", "kv/runtime", "gitlab-main", "main"),
}
failures = []


def check(condition, message):
    print(("PASS " if condition else "FAIL ") + message)
    if not condition:
        failures.append(message)
    return condition


def kubectl(*args, stdin=None):
    return subprocess.run(
        ["kubectl", *args], input=stdin, capture_output=True, text=True, check=True
    ).stdout.strip()


def vault_addr(market):
    return f"http://vault-{market}.{IP}.sslip.io:31080"


def sa_token(namespace, account="nifi", audience="vault"):
    args = ["-n", namespace, "create", "token", account, "--duration", "10m"]
    if audience:
        args += ["--audience", audience]
    return kubectl(*args)


def denied(callable_):
    try:
        callable_()
        return False
    except HttpError as error:
        return error.code in (400, 403)


def main():
    for ns, workloads in NAMESPACES.items():
        pods = json.loads(kubectl("-n", ns, "get", "pods", "-o", "json"))["items"]
        running = [p for p in pods if p["status"].get("phase") == "Running"]
        ready = all(
            c.get("ready")
            for p in running
            for c in p["status"].get("containerStatuses", [])
        )
        oom = [
            p["metadata"]["name"]
            for p in running
            for c in p["status"].get("containerStatuses", [])
            if (c.get("lastState", {}).get("terminated") or {}).get("reason")
            == "OOMKilled"
        ]
        if workloads:
            check(
                len(running) >= len(workloads) and ready and not oom,
                f"{ns}: {len(running)} running pod(s), ready={ready}, OOMKilled={oom or 'none'}",
            )
    gateway = json.loads(
        kubectl("-n", "lab-gateway", "get", "gateway", "lab", "-o", "json")
    )
    programmed = any(
        c["type"] == "Programmed" and c["status"] == "True"
        for c in gateway["status"]["conditions"]
    )
    check(
        programmed,
        "Gateway lab-gateway/lab programmed (NodePort 31080 HTTP, 31443 TLS passthrough)",
    )

    for ns, topics in [
        ("kafka-vn", ["trips", "trips-staging"]),
        ("kafka-indo", ["trips"]),
    ]:
        for topic in topics:
            out = kubectl(
                "-n",
                ns,
                "exec",
                "kafka-0",
                "--",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--describe",
                "--topic",
                topic,
            )
            check(
                "PartitionCount: 3" in out and "Leader: -1" not in out,
                f"{ns}/{topic}: 3 partitions with leaders",
            )

    for ns in ("postgres-vn", "postgres-indo"):
        for schema in ("market", "staging"):
            count = kubectl(
                "-n",
                ns,
                "exec",
                "postgres-0",
                "--",
                "psql",
                "-U",
                "admin",
                "-d",
                "market",
                "-Atc",
                f"SELECT count(*) FROM {schema}.trip_events",
            )
            check(count.isdigit(), f"{ns}/{schema}.trip_events: {count} rows")

    for market in ("vn", "indo"):
        health = http(
            vault_addr(market)
            + "/v1/sys/health?standbyok=true&sealedcode=200&uninitcode=200"
        )
        check(
            health["initialized"] and not health["sealed"],
            f"Vault {market}: initialized and unsealed",
        )
        keyfile = PRIVATE / f"vault-{market}-init.json"
        check(
            keyfile.exists() and keyfile.stat().st_mode & 0o077 == 0,
            f"Vault {market}: key file kept 0600 outside the cluster",
        )

    # Positive: each NiFi ServiceAccount logs in with its own role and reads only its secrets.
    for market, (vm, role, params, _client, _branch) in NIFI.items():
        vault = Vault(vault_addr(vm), role)
        auth = vault.login(sa_token(f"nifi-{market}"))
        check(
            auth["metadata"]["service_account_namespace"] == f"nifi-{market}",
            f"Vault {vm} role {role}: login with ServiceAccount nifi-{market}/nifi",
        )
        check(
            set(vault.read(params)) >= {"kafka.topic", "db.url"},
            f"role {role}: read {params}",
        )
        others = {"stag": "kv/runtime", "vn": "kv/staging", "indo": "kv/staging"}[
            market
        ]
        check(denied(lambda: vault.read(others)), f"role {role}: read {others} denied")
        vault.revoke()
    # Negative: wrong namespace, wrong account, wrong audience.
    check(
        denied(lambda: Vault(vault_addr("vn"), "nifi-vn").login(sa_token("nifi-indo"))),
        "Vault vn: ServiceAccount nifi-indo/nifi cannot use role nifi-vn",
    )
    check(
        denied(lambda: Vault(vault_addr("vn"), "nifi-stag").login(sa_token("nifi-vn"))),
        "Vault vn: ServiceAccount nifi-vn/nifi cannot use role nifi-stag",
    )
    check(
        denied(
            lambda: Vault(vault_addr("vn"), "nifi-vn").login(
                sa_token("nifi-vn", "default")
            )
        ),
        "Vault vn: ServiceAccount nifi-vn/default cannot use role nifi-vn",
    )
    check(
        denied(
            lambda: Vault(vault_addr("vn"), "nifi-vn").login(
                sa_token("nifi-vn", audience=None)
            )
        ),
        "Vault vn: token without audience 'vault' rejected",
    )
    check(
        denied(lambda: Vault(vault_addr("indo"), "nifi-vn").login(sa_token("nifi-vn"))),
        "Vault indo: has no role for NiFi VN",
    )

    for market, (vm, role, params, client, branch) in NIFI.items():
        base = f"https://nifi-{market}.{IP}.sslip.io:31443"
        check(
            "<html" in str(http(base + "/nifi/")).lower(),
            f"NiFi {market}: UI via Gateway {base}/nifi/",
        )
        nifi = NiFi(base + "/nifi-api", "admin", ENV["NIFI_PASSWORD"]).login()
        contexts = [
            c
            for c in nifi.get("/flow/parameter-contexts")["parameterContexts"]
            if c["component"]["name"] == "shared-runtime"
        ]
        if check(len(contexts) == 1, f"NiFi {market}: one shared-runtime context"):
            context = nifi.get("/parameter-contexts/" + contexts[0]["component"]["id"])[
                "component"
            ]
            values = {
                p["parameter"]["name"]: p["parameter"] for p in context["parameters"]
            }
            vault = Vault(vault_addr(vm), role)
            vault.login(sa_token(f"nifi-{market}"))
            expected = vault.read(params)
            vault.revoke()
            same = set(values) == set(expected) and all(
                values[k]["sensitive"] == (k == "db.password")
                and (k == "db.password" or values[k]["value"] == expected[k])
                for k in expected
            )
            check(
                same,
                f"NiFi {market}: 8 parameters equal Vault {vm} {params}, only db.password sensitive",
            )
        clients = [
            c
            for c in nifi.get("/controller/registry-clients")["registries"]
            if c["component"]["name"] == client
        ]
        if check(len(clients) == 1, f"NiFi {market}: Registry Client {client}"):
            ident = clients[0]["id"]
            branches = [
                b["branch"]["name"]
                for b in nifi.get(f"/flow/registries/{ident}/branches")["branches"]
            ]
            buckets = [
                b["bucket"]["name"]
                for b in nifi.get(
                    f"/flow/registries/{ident}/buckets?"
                    + urllib.parse.urlencode({"branch": branch})
                )["buckets"]
            ]
            check(
                branch in branches and "shared" in buckets,
                f"NiFi {market}: {client} reads GitLab branch {branch}, bucket shared",
            )

    readiness = kubectl(
        "-n",
        "gitlab",
        "exec",
        "gitlab-0",
        "--",
        "curl",
        "-fsS",
        "http://127.0.0.1/-/readiness?all=1",
    )
    check(
        json.loads(readiness).get("status") == "ok", "GitLab readiness (all checks) ok"
    )
    gitlab = f"http://gitlab.{IP}.sslip.io:31080"
    check(
        "<html" in http(gitlab + "/users/sign_in").lower(),
        f"GitLab UI via Gateway {gitlab}",
    )
    with socket.create_connection((IP, 31022), timeout=10) as connection:
        check(
            connection.recv(64).startswith(b"SSH-2.0-"),
            "GitLab SSH banner on NodePort 31022",
        )
    tokens = {
        "nifi-staging-writer": ("vn", "kv/gitlab-registry/staging", ["api"]),
        "nifi-vn-reader": ("vn", "kv/gitlab-registry/vn", ["read_api"]),
        "nifi-indo-reader": ("indo", "kv/gitlab-registry/indo", ["read_api"]),
    }
    for name, (vm, path, scopes) in tokens.items():
        root = json.loads((PRIVATE / f"vault-{vm}-init.json").read_text())["root_token"]
        secret = http(f"{vault_addr(vm)}/v1/{path}", headers={"X-Vault-Token": root})[
            "data"
        ]
        info = http(
            gitlab + "/api/v4/personal_access_tokens/self",
            headers={"PRIVATE-TOKEN": secret["access_token"]},
        )
        user = http(
            gitlab + "/api/v4/user", headers={"PRIVATE-TOKEN": secret["access_token"]}
        )
        check(
            info["active"]
            and info["scopes"] == scopes
            and info["name"] == name
            and user.get("bot"),
            f"GitLab {name}: active Project Access Token, bot user {user['username']}, scopes {scopes}",
        )

    print(("FAILED: " + str(len(failures))) if failures else "All checks passed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
