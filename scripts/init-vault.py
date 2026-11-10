#!/usr/bin/env python3
"""Initialize once, unseal on every restart, seed only missing KV secrets."""

import json, os, pathlib, urllib.request, urllib.error

root = pathlib.Path(__file__).resolve().parents[1]
env = dict(
    line.split("=", 1)
    for line in (root / ".env").read_text().splitlines()
    if line and not line.startswith("#")
)
ip = env["HOST_IP"]
private = root / ".private"
private.mkdir(mode=0o700, exist_ok=True)


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
    with urllib.request.urlopen(request, timeout=15) as response:
        content = response.read()
        return json.loads(content) if content else {}


for market, port in [("vn", 18200), ("indo", 28200)]:
    base = f"http://{ip}:{port}"
    keyfile = private / f"vault-{market}-init.json"
    if not req(base, "sys/init")["initialized"]:
        if keyfile.exists():
            raise SystemExit(
                f"{keyfile} exists but Vault is uninitialized. Resolve volume/key mismatch manually."
            )
        init = req(base, "sys/init", "PUT", {"secret_shares": 1, "secret_threshold": 1})
        fd = os.open(keyfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(init, f)
    if not keyfile.exists():
        raise SystemExit(f"Missing {keyfile}; restore keys, do not reinitialize.")
    init = json.loads(keyfile.read_text())
    root_token = init["root_token"]
    if req(base, "sys/seal-status")["sealed"]:
        req(base, "sys/unseal", "PUT", {"key": init["keys"][0]})
    mounts = req(base, "sys/mounts", token=root_token)
    if "kv/" not in mounts:
        req(
            base,
            "sys/mounts/kv",
            "POST",
            {"type": "kv", "options": {"version": "1"}},
            root_token,
        )
    elif (
        mounts["kv/"]["type"] != "kv"
        or mounts["kv/"].get("options", {}).get("version", "1") != "1"
    ):
        raise SystemExit("kv/ must be KV v1")

    def seed(name, port_k, port_db, topic, group, schema):
        values = {
            "kafka.bootstrap": f"{ip}:{port_k}",
            "kafka.topic": topic,
            "kafka.group": group,
            "db.url": f"jdbc:postgresql://{ip}:{port_db}/market",
            "db.username": "admin",
            "db.password": env["DB_PASSWORD"],
            "db.schema": schema,
            "db.table": "trip_events",
        }
        try:
            req(base, "kv/" + name, token=root_token)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            req(base, "kv/" + name, "POST", values, root_token)

    if market == "vn":
        seed("runtime", 19092, 15432, "trips", "nifi-vn", "market")
        seed("staging", 19092, 15432, "trips-staging", "nifi-staging", "staging")
    else:
        seed("runtime", 29092, 25432, "trips", "nifi-indo", "market")
    policy = 'path "kv" { capabilities = ["list"] }\npath "kv/*" { capabilities = ["read", "list"] }\npath "auth/token/lookup-self" { capabilities = ["read"] }'
    req(base, "sys/policies/acl/nifi-read", "PUT", {"policy": policy}, root_token)
    # Easy fixed LAB token, scoped to KV read/list; the actual root token stays in .private.
    token = "nifi-" + market + "-lab-token"
    try:
        req(base, "auth/token/lookup-self", token=token)
    except urllib.error.HTTPError as e:
        if e.code not in (400, 403):
            raise
        req(
            base,
            "auth/token/create",
            "POST",
            {
                "id": token,
                "policies": ["nifi-read"],
                "no_default_policy": True,
                "ttl": "720h",
                "renewable": True,
            },
            root_token,
        )
    print(
        f"{market}: initialized/unsealed; read token={token}; existing secrets not overwritten"
    )
