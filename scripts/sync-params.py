#!/usr/bin/env python3
"""LAB bridge: Vault KV1 -> local NiFi shared-runtime context. Never writes Git.
Stop the target PG first. Existing-context updates are asynchronous and checked.
"""

import json, pathlib, urllib.request, urllib.parse, ssl, sys, time

root = pathlib.Path(__file__).resolve().parents[1]
env = dict(
    line.split("=", 1)
    for line in (root / ".env").read_text().splitlines()
    if line and not line.startswith("#")
)
market = sys.argv[1] if len(sys.argv) == 2 else ""
settings = {
    "stag": (18443, 18200, "staging", "vn"),
    "vn": (28443, 18200, "runtime", "vn"),
    "indo": (38443, 28200, "runtime", "indo"),
}
if market not in settings:
    raise SystemExit("Usage: sync-params.py stag|vn|indo")
np, vp, secret, vm = settings[market]
ip = env["HOST_IP"]
# Self-signed NiFi certificate only in this lab. Use a trusted CA outside the lab.
ctx = ssl._create_unverified_context()


def call(base, path, method="GET", data=None, headers=None):
    headers = dict(headers or {})
    if isinstance(data, dict):
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        payload = r.read().decode()
        if not payload:
            return {}
        if "json" in r.headers.get("Content-Type", ""):
            return json.loads(payload)
        return payload


v = call(
    f"http://{ip}:{vp}",
    "/v1/kv/" + secret,
    headers={"X-Vault-Token": "nifi-" + vm + "-lab-token"},
)["data"]
required = {
    "kafka.bootstrap",
    "kafka.topic",
    "kafka.group",
    "db.url",
    "db.username",
    "db.password",
    "db.schema",
    "db.table",
}
if set(v) != required or any(not isinstance(x, str) for x in v.values()):
    raise SystemExit(
        "Vault keys/types differ from expected contract; review before apply."
    )
base = f"https://{ip}:{np}/nifi-api"
token = call(
    base,
    "/access/token",
    "POST",
    urllib.parse.urlencode(
        {"username": "admin", "password": env["NIFI_PASSWORD"]}
    ).encode(),
    {"Content-Type": "application/x-www-form-urlencoded"},
)
h = {"Authorization": "Bearer " + token}
contexts = call(base, "/flow/parameter-contexts", headers=h)["parameterContexts"]
found = [x for x in contexts if x["component"]["name"] == "shared-runtime"]
if len(found) > 1:
    raise SystemExit("Ambiguous context name")
params = [
    {"parameter": {"name": k, "value": val, "sensitive": k == "db.password"}}
    for k, val in sorted(v.items())
]
if not found:
    result = call(
        base,
        "/parameter-contexts",
        "POST",
        {
            "revision": {"version": 0},
            "component": {
                "name": "shared-runtime",
                "description": "Local values from Vault; registry removes values",
                "parameters": params,
            },
        },
        h,
    )
    print("Created shared-runtime:", result["component"]["id"])
    sys.exit(0)
ident = found[0]["component"]["id"]
current = call(base, "/parameter-contexts/" + ident, headers=h)
if current["component"].get("parameterProviderConfiguration"):
    raise SystemExit("Context managed by provider; do not mix ownership")
existing = {
    p["parameter"]["name"]: p["parameter"]
    for p in current["component"].get("parameters", [])
}
for k in required & existing.keys():
    if bool(existing[k].get("sensitive")) != (k == "db.password"):
        raise SystemExit("Sensitivity mismatch for " + k + "; resolve manually")
result = call(
    base,
    f"/parameter-contexts/{ident}/update-requests",
    "POST",
    {
        "revision": current["revision"],
        "component": {"id": ident, "name": "shared-runtime", "parameters": params},
    },
    h,
)
request_id = result["request"]["requestId"]
path = f"/parameter-contexts/{ident}/update-requests/{request_id}"
for _ in range(120):
    status = call(base, path, headers=h)["request"]
    if status.get("complete"):
        if status.get("failureReason"):
            raise SystemExit("NiFi update failed: " + status["failureReason"])
        call(base, path, "DELETE", headers=h)
        print("Updated shared-runtime. Inspect PG binding and validation before start.")
        break
    time.sleep(1)
else:
    raise SystemExit("Update timed out; inspect " + path + " before retry")
