#!/usr/bin/env python3
"""Prepare GitLab project data360/flows-nifi and NiFi Project Access Tokens.

Runs gitlab-bootstrap.rb inside pod gitlab/gitlab-0 (gitlab-rails runner), so no
Personal Access Token is needed for administration. Then stores each Project
Access Token (PrAT) in Vault, where only the matching NiFi ServiceAccount role
can read it:

  PrAT                 role in project   scope      Vault path (reader role)
  nifi-staging-writer  Developer         api        VN   kv/gitlab-registry/staging (nifi-stag)
  nifi-vn-reader       Reporter          read_api   VN   kv/gitlab-registry/vn      (nifi-vn)
  nifi-indo-reader     Reporter          read_api   INDO kv/gitlab-registry/indo    (nifi-indo)

Existing tokens are kept while GitLab still reports them active, so re-running
is safe. Options:
  --rotate                 revoke and recreate the three NiFi tokens
  --recreate-project       delete data360/flows-nifi (snapshots, MRs, tokens) and
                           create it again: clean state for the practice guide
  --release-token          also create lab-release-maintainer (Maintainer, api,
                           1 day) for automated MR merges in k8s/tests/e2e_lab.py
  --revoke-release-token   revoke lab-release-maintainer
Then run: bash k8s/scripts/nifi-sync.sh registry all
"""

import json
import os
import pathlib
import subprocess
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
GITLAB = os.environ.get("GITLAB_URL", f"http://gitlab.{IP}.sslip.io:31080").rstrip("/")
VAULT = {
    "vn": os.environ.get("VAULT_VN_ADDR", f"http://vault-vn.{IP}.sslip.io:31080"),
    "indo": os.environ.get("VAULT_INDO_ADDR", f"http://vault-indo.{IP}.sslip.io:31080"),
}
DEVELOPER, REPORTER, MAINTAINER = 30, 20, 40
LEVEL_NAMES = {DEVELOPER: "Developer", REPORTER: "Reporter", MAINTAINER: "Maintainer"}
NIFI_TOKENS = [
    {
        "name": "nifi-staging-writer",
        "access_level": DEVELOPER,
        "scopes": ["api"],
        "vault": "vn",
        "path": "kv/gitlab-registry/staging",
    },
    {
        "name": "nifi-vn-reader",
        "access_level": REPORTER,
        "scopes": ["read_api"],
        "vault": "vn",
        "path": "kv/gitlab-registry/vn",
    },
    {
        "name": "nifi-indo-reader",
        "access_level": REPORTER,
        "scopes": ["read_api"],
        "vault": "indo",
        "path": "kv/gitlab-registry/indo",
    },
]
RELEASE = {
    "name": "lab-release-maintainer",
    "access_level": MAINTAINER,
    "scopes": ["api"],
    "days": 1,
}


def http(url, method="GET", data=None, headers=None):
    headers = dict(headers or {})
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read()
        return json.loads(content) if content else {}


def vault_root(market):
    return json.loads((PRIVATE / f"vault-{market}-init.json").read_text())["root_token"]


def vault_read(market, path):
    try:
        return http(
            f"{VAULT[market]}/v1/{path}", headers={"X-Vault-Token": vault_root(market)}
        )["data"]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def vault_write(market, path, data):
    http(
        f"{VAULT[market]}/v1/{path}",
        "POST",
        data,
        {"X-Vault-Token": vault_root(market)},
    )


def token_active(token):
    try:
        info = http(
            f"{GITLAB}/api/v4/personal_access_tokens/self",
            headers={"PRIVATE-TOKEN": token},
        )
        return info.get("active") and not info.get("revoked")
    except urllib.error.HTTPError:
        return False


def run_rails(specs, recreate=False):
    script = (pathlib.Path(__file__).parent / "gitlab-bootstrap.rb").read_bytes()
    pod = ["kubectl", "-n", "gitlab", "exec", "-i", "gitlab-0", "--"]
    subprocess.run(
        pod + ["sh", "-c", "umask 022; cat > /tmp/lab-gitlab-bootstrap.rb"],
        input=script,
        check=True,
    )
    try:
        run = subprocess.run(
            pod
            + [
                "env",
                "LAB_TOKENS=" + json.dumps(specs),
                f"LAB_RECREATE_PROJECT={int(recreate)}",
                "gitlab-rails",
                "runner",
                "/tmp/lab-gitlab-bootstrap.rb",
            ],
            capture_output=True,
            text=True,
        )
    finally:
        subprocess.run(pod + ["rm", "-f", "/tmp/lab-gitlab-bootstrap.rb"], check=False)
    if run.returncode:
        # Token values are only printed in the LAB_RESULT line, which a failed run never reaches.
        raise SystemExit(
            "gitlab-rails runner failed:\n"
            + "\n".join((run.stderr or run.stdout).splitlines()[-25:])
        )
    output = run.stdout
    line = next(l for l in output.splitlines() if l.startswith("LAB_RESULT "))
    return json.loads(line.split(" ", 1)[1])


def main():
    args = set(sys.argv[1:])
    unknown = args - {
        "--rotate",
        "--recreate-project",
        "--release-token",
        "--revoke-release-token",
    }
    if unknown:
        raise SystemExit(__doc__)
    specs, plan = [], []
    for spec in NIFI_TOKENS:
        current = vault_read(spec["vault"], spec["path"])
        if (
            not args & {"--rotate", "--recreate-project"}
            and current
            and token_active(current.get("access_token", ""))
        ):
            print(
                f"keep {spec['name']}: still active in GitLab ({spec['vault']} {spec['path']})"
            )
            continue
        specs.append(
            {k: spec[k] for k in ("name", "access_level", "scopes")} | {"days": 180}
        )
        plan.append(spec)
    if "--release-token" in args:
        specs.append(RELEASE)
    if "--revoke-release-token" in args:
        specs.append(RELEASE | {"revoke_only": True})

    result = run_rails(specs, "--recreate-project" in args)
    for step in result["steps"]:
        print("gitlab:", step)
    created = {t["name"]: t for t in result["tokens"]}
    PRIVATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    for spec in plan:
        token = created[spec["name"]]
        vault_write(
            spec["vault"],
            spec["path"],
            {
                "access_token": token["token"],
                "token_name": token["name"],
                "token_type": "project_access_token",
                "token_id": str(token["id"]),
                "bot_user": token["bot_user"],
                "access_level": LEVEL_NAMES[token["access_level"]],
                "scopes": ",".join(token["scopes"]),
                "expires_at": token["expires_at"],
                "project": result["project"],
            },
        )
        print(
            f"stored {spec['name']} ({LEVEL_NAMES[token['access_level']]}, {','.join(token['scopes'])}, "
            f"expires {token['expires_at']}) in Vault {spec['vault']} {spec['path']}"
        )
    if RELEASE["name"] in created:
        path = PRIVATE / "gitlab-release-token.json"
        fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(created[RELEASE["name"]], handle)
        print(
            f"release token saved to {path.relative_to(ROOT)} (expires {created[RELEASE['name']]['expires_at']})"
        )
    if "--revoke-release-token" in args:
        (PRIVATE / "gitlab-release-token.json").unlink(missing_ok=True)
    print(
        f"project {result['project']} default={result['default_branch']} branches={result['branches']}"
    )
    if plan:
        print("Next: bash k8s/scripts/nifi-sync.sh registry all")


if __name__ == "__main__":
    main()
