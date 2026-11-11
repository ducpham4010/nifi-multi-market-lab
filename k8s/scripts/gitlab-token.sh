#!/usr/bin/env bash
# Print the Project Access Token stored in Vault for one NiFi instance (operator use,
# e.g. Registry Client by hand, git clone or release-sha.py). Reads with the root token
# kept in .private/k8s; nothing is written.
#   bash k8s/scripts/gitlab-token.sh stag|vn|indo
source "$(dirname "$0")/common.sh"
case "${1:?Usage: gitlab-token.sh stag|vn|indo}" in
  stag) vault=vn; path=kv/gitlab-registry/staging ;;
  vn) vault=vn; path=kv/gitlab-registry/vn ;;
  indo) vault=indo; path=kv/gitlab-registry/indo ;;
  *) echo 'Use stag, vn or indo' >&2; exit 1 ;;
esac
python3 - "$vault" "$path" "$HOST_IP" <<'PYCODE'
import json, pathlib, sys, urllib.request
vault, path, ip = sys.argv[1:]
root = json.loads(pathlib.Path(f".private/k8s/vault-{vault}-init.json").read_text())["root_token"]
request = urllib.request.Request(f"http://vault-{vault}.{ip}.sslip.io:31080/v1/{path}", headers={"X-Vault-Token": root})
data = json.load(urllib.request.urlopen(request, timeout=20))["data"]
print(data["access_token"])
print(f"# {data['token_name']} ({data['access_level']}, {data['scopes']}, expires {data['expires_at']})", file=sys.stderr)
PYCODE
