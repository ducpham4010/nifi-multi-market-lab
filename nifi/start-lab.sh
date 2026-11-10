#!/usr/bin/env bash
set -euo pipefail

# NiFi's automatic management-port selection races when multiple instances
# start together on the host network. Set a distinct loopback URI per instance.
port="${NIFI_MANAGEMENT_PORT:?Set a unique NIFI_MANAGEMENT_PORT per instance}"
case "$port" in
  ''|*[!0-9]*) echo 'NIFI_MANAGEMENT_PORT must be numeric' >&2; exit 1 ;;
esac
if (( port < 1024 || port > 65535 )); then
  echo 'NIFI_MANAGEMENT_PORT must be between 1024 and 65535' >&2
  exit 1
fi
bootstrap="${NIFI_HOME:-/opt/nifi/nifi-current}/conf/bootstrap.conf"
sed -i '/^management\.server\.address=/d' "$bootstrap"
printf '\nmanagement.server.address=http://127.0.0.1:%s\n' "$port" >> "$bootstrap"
python3 /opt/nifi/scripts/configure-browser-tls.py
exec /opt/nifi/scripts/start.sh
