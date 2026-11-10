#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
command -v docker >/dev/null
docker compose version
docker compose config --quiet
python3 - "$HOST_IP" <<'PYCODE'
import ipaddress,sys
ipaddress.IPv4Address(sys.argv[1])
PYCODE
if ! ip -4 addr show | grep -Fq " $HOST_IP/"; then echo 'HOST_IP is not assigned to this host' >&2; exit 1; fi
for port in 18080 18022 18443 28443 38443 19092 19093 29092 29093 15432 25432 18200 28200 18000 28000 38000 52020 52021 52022; do
 if ss -H -ltn "sport = :$port" | grep -q .; then echo "Port $port in use; if lab already running this is expected"; fi
done
free -h
df -h /var/lib/docker
