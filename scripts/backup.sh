#!/usr/bin/env bash
# Stops services for a consistent lab-wide backup. Maintenance window required.
source "$(dirname "$0")/common.sh"
mkdir -p backups
chmod 700 backups
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$(pwd)/backups/$stamp"
mkdir -m 700 "$out"
docker pull alpine:3.21 >/dev/null
mapfile -t volumes < <(docker volume ls -q --filter label=com.docker.compose.project=nifi-market-lab)
[[ ${#volumes[@]} -gt 0 ]] || { echo 'No volumes'; exit 1; }
docker compose stop
trap 'docker compose start' EXIT
for volume in "${volumes[@]}"; do
 docker run --rm -v "$volume:/data:ro" -v "$out:/backup:Z" alpine:3.21 \
  tar czf "/backup/$volume.tgz" -C /data .
done
cp .env "$out/env.private"
cp -r .private "$out/" 2>/dev/null || true
printf 'Backup: %s; Vault must be unsealed after restart.\n' "$out"
