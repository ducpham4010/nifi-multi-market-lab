#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
case "${1:?Usage: query.sh stag|vn|indo}" in
 stag) db=vn; port=15432; schema=staging;;
 vn) db=vn; port=15432; schema=market;;
 indo) db=indo; port=25432; schema=market;;
 *) exit 1;;
esac
docker compose exec -T -e PGPASSWORD="$DB_PASSWORD" "postgres-$db" \
 psql -h "$HOST_IP" -p "$port" -U admin -d market \
 -c "SELECT * FROM $schema.trip_events ORDER BY event_id;"
