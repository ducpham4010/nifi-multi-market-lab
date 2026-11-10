#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
market="${1:?Usage: produce.sh stag|vn|indo [event_id]}"
case "$market" in
 stag) broker=vn; port=19092; topic=trips-staging;;
 vn) broker=vn; port=19092; topic=trips;;
 indo) broker=indo; port=29092; topic=trips;;
 *) echo 'Use stag, vn or indo'; exit 1;;
esac
id="${2:-$market-$(date +%s%N)}"
python3 - "$id" <<'PYCODE' | docker compose exec -T "kafka-$broker" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$HOST_IP:$port" --topic "$topic"
import json,sys,datetime
print(json.dumps(dict(event_id=sys.argv[1],customer_id='C001',amount=125.50,event_time=datetime.datetime.now(datetime.timezone.utc).isoformat())))
PYCODE
printf 'Sent event_id=%s to %s/%s\n' "$id" "$market" "$topic"
