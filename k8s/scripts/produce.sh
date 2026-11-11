#!/usr/bin/env bash
# Send one test event to the Kafka topic of a NiFi instance (via kubectl exec).
#   bash k8s/scripts/produce.sh stag|vn|indo [event_id]
source "$(dirname "$0")/common.sh"
market="${1:?Usage: produce.sh stag|vn|indo [event_id]}"
ns="$(market_kafka_ns "$market")" || { echo 'Use stag, vn or indo' >&2; exit 1; }
topic="$(market_topic "$market")"
id="${2:-$market-$(date +%s%N)}"
python3 - "$id" <<'PYCODE' | kubectl -n "$ns" exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$topic"
import datetime, json, sys
print(json.dumps(dict(event_id=sys.argv[1], customer_id="C001", amount=125.50,
                      event_time=datetime.datetime.now(datetime.timezone.utc).isoformat())))
PYCODE
printf 'Sent event_id=%s to %s/%s\n' "$id" "$ns" "$topic"
