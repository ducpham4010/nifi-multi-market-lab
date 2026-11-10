#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
for spec in 'vn 19092 trips' 'vn 19092 trips-staging' 'indo 29092 trips'; do
 read -r market port topic <<< "$spec"
 docker compose exec -T "kafka-$market" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$HOST_IP:$port" --create --if-not-exists --topic "$topic" \
  --partitions 3 --replication-factor 1
done
