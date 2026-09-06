#!/usr/bin/env bash
# Starts the Kafka broker defined in podman-compose.yml and blocks until it answers API requests.
set -euo pipefail
cd "$(dirname "$0")/.."

podman compose -f podman-compose.yml up -d

echo "Waiting for Kafka on localhost:9092 ..."
for _ in $(seq 1 60); do
  if podman exec fixflow-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka is ready."
    exit 0
  fi
  sleep 2
done

echo "Kafka did not become ready in time. Last log lines:" >&2
podman logs --tail 50 fixflow-kafka >&2 || true
exit 1
