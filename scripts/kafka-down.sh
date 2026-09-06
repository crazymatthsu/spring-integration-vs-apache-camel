#!/usr/bin/env bash
# Stops and removes the Kafka broker started by scripts/kafka-up.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
podman compose -f podman-compose.yml down
