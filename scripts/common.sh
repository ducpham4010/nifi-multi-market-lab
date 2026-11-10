#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ ! -f .env ]]; then echo 'Copy .env.example to .env and edit HOST_IP first.' >&2; exit 1; fi
set -a
source .env
set +a
[[ "$HOST_IP" != '0.0.0.0' && "$HOST_IP" != '127.0.0.1' ]] || { echo 'Use LAN IP'; exit 1; }
