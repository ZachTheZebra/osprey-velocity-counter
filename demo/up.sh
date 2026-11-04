#!/bin/bash
# Build the Osprey worker and run the two-event demo. Requires Docker.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OSPREY="${OSPREY_DIR:-$HOME/.cache/osprey-velocity-demo}"
if [ ! -f "$OSPREY/osprey_worker/Dockerfile" ]; then
  git clone --depth 1 https://github.com/roostorg/osprey.git "$OSPREY"
fi
export OSPREY_DIR="$OSPREY"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
else
  COMPOSE=(docker-compose)
fi
"${COMPOSE[@]}" -f "$ROOT/demo/docker-compose.yaml" up --build --abort-on-container-exit --exit-code-from worker
