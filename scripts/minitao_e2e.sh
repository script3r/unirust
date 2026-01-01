#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MINITAO_DIR="$ROOT_DIR/../pqc-index/minitao"
MINITAO_ADDR=${MINITAO_ADDR:-http://127.0.0.1:50051}
MINITAO_LISTEN=${MINITAO_LISTEN:-127.0.0.1:50051}
LOG_FILE="$ROOT_DIR/minitao_server.log"

if [[ ! -d "$MINITAO_DIR" ]]; then
  echo "minitao directory not found at $MINITAO_DIR" >&2
  exit 1
fi

pushd "$MINITAO_DIR" >/dev/null
MINITAO_ROLE=leader MINITAO_LISTEN="$MINITAO_LISTEN" cargo run --bin minitao-server >"$LOG_FILE" 2>&1 &
MINITAO_PID=$!
popd >/dev/null

cleanup() {
  if kill -0 "$MINITAO_PID" 2>/dev/null; then
    kill "$MINITAO_PID" 2>/dev/null || true
    wait "$MINITAO_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

sleep 2

pushd "$ROOT_DIR" >/dev/null
MINITAO_ADDR="$MINITAO_ADDR" cargo run --example stream_to_minitao_server
popd >/dev/null

echo "minitao e2e completed"
