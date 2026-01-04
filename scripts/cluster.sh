#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$ROOT_DIR/target/release}"
ACTION="${1:-start}"

SHARDS="${SHARDS:-5}"
ROUTER_PORT="${ROUTER_PORT:-50060}"
SHARD_PORT_BASE="${SHARD_PORT_BASE:-50061}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/cluster_data}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/cluster_logs}"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.cluster}"
ONTOLOGY="${ONTOLOGY:-}"
CONFIG_VERSION="${CONFIG_VERSION:-}"
TUNING="${TUNING:-}"
REPAIR="${REPAIR:-0}"
CARGO_FEATURES="${CARGO_FEATURES:-}"
SHARD_WAIT_SECS="${SHARD_WAIT_SECS:-10}"

usage() {
  cat <<EOF
Usage: $0 [start|stop|status|build]

Environment:
  SHARDS=5
  ROUTER_PORT=50060
  SHARD_PORT_BASE=50061
  DATA_DIR=$ROOT_DIR/cluster_data
  LOG_DIR=$ROOT_DIR/cluster_logs
  RUN_DIR=$ROOT_DIR/.cluster
  ONTOLOGY=/path/to/ontology.json
  CONFIG_VERSION=optional-version-string
  TUNING=balanced|low-latency|high-throughput|bulk-ingest|memory-saver
  REPAIR=0|1
  CARGO_FEATURES=comma-separated-cargo-features
  SHARD_WAIT_SECS=10
EOF
}

build_bins() {
  local build_cmd=(cargo build --release --bin unirust_shard --bin unirust_router)
  if [[ -n "$CARGO_FEATURES" ]]; then
    build_cmd+=(--features "$CARGO_FEATURES")
  fi
  "${build_cmd[@]}"
}

guard_data_dir() {
  if [[ -z "$DATA_DIR" || "$DATA_DIR" == "/" ]]; then
    echo "Refusing to remove DATA_DIR='$DATA_DIR'"
    exit 1
  fi
}

remove_data_dir() {
  guard_data_dir
  if [[ -e "$DATA_DIR" ]]; then
    rm -rf "$DATA_DIR"
  fi
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_secs="$3"
  local start
  start="$(date +%s)"
  while true; do
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    if [[ $(( $(date +%s) - start )) -ge "$timeout_secs" ]]; then
      return 1
    fi
    sleep 0.1
  done
}

start_cluster() {
  build_bins
  remove_data_dir
  mkdir -p "$DATA_DIR" "$LOG_DIR" "$RUN_DIR"

  local shard_list=""
  local shard_ports=()
  for i in $(seq 0 $((SHARDS - 1))); do
    local port=$((SHARD_PORT_BASE + i))
    local shard_dir="$DATA_DIR/shard-$i"
    mkdir -p "$shard_dir"
    local shard_args=(--listen "127.0.0.1:${port}" --shard-id "$i" --data-dir "$shard_dir")
    if [[ -n "$ONTOLOGY" ]]; then
      shard_args+=(--ontology "$ONTOLOGY")
    fi
    if [[ -n "$CONFIG_VERSION" ]]; then
      shard_args+=(--config-version "$CONFIG_VERSION")
    fi
    if [[ -n "$TUNING" ]]; then
      shard_args+=(--tuning "$TUNING")
    fi
    if [[ "$REPAIR" == "1" ]]; then
      shard_args+=(--repair)
    fi

    "$BIN_DIR/unirust_shard" "${shard_args[@]}" >"$LOG_DIR/shard-$i.log" 2>&1 &
    echo $! >"$RUN_DIR/shard-$i.pid"
    echo "Shard $i listening on 127.0.0.1:${port}"
    shard_ports+=("$port")

    local entry="127.0.0.1:${port}"
    if [[ -z "$shard_list" ]]; then
      shard_list="$entry"
    else
      shard_list="$shard_list,$entry"
    fi
  done

  for port in "${shard_ports[@]}"; do
    if ! wait_for_port "127.0.0.1" "$port" "$SHARD_WAIT_SECS"; then
      echo "Shard on port ${port} failed to start within ${SHARD_WAIT_SECS}s."
      exit 1
    fi
  done

  local router_args=(--listen "127.0.0.1:${ROUTER_PORT}" --shards "$shard_list")
  if [[ -n "$ONTOLOGY" ]]; then
    router_args+=(--ontology "$ONTOLOGY")
  fi
  if [[ -n "$CONFIG_VERSION" ]]; then
    router_args+=(--config-version "$CONFIG_VERSION")
  fi

  "$BIN_DIR/unirust_router" "${router_args[@]}" >"$LOG_DIR/router.log" 2>&1 &
  echo $! >"$RUN_DIR/router.pid"

  echo "Cluster started with ${SHARDS} shards."
  echo "Router listening on 127.0.0.1:${ROUTER_PORT}"
}

stop_cluster() {
  pkill -f "/unirust_shard" >/dev/null 2>&1 || true
  pkill -f "/unirust_router" >/dev/null 2>&1 || true
  if [[ ! -d "$RUN_DIR" ]]; then
    echo "No PID directory found."
    return 0
  fi

  for pid_file in "$RUN_DIR"/shard-*.pid "$RUN_DIR"/router.pid; do
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file")"
      if ps -p "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
      rm -f "$pid_file"
    fi
  done

  echo "Cluster stopped."
}

status_cluster() {
  if [[ ! -d "$RUN_DIR" ]]; then
    echo "No PID directory found."
    return 0
  fi

  local found=0
  for pid_file in "$RUN_DIR"/shard-*.pid "$RUN_DIR"/router.pid; do
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file")"
      if ps -p "$pid" >/dev/null 2>&1; then
        echo "$(basename "$pid_file" .pid) running (pid $pid)"
        found=1
      else
        echo "$(basename "$pid_file" .pid) not running (stale pid $pid)"
        found=1
      fi
    fi
  done

  if [[ "$found" -eq 0 ]]; then
    echo "No PID files found."
  fi
}

case "$ACTION" in
  build)
    build_bins
    ;;
  start)
    start_cluster
    ;;
  stop)
    stop_cluster
    ;;
  status)
    status_cluster
    ;;
  *)
    usage
    exit 1
    ;;
esac
