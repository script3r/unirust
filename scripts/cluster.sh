#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$ROOT_DIR/target/release}"
ACTION="${1:-start}"

SHARDS="${SHARDS:-3}"
ROUTER_PORT="${ROUTER_PORT:-50060}"
SHARD_PORT_BASE="${SHARD_PORT_BASE:-50061}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/cluster_data}"
BACKUP_DIR="${BACKUP_DIR:-$ROOT_DIR/cluster_backups}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/cluster_logs}"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.cluster}"
ONTOLOGY="${ONTOLOGY:-$ROOT_DIR/examples/loadtest-ontology.json}"
CONFIG_VERSION="${CONFIG_VERSION:-}"
CHECKPOINT_INTERVAL_SECS="${CHECKPOINT_INTERVAL_SECS:-0}"
PROFILE="${PROFILE:-high-throughput}"
REPAIR="${REPAIR:-0}"
CARGO_FEATURES="${CARGO_FEATURES:-}"
SHARD_WAIT_SECS="${SHARD_WAIT_SECS:-10}"
STOP_WAIT_SECS="${STOP_WAIT_SECS:-10}"
UNIRUST_CONFIRM_RESET="${UNIRUST_CONFIRM_RESET:-0}"

usage() {
  cat <<EOF
Usage: $0 [start|restart|stop|status|reset|build]

Environment:
  SHARDS=3
  ROUTER_PORT=50060
  SHARD_PORT_BASE=50061
  DATA_DIR=$ROOT_DIR/cluster_data
  BACKUP_DIR=$ROOT_DIR/cluster_backups
  LOG_DIR=$ROOT_DIR/cluster_logs
  RUN_DIR=$ROOT_DIR/.cluster
  ONTOLOGY=/path/to/ontology.json
    Default: $ROOT_DIR/examples/loadtest-ontology.json
  CONFIG_VERSION=optional-version-string
  CHECKPOINT_INTERVAL_SECS=0  Automatic coordinated checkpoint interval (0 disables)
  PROFILE=balanced|low-latency|high-throughput|bulk-ingest|memory-saver|billion-scale
  REPAIR=0|1
  CARGO_FEATURES=comma-separated-cargo-features
  SHARD_WAIT_SECS=10
  STOP_WAIT_SECS=10
  UNIRUST_CONFIRM_RESET=1  Required for the destructive reset action
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

pid_matches() {
  local pid_file="$1"
  local expected_binary="$2"
  [[ -f "$pid_file" ]] || return 1

  local pid
  read -r pid <"$pid_file" || return 1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1 || return 1

  local command
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"$expected_binary"* ]]
}

assert_cluster_stopped() {
  if pid_matches "$RUN_DIR/router.pid" "unirust_router"; then
    echo "Router is already running. Use '$0 restart' or '$0 stop' first."
    return 1
  fi

  local pid_file
  for pid_file in "$RUN_DIR"/shard-*.pid; do
    [[ -f "$pid_file" ]] || continue
    if pid_matches "$pid_file" "unirust_shard"; then
      echo "$(basename "$pid_file" .pid) is already running. Use '$0 restart' or '$0 stop' first."
      return 1
    fi
  done
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
  assert_cluster_stopped
  build_bins
  mkdir -p "$DATA_DIR" "$BACKUP_DIR" "$LOG_DIR" "$RUN_DIR"

  local shard_list=""
  local shard_ports=()
  for i in $(seq 0 $((SHARDS - 1))); do
    local port=$((SHARD_PORT_BASE + i))
    local shard_dir="$DATA_DIR/shard-$i"
    local backup_dir="$BACKUP_DIR/shard-$i"
    mkdir -p "$shard_dir"
    mkdir -p "$backup_dir"
    local shard_args=(--listen "127.0.0.1:${port}" --shard-id "$i" --data-dir "$shard_dir" --backup-dir "$backup_dir")
    if [[ -n "$ONTOLOGY" ]]; then
      shard_args+=(--ontology "$ONTOLOGY")
    fi
    if [[ -n "$CONFIG_VERSION" ]]; then
      shard_args+=(--config-version "$CONFIG_VERSION")
    fi
    if [[ -n "$PROFILE" ]]; then
      shard_args+=(--profile "$PROFILE")
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
      stop_cluster
      return 1
    fi
  done

  local router_args=(--listen "127.0.0.1:${ROUTER_PORT}" --shards "$shard_list")
  if [[ -n "$ONTOLOGY" ]]; then
    router_args+=(--ontology "$ONTOLOGY")
  fi
  if [[ -n "$CONFIG_VERSION" ]]; then
    router_args+=(--config-version "$CONFIG_VERSION")
  fi
  router_args+=(--checkpoint-interval-secs "$CHECKPOINT_INTERVAL_SECS")

  "$BIN_DIR/unirust_router" "${router_args[@]}" >"$LOG_DIR/router.log" 2>&1 &
  echo $! >"$RUN_DIR/router.pid"

  if ! wait_for_port "127.0.0.1" "$ROUTER_PORT" "$SHARD_WAIT_SECS"; then
    echo "Router on port ${ROUTER_PORT} failed to start within ${SHARD_WAIT_SECS}s."
    stop_cluster
    return 1
  fi

  echo "Cluster started with ${SHARDS} shards."
  echo "Router listening on 127.0.0.1:${ROUTER_PORT}"
  echo "Persistent data directory: $DATA_DIR"
}

stop_pid_file() {
  local pid_file="$1"
  local expected_binary="$2"
  [[ -f "$pid_file" ]] || return 0

  if ! pid_matches "$pid_file" "$expected_binary"; then
    echo "Removing stale or mismatched PID file: $pid_file"
    rm -f "$pid_file"
    return 0
  fi

  local pid
  read -r pid <"$pid_file"
  kill -TERM "$pid" >/dev/null 2>&1 || true

  local attempts=$((STOP_WAIT_SECS * 10))
  for ((attempt = 0; attempt < attempts; attempt++)); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$pid_file"
      return 0
    fi
    sleep 0.1
  done

  echo "$expected_binary did not stop within ${STOP_WAIT_SECS}s; sending SIGKILL."
  kill -KILL "$pid" >/dev/null 2>&1 || true
  rm -f "$pid_file"
}

stop_cluster() {
  if [[ ! -d "$RUN_DIR" ]]; then
    echo "No PID directory found."
    return 0
  fi

  stop_pid_file "$RUN_DIR/router.pid" "unirust_router"
  local pid_file
  for pid_file in "$RUN_DIR"/shard-*.pid; do
    [[ -f "$pid_file" ]] || continue
    stop_pid_file "$pid_file" "unirust_shard"
  done

  echo "Cluster stopped."
}

reset_cluster() {
  assert_cluster_stopped
  if [[ "$UNIRUST_CONFIRM_RESET" != "1" ]]; then
    echo "Refusing to delete persistent data without UNIRUST_CONFIRM_RESET=1."
    return 1
  fi
  remove_data_dir
  echo "Deleted persistent data directory: $DATA_DIR"
}

status_cluster() {
  if [[ ! -d "$RUN_DIR" ]]; then
    echo "No PID directory found."
    return 0
  fi

  local found=0
  local pid_file
  for pid_file in "$RUN_DIR"/router.pid "$RUN_DIR"/shard-*.pid; do
    [[ -f "$pid_file" ]] || continue
    local expected_binary="unirust_shard"
    if [[ "$(basename "$pid_file")" == "router.pid" ]]; then
      expected_binary="unirust_router"
    fi
    local pid
    read -r pid <"$pid_file" || pid="invalid"
    if pid_matches "$pid_file" "$expected_binary"; then
      echo "$(basename "$pid_file" .pid) running (pid $pid)"
    else
      echo "$(basename "$pid_file" .pid) not running (stale or mismatched pid $pid)"
    fi
    found=1
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
  restart)
    stop_cluster
    start_cluster
    ;;
  stop)
    stop_cluster
    ;;
  status)
    status_cluster
    ;;
  reset)
    reset_cluster
    ;;
  *)
    usage
    exit 1
    ;;
esac
