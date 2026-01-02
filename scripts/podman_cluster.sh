#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-unirust-distributed}"
NETWORK="${NETWORK:-unirust-net}"
SHARDS="${SHARDS:-2}"
ROUTER_PORT="${ROUTER_PORT:-50060}"
ONTOLOGY="${ONTOLOGY:-}"

action="${1:-start}"

build_image() {
  if ! podman image exists "$IMAGE"; then
    podman build -t "$IMAGE" -f Containerfile .
  fi
}

start_cluster() {
  build_image
  if ! podman network inspect "$NETWORK" >/dev/null 2>&1; then
    podman network create "$NETWORK"
  fi

  podman rm -f unirust-router >/dev/null 2>&1 || true
  for i in $(seq 0 $((SHARDS - 1))); do
    podman rm -f "unirust-shard-$i" >/dev/null 2>&1 || true
  done

  for i in $(seq 0 $((SHARDS - 1))); do
    ontology_args=()
    if [[ -n "$ONTOLOGY" ]]; then
      ontology_args=(--ontology "$ONTOLOGY")
    fi
    podman run -d \
      --name "unirust-shard-$i" \
      --network "$NETWORK" \
      "$IMAGE" \
      unirust_shard \
      --listen "0.0.0.0:50061" \
      --shard-id "$i" \
      "${ontology_args[@]}"
  done

  shard_list=""
  for i in $(seq 0 $((SHARDS - 1))); do
    entry="unirust-shard-$i:50061"
    if [[ -z "$shard_list" ]]; then
      shard_list="$entry"
    else
      shard_list="$shard_list,$entry"
    fi
  done

  ontology_args=()
  if [[ -n "$ONTOLOGY" ]]; then
    ontology_args=(--ontology "$ONTOLOGY")
  fi
  podman run -d \
    --name unirust-router \
    --network "$NETWORK" \
    -p "$ROUTER_PORT:50060" \
    "$IMAGE" \
    unirust_router \
    --listen "0.0.0.0:50060" \
    --shards "$shard_list" \
    "${ontology_args[@]}"

  echo "Cluster started."
  echo "Router listening on localhost:${ROUTER_PORT}"
}

stop_cluster() {
  podman rm -f -t 3 unirust-router >/dev/null 2>&1 || true
  for container in $(podman ps -a --format "{{.Names}}" | grep -E "^unirust-shard-" || true); do
    podman rm -f -t 3 "$container" >/dev/null 2>&1 || true
  done
  echo "Cluster stopped."
}

status_cluster() {
  podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "unirust-(router|shard)"
}

case "$action" in
  build)
    podman build -t "$IMAGE" -f Containerfile .
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
    echo "Usage: $0 [build|start|stop|status]"
    exit 1
    ;;
esac
