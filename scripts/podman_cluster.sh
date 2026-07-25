#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-unirust-distributed}"
NETWORK="${NETWORK:-unirust-net}"
SHARDS="${SHARDS:-2}"
ROUTER_PORT="${ROUTER_PORT:-50060}"
ONTOLOGY="${ONTOLOGY:-$ROOT_DIR/examples/loadtest-ontology.json}"
CONFIG_VERSION="${CONFIG_VERSION:-}"
PROFILE="${PROFILE:-high-throughput}"
CHECKPOINT_INTERVAL_SECS="${CHECKPOINT_INTERVAL_SECS:-3600}"
CONTAINER_PREFIX="${CONTAINER_PREFIX:-unirust}"
VOLUME_PREFIX="${VOLUME_PREFIX:-unirust-data}"
BACKUP_VOLUME_PREFIX="${BACKUP_VOLUME_PREFIX:-unirust-backup}"
STOP_WAIT_SECS="${STOP_WAIT_SECS:-10}"
UNIRUST_CONFIRM_RESET="${UNIRUST_CONFIRM_RESET:-0}"

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

  if podman container exists "${CONTAINER_PREFIX}-router"; then
    echo "Cluster containers already exist. Use '$0 restart' or '$0 stop' first."
    return 1
  fi
  for i in $(seq 0 $((SHARDS - 1))); do
    if podman container exists "${CONTAINER_PREFIX}-shard-$i"; then
      echo "Cluster containers already exist. Use '$0 restart' or '$0 stop' first."
      return 1
    fi
  done

  ontology_mount=()
  ontology_args=()
  if [[ -n "$ONTOLOGY" ]]; then
    if [[ ! -f "$ONTOLOGY" ]]; then
      echo "Ontology file does not exist: $ONTOLOGY"
      return 1
    fi
    ontology_mount=(-v "$ONTOLOGY:/etc/unirust/ontology.json:ro")
    ontology_args=(--ontology /etc/unirust/ontology.json)
  fi

  version_args=()
  if [[ -n "$CONFIG_VERSION" ]]; then
    version_args=(--config-version "$CONFIG_VERSION")
  fi

  for i in $(seq 0 $((SHARDS - 1))); do
    podman run -d \
      --name "${CONTAINER_PREFIX}-shard-$i" \
      --network "$NETWORK" \
      --restart=unless-stopped \
      -v "${VOLUME_PREFIX}-shard-$i:/data" \
      -v "${BACKUP_VOLUME_PREFIX}-shard-$i:/backup" \
      "${ontology_mount[@]}" \
      "$IMAGE" \
      shard \
      --shard-id "$i" \
      --data-dir /data \
      --backup-dir /backup \
      --profile "$PROFILE" \
      "${ontology_args[@]}" \
      "${version_args[@]}"
  done

  shard_list=""
  for i in $(seq 0 $((SHARDS - 1))); do
    entry="${CONTAINER_PREFIX}-shard-$i:50061"
    if [[ -z "$shard_list" ]]; then
      shard_list="$entry"
    else
      shard_list="$shard_list,$entry"
    fi
  done

  podman run -d \
    --name "${CONTAINER_PREFIX}-router" \
    --network "$NETWORK" \
    --restart=unless-stopped \
    -p "$ROUTER_PORT:50060" \
    "${ontology_mount[@]}" \
    "$IMAGE" \
    router \
    --shards "$shard_list" \
    --checkpoint-interval-secs "$CHECKPOINT_INTERVAL_SECS" \
    "${ontology_args[@]}" \
    "${version_args[@]}"

  echo "Cluster started."
  echo "Router listening on localhost:${ROUTER_PORT}"
  echo "Persistent volumes use prefix: ${VOLUME_PREFIX}-shard-"
  echo "Checkpoint volumes use prefix: ${BACKUP_VOLUME_PREFIX}-shard-"
}

stop_cluster() {
  containers=("${CONTAINER_PREFIX}-router")
  while IFS= read -r container; do
    [[ -n "$container" ]] && containers+=("$container")
  done < <(
    podman ps -a --format "{{.Names}}" |
      while IFS= read -r name; do
        [[ "$name" == "${CONTAINER_PREFIX}-shard-"* ]] && printf '%s\n' "$name"
      done
  )

  for container in "${containers[@]}"; do
    if podman container exists "$container"; then
      podman stop --time "$STOP_WAIT_SECS" "$container" >/dev/null 2>&1 || true
      podman rm "$container" >/dev/null 2>&1 || true
    fi
  done
  echo "Cluster stopped."
}

reset_cluster() {
  if [[ "$UNIRUST_CONFIRM_RESET" != "1" ]]; then
    echo "Refusing to delete persistent volumes without UNIRUST_CONFIRM_RESET=1."
    return 1
  fi
  stop_cluster
  while IFS= read -r volume; do
    if [[ "$volume" == "${VOLUME_PREFIX}-shard-"* || "$volume" == "${BACKUP_VOLUME_PREFIX}-shard-"* ]]; then
      podman volume rm "$volume"
    fi
  done < <(podman volume ls --format "{{.Name}}")
  echo "Deleted persistent volumes with prefix: ${VOLUME_PREFIX}-shard-"
  echo "Deleted checkpoint volumes with prefix: ${BACKUP_VOLUME_PREFIX}-shard-"
}

status_cluster() {
  podman ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" |
    while IFS= read -r line; do
      [[ "$line" == NAMES* || "$line" == "${CONTAINER_PREFIX}-router"* || "$line" == "${CONTAINER_PREFIX}-shard-"* ]] &&
        printf '%s\n' "$line"
    done
}

case "$action" in
  build)
    podman build -t "$IMAGE" -f Containerfile .
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
    echo "Usage: $0 [build|start|restart|stop|status|reset]"
    exit 1
    ;;
esac
