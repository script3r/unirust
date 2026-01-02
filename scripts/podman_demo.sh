#!/usr/bin/env bash
set -euo pipefail

ROUTER_ADDR="${ROUTER_ADDR:-http://unirust-router:50060}"
IMAGE="${IMAGE:-unirust-distributed}"
NETWORK="${NETWORK:-unirust-net}"

tmp_ontology="$(mktemp)"
trap 'rm -f "$tmp_ontology"' EXIT

cat >"$tmp_ontology" <<'JSON'
{
  "identity_keys": [
    { "name": "email", "attributes": ["email"] }
  ],
  "strong_identifiers": [],
  "constraints": []
}
JSON

podman run --rm \
  --network "$NETWORK" \
  -v "$tmp_ontology:/tmp/ontology.json:ro" \
  "$IMAGE" \
  unirust_client --router "$ROUTER_ADDR" --ontology "/tmp/ontology.json"
