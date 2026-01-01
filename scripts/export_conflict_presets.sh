#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SRC="$ROOT_DIR/src/conflicts.rs"
OUT="$ROOT_DIR/web/presets.json"

if [[ ! -f "$SRC" ]]; then
  echo "missing conflicts.rs at $SRC" >&2
  exit 1
fi

awk '
  /PRESET_JSON_START/ {in_block=1; next}
  /PRESET_JSON_END/ {in_block=0}
  in_block {print}
' "$SRC" | sed '1d;$d' > "$OUT"

cargo run --quiet --bin preset_graph -- "$OUT"
echo "wrote presets with graph data to $OUT"
