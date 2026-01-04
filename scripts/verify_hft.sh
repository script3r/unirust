#!/usr/bin/env bash
#
# SECTION 4: HFT VERIFICATION SCRIPT
#
# Validates HFT optimizations achieve target throughput on 5-shard distributed cluster.
#
# Baseline: ~60K rec/sec (distributed cluster, pre-optimization)
# Target: ≥200% improvement = ≥180K rec/sec
#
# Usage:
#   ./scripts/verify_hft.sh [--quick]
#   ./scripts/verify_hft.sh [--full]
#   ./scripts/verify_hft.sh [--cleanup]
#
# Environment:
#   SHARDS=5               Number of shards (default: 5)
#   ROUTER_PORT=50060      Router gRPC port
#   DURATION_SECS=30       Load test duration
#   NUM_WORKERS=32         Concurrent workers
#   BATCH_SIZE=100         Records per batch

set -euo pipefail

# === Configuration ===
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SHARDS="${SHARDS:-5}"
ROUTER_PORT="${ROUTER_PORT:-50060}"
SHARD_PORT_BASE="${SHARD_PORT_BASE:-50061}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/cluster_data}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/cluster_logs}"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.cluster}"
BIN_DIR="${BIN_DIR:-$ROOT_DIR/target/release}"

# Load test parameters
DURATION_QUICK=15
DURATION_FULL=60
NUM_WORKERS="${NUM_WORKERS:-32}"
BATCH_SIZE="${BATCH_SIZE:-100}"

# Targets
BASELINE_HISTORICAL=60000    # Pre-optimization distributed baseline
TARGET_RPS=180000            # 200% improvement target
TARGET_IMPROVEMENT=200       # Percentage

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# === Functions ===
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
header() { echo -e "\n${BOLD}${BLUE}=== $* ===${NC}\n"; }

cleanup_cluster() {
    info "Stopping existing cluster processes..."
    pkill -f "unirust_shard" 2>/dev/null || true
    pkill -f "unirust_router" 2>/dev/null || true
    rm -rf "$RUN_DIR"/*.pid 2>/dev/null || true
    sleep 1
}

wait_for_port() {
    local host="$1" port="$2" timeout="${3:-30}"
    local start=$(date +%s)
    while ! (echo >/dev/tcp/"$host"/"$port") 2>/dev/null; do
        if (( $(date +%s) - start >= timeout )); then
            return 1
        fi
        sleep 0.1
    done
}

build_release() {
    header "Building Release Binaries"
    cargo build --release --bin unirust_shard --bin unirust_router --features test-support 2>&1 | tail -5
    cargo build --release --bin unirust_loadtest --features test-support 2>&1 | tail -3
    info "Build complete."
}

start_cluster() {
    header "Starting ${SHARDS}-Shard Cluster"

    cleanup_cluster
    mkdir -p "$DATA_DIR" "$LOG_DIR" "$RUN_DIR"

    local shard_list=""
    local shard_ports=()

    for i in $(seq 0 $((SHARDS - 1))); do
        local port=$((SHARD_PORT_BASE + i))
        local shard_dir="$DATA_DIR/shard-$i"
        rm -rf "$shard_dir"
        mkdir -p "$shard_dir"

        "$BIN_DIR/unirust_shard" \
            --listen "127.0.0.1:${port}" \
            --shard-id "$i" \
            --data-dir "$shard_dir" \
            --tuning high-throughput \
            >"$LOG_DIR/shard-$i.log" 2>&1 &
        echo $! >"$RUN_DIR/shard-$i.pid"

        shard_ports+=("$port")
        if [[ -z "$shard_list" ]]; then
            shard_list="127.0.0.1:${port}"
        else
            shard_list="${shard_list},127.0.0.1:${port}"
        fi
        info "Shard $i started on port $port"
    done

    # Wait for shards
    for port in "${shard_ports[@]}"; do
        if ! wait_for_port "127.0.0.1" "$port" 15; then
            error "Shard on port $port failed to start"
            cat "$LOG_DIR/shard-${port}.log" 2>/dev/null || true
            exit 1
        fi
    done

    # Start router
    "$BIN_DIR/unirust_router" \
        --listen "127.0.0.1:${ROUTER_PORT}" \
        --shards "$shard_list" \
        >"$LOG_DIR/router.log" 2>&1 &
    echo $! >"$RUN_DIR/router.pid"

    if ! wait_for_port "127.0.0.1" "$ROUTER_PORT" 10; then
        error "Router failed to start"
        cat "$LOG_DIR/router.log" 2>/dev/null || true
        exit 1
    fi

    info "Router started on port $ROUTER_PORT"
    info "Cluster ready with $SHARDS shards"
}

run_loadtest() {
    local duration="$1"
    local label="${2:-}"

    info "Running loadtest: ${duration}s, ${NUM_WORKERS} workers, batch=${BATCH_SIZE}"

    local result
    result=$("$BIN_DIR/unirust_loadtest" \
        --endpoint "http://127.0.0.1:${ROUTER_PORT}" \
        --duration "$duration" \
        --workers "$NUM_WORKERS" \
        --batch-size "$BATCH_SIZE" \
        --no-tui 2>&1) || {
        error "Loadtest failed"
        echo "$result"
        return 1
    }

    # Extract throughput from final output
    local rps
    rps=$(echo "$result" | grep -oP 'Current:\s*\K[\d,]+' | tail -1 | tr -d ',' || echo "0")

    # Try alternate pattern if needed
    if [[ "$rps" == "0" || -z "$rps" ]]; then
        rps=$(echo "$result" | grep -oP '[\d,]+\s*rec/s' | tail -1 | grep -oP '[\d,]+' | tr -d ',' || echo "0")
    fi

    # One more attempt: parse the summary
    if [[ "$rps" == "0" || -z "$rps" ]]; then
        rps=$(echo "$result" | grep -E "throughput|Throughput" | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
    fi

    echo "$rps"
}

run_hft_tests() {
    header "Running HFT Unit Tests"

    local test_output
    test_output=$(cargo test --release --lib hft:: 2>&1) || {
        error "HFT tests failed"
        echo "$test_output" | tail -30
        return 1
    }

    local passed
    passed=$(echo "$test_output" | grep -oP '\d+ passed' | grep -oP '\d+' || echo "0")
    info "HFT tests: $passed passed"
}

# === Main ===

ACTION="${1:-full}"

case "$ACTION" in
    --cleanup|cleanup)
        cleanup_cluster
        rm -rf "$DATA_DIR"
        info "Cluster data cleaned up"
        exit 0
        ;;
    --quick|quick)
        DURATION=$DURATION_QUICK
        ;;
    --full|full|*)
        DURATION=$DURATION_FULL
        ;;
esac

header "HFT VERIFICATION SCRIPT"
info "Configuration:"
info "  Shards:        $SHARDS"
info "  Router port:   $ROUTER_PORT"
info "  Duration:      ${DURATION}s"
info "  Workers:       $NUM_WORKERS"
info "  Batch size:    $BATCH_SIZE"
info "  Baseline:      $BASELINE_HISTORICAL rec/sec"
info "  Target:        $TARGET_RPS rec/sec (${TARGET_IMPROVEMENT}%)"

# Step 1: Build
build_release

# Step 2: Run unit tests
run_hft_tests

# Step 3: Start cluster
start_cluster

# Let cluster warm up
sleep 2

# Step 4: Run loadtest
header "Running Distributed Load Test"
RESULT_RPS=$(run_loadtest "$DURATION" "hft")

if [[ "$RESULT_RPS" == "0" || -z "$RESULT_RPS" ]]; then
    # Fallback: unable to parse loadtest output
    warn "Could not parse loadtest output"
    RESULT_RPS="unknown"
fi

info "Measured throughput: $RESULT_RPS rec/sec"

# Step 5: Calculate improvement
header "Calculating Results"

PASS=true
IMPROVEMENT=0
IMPROVEMENT_FACTOR="0.00"

if [[ "$RESULT_RPS" != "unknown" && "$RESULT_RPS" =~ ^[0-9]+$ && "$RESULT_RPS" -gt 0 ]]; then
    IMPROVEMENT=$(( (RESULT_RPS - BASELINE_HISTORICAL) * 100 / BASELINE_HISTORICAL ))
    IMPROVEMENT_FACTOR=$(awk "BEGIN {printf \"%.2f\", $RESULT_RPS / $BASELINE_HISTORICAL}")
else
    warn "Could not calculate improvement (result: $RESULT_RPS)"
    RESULT_RPS="N/A"
fi

# Step 6: Report
header "VERIFICATION SUMMARY"

echo "┌────────────────────────────────────────────────────────────┐"
echo "│                    HFT VERIFICATION REPORT                 │"
echo "├────────────────────────────────────────────────────────────┤"
echo "│ Cluster Configuration                                      │"
printf "│   Shards:              %-37s│\n" "$SHARDS"
printf "│   Workers:             %-37s│\n" "$NUM_WORKERS"
printf "│   Duration:            %-37s│\n" "${DURATION}s"
echo "├────────────────────────────────────────────────────────────┤"
echo "│ Throughput Results                                         │"
printf "│   Historical Baseline: %-24s rec/sec │\n" "$BASELINE_HISTORICAL"
printf "│   Measured:            %-24s rec/sec │\n" "$RESULT_RPS"
printf "│   Target:              %-24s rec/sec │\n" "$TARGET_RPS"
echo "├────────────────────────────────────────────────────────────┤"
echo "│ Improvement                                                │"
if [[ "$RESULT_RPS" =~ ^[0-9]+$ ]]; then
    printf "│   Percentage:          %-36s │\n" "${IMPROVEMENT}%"
    printf "│   Factor:              %-36s │\n" "${IMPROVEMENT_FACTOR}x"
else
    printf "│   Percentage:          %-36s │\n" "N/A"
    printf "│   Factor:              %-36s │\n" "N/A"
fi
echo "├────────────────────────────────────────────────────────────┤"
echo "│ Verification Status                                        │"

if [[ "$RESULT_RPS" =~ ^[0-9]+$ ]]; then
    if [[ "$RESULT_RPS" -ge "$TARGET_RPS" ]]; then
        printf "│   %-56s │\n" "${GREEN}✓ TARGET MET: $RESULT_RPS >= $TARGET_RPS${NC}"
    else
        printf "│   %-56s │\n" "${RED}✗ TARGET MISSED: $RESULT_RPS < $TARGET_RPS${NC}"
        PASS=false
    fi

    if [[ "$IMPROVEMENT" -ge "$TARGET_IMPROVEMENT" ]]; then
        printf "│   %-56s │\n" "${GREEN}✓ IMPROVEMENT MET: ${IMPROVEMENT}% >= ${TARGET_IMPROVEMENT}%${NC}"
    else
        printf "│   %-56s │\n" "${RED}✗ IMPROVEMENT MISSED: ${IMPROVEMENT}% < ${TARGET_IMPROVEMENT}%${NC}"
        PASS=false
    fi
else
    printf "│   %-56s │\n" "${YELLOW}? Could not verify (no data)${NC}"
    PASS=false
fi

echo "└────────────────────────────────────────────────────────────┘"
echo

# Step 7: Cleanup
header "Cleanup"
cleanup_cluster
info "Cluster stopped"

# Final verdict
echo
if $PASS; then
    echo -e "${GREEN}${BOLD}========================================${NC}"
    echo -e "${GREEN}${BOLD}  VERIFICATION PASSED                   ${NC}"
    echo -e "${GREEN}${BOLD}  HFT optimizations achieve ≥200%       ${NC}"
    echo -e "${GREEN}${BOLD}  improvement on distributed cluster    ${NC}"
    echo -e "${GREEN}${BOLD}========================================${NC}"
    exit 0
else
    echo -e "${RED}${BOLD}========================================${NC}"
    echo -e "${RED}${BOLD}  VERIFICATION FAILED                   ${NC}"
    echo -e "${RED}${BOLD}  Target: $TARGET_RPS rec/sec           ${NC}"
    echo -e "${RED}${BOLD}  Actual: $RESULT_RPS rec/sec           ${NC}"
    echo -e "${RED}${BOLD}========================================${NC}"
    exit 1
fi
