#!/bin/bash
# 04_VERIFY.sh - HFT Architecture Verification Script
#
# This script benchmarks the unirust system to verify the 200% improvement target.
# It runs a 5-shard cluster and measures throughput before and after optimizations.

set -e

# Configuration
DURATION=${DURATION:-120}         # Test duration in seconds
ENTITY_COUNT=${ENTITY_COUNT:-2000000}  # Total entities to ingest
STREAMS=${STREAMS:-16}            # Number of concurrent streams
BATCH_SIZE=${BATCH_SIZE:-2000}    # Records per batch
OVERLAP_PROB=${OVERLAP_PROB:-0.1} # Probability of identity overlap
SEED=${SEED:-42}                  # Random seed for reproducibility
BASELINE_THROUGHPUT=91000         # Baseline from current implementation
TARGET_IMPROVEMENT=2.0            # 200% improvement target

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Working directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Results file
RESULTS_FILE="/tmp/unirust_hft_verification_$(date +%s).txt"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}       UNIRUST HFT ARCHITECTURE VERIFICATION                ${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""
echo -e "Configuration:"
echo -e "  Duration:        ${YELLOW}${DURATION}s${NC}"
echo -e "  Entity Count:    ${YELLOW}${ENTITY_COUNT}${NC}"
echo -e "  Streams:         ${YELLOW}${STREAMS}${NC}"
echo -e "  Batch Size:      ${YELLOW}${BATCH_SIZE}${NC}"
echo -e "  Overlap Prob:    ${YELLOW}${OVERLAP_PROB}${NC}"
echo -e "  Baseline:        ${YELLOW}${BASELINE_THROUGHPUT} rec/sec${NC}"
echo -e "  Target:          ${YELLOW}$(echo "$BASELINE_THROUGHPUT * $TARGET_IMPROVEMENT" | bc) rec/sec${NC}"
echo ""

# Build the project
echo -e "${BLUE}[1/5] Building release binary...${NC}"
cargo build --release 2>&1 | tail -5
echo -e "${GREEN}Build complete.${NC}"
echo ""

# Check if cluster script exists
if [ ! -f "scripts/cluster.sh" ]; then
    echo -e "${RED}Error: scripts/cluster.sh not found${NC}"
    exit 1
fi

# Start the 5-shard cluster
echo -e "${BLUE}[2/5] Starting 5-shard cluster...${NC}"

# Stop any existing cluster
pkill -f "unirust-rs.*--shard" 2>/dev/null || true
sleep 2

# Start the cluster
./scripts/cluster.sh start 5 &
CLUSTER_PID=$!
sleep 10  # Wait for cluster to initialize

# Verify cluster is running
ROUTER_PORT=50060
if ! curl -s "http://127.0.0.1:${ROUTER_PORT}/health" > /dev/null 2>&1; then
    # Try with nc if curl doesn't work
    if ! nc -z 127.0.0.1 $ROUTER_PORT 2>/dev/null; then
        echo -e "${YELLOW}Warning: Could not verify router health, but proceeding...${NC}"
    fi
fi
echo -e "${GREEN}Cluster started.${NC}"
echo ""

# Run the benchmark
echo -e "${BLUE}[3/5] Running load test benchmark...${NC}"
echo ""

START_TIME=$(date +%s%3N)

# Run the load test
./target/release/unirust-rs loadtest \
    --router "http://127.0.0.1:${ROUTER_PORT}" \
    --count "$ENTITY_COUNT" \
    --streams "$STREAMS" \
    --batch-size "$BATCH_SIZE" \
    --overlap "$OVERLAP_PROB" \
    --seed "$SEED" \
    --duration "$DURATION" \
    2>&1 | tee "$RESULTS_FILE"

END_TIME=$(date +%s%3N)
ELAPSED_MS=$((END_TIME - START_TIME))
ELAPSED_SEC=$(echo "scale=2; $ELAPSED_MS / 1000" | bc)

echo ""
echo -e "${GREEN}Load test complete in ${ELAPSED_SEC}s${NC}"
echo ""

# Stop the cluster
echo -e "${BLUE}[4/5] Stopping cluster...${NC}"
./scripts/cluster.sh stop 2>/dev/null || pkill -f "unirust-rs.*--shard" 2>/dev/null || true
sleep 2
echo -e "${GREEN}Cluster stopped.${NC}"
echo ""

# Analyze results
echo -e "${BLUE}[5/5] Analyzing results...${NC}"
echo ""

# Extract throughput from results
THROUGHPUT=$(grep -oP 'Throughput:\s+\K[\d.]+' "$RESULTS_FILE" 2>/dev/null || echo "0")

if [ "$THROUGHPUT" = "0" ] || [ -z "$THROUGHPUT" ]; then
    # Try alternative pattern
    THROUGHPUT=$(grep -oP '[\d,]+(?=\s+rec/sec)' "$RESULTS_FILE" | tr -d ',' | head -1 || echo "0")
fi

if [ "$THROUGHPUT" = "0" ] || [ -z "$THROUGHPUT" ]; then
    echo -e "${RED}Error: Could not extract throughput from results${NC}"
    echo -e "Results file contents:"
    cat "$RESULTS_FILE"
    exit 1
fi

# Calculate improvement
IMPROVEMENT=$(echo "scale=4; $THROUGHPUT / $BASELINE_THROUGHPUT" | bc)
IMPROVEMENT_PCT=$(echo "scale=1; ($IMPROVEMENT - 1) * 100" | bc)
TARGET_THROUGHPUT=$(echo "$BASELINE_THROUGHPUT * $TARGET_IMPROVEMENT" | bc)

echo -e "============================================================"
echo -e "                    VERIFICATION RESULTS                     "
echo -e "============================================================"
echo ""
echo -e "  Baseline:          ${BASELINE_THROUGHPUT} rec/sec"
echo -e "  Measured:          ${THROUGHPUT} rec/sec"
echo -e "  Target:            ${TARGET_THROUGHPUT} rec/sec"
echo ""
echo -e "  Improvement:       ${IMPROVEMENT_PCT}%"
echo ""

# Determine pass/fail
if (( $(echo "$THROUGHPUT >= $TARGET_THROUGHPUT" | bc -l) )); then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}  VERIFICATION PASSED: 200%+ improvement achieved!          ${NC}"
    echo -e "${GREEN}============================================================${NC}"
    EXIT_CODE=0
else
    REMAINING=$(echo "scale=1; (($TARGET_THROUGHPUT - $THROUGHPUT) / $BASELINE_THROUGHPUT) * 100" | bc)
    echo -e "${YELLOW}============================================================${NC}"
    echo -e "${YELLOW}  VERIFICATION INCOMPLETE: ${REMAINING}% more improvement needed${NC}"
    echo -e "${YELLOW}============================================================${NC}"
    echo ""
    echo -e "  Analysis:"
    echo -e "  - Current throughput: ${THROUGHPUT} rec/sec"
    echo -e "  - Gap to target:      $(echo "$TARGET_THROUGHPUT - $THROUGHPUT" | bc) rec/sec"
    echo ""
    echo -e "  Recommendations:"
    echo -e "  1. Enable partitioned processing in ShardNode"
    echo -e "  2. Verify crossbeam-channel is being used for partition communication"
    echo -e "  3. Check that partition count matches CPU core count"
    echo -e "  4. Profile for remaining lock contention"
    EXIT_CODE=1
fi

echo ""
echo -e "Full results saved to: $RESULTS_FILE"
echo ""

# Generate detailed metrics report
echo -e "${BLUE}Detailed Metrics:${NC}"
echo ""
grep -E "(Throughput|Latency|Clusters|Conflicts|Stream)" "$RESULTS_FILE" || true
echo ""

exit $EXIT_CODE
