#!/bin/bash
set -euo pipefail
# Tidal 压测脚本
# 用法: ./scripts/loadtest.sh [QPS] [DURATION_SECONDS]
# gift_id=1 荧光棒 10 coins → 每个用户 1M coins 可以送 10万次

QPS="${1:-1000}"
DURATION="${2:-30}"
BASE_URL="${3:-http://localhost:8080}"
TARGET_FILE="/tmp/tidal_targets_${QPS}.json"
RESULT_FILE="/tmp/tidal_result_${QPS}.bin"

TOTAL=$((QPS * DURATION))

echo "=== Tidal Load Test ==="
echo "Target QPS:  $QPS"
echo "Duration:    ${DURATION}s"
echo "Total reqs:  $TOTAL"
echo "Gift:        荧光棒 (10 coins)"
echo ""

echo "Generating $TOTAL targets..."
go run scripts/gen_targets.go "$TOTAL" > "$TARGET_FILE"
echo "Done ($(wc -l < "$TARGET_FILE") lines)"

echo ""
echo "--- Attack ---"
vegeta attack -format=json -rate="$QPS" -duration="${DURATION}s" -targets="$TARGET_FILE" \
  | tee "$RESULT_FILE" \
  | vegeta report -type=text

echo ""
echo "--- Latency Histogram (ms) ---"
vegeta report -type='hist[0,2ms,5ms,10ms,20ms,50ms,100ms,200ms,500ms,1s]' "$RESULT_FILE"
