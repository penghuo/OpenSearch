#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/distribution/archives/linux-tar/build/install/opensearch-3.6.0-SNAPSHOT"
OSB="/home/penghuo/osb.sh"
RESULTS_DIR="benchmark-results"

mkdir -p "$RESULTS_DIR"

# ── 1. Clean previous data and start OpenSearch ──────────────────────────────
echo "=== Cleaning previous data ==="
rm -rf "$OS_HOME/data"

echo "=== Starting OpenSearch ==="
OPENSEARCH_JAVA_OPTS="-Xms16g -Xmx16g" \
    "$OS_HOME/bin/opensearch" \
    -Eopensearch.experimental.feature.parquet_doc_values.enabled=true \
    -Ediscovery.type=single-node \
    -d -p opensearch.pid

echo "=== Waiting for cluster green ==="
for i in $(seq 1 120); do
    STATUS=$(curl -s localhost:9200/_cluster/health 2>/dev/null | grep -oP '"status"\s*:\s*"\K[^"]+' || true)
    if [ "$STATUS" = "green" ] || [ "$STATUS" = "yellow" ]; then
        echo "Cluster is $STATUS"
        break
    fi
    sleep 5
done

# ── 2. Baseline benchmark ────────────────────────────────────────────────────
echo "=== Running baseline benchmark ==="
$OSB execute-test \
    --workload http_logs \
    --target-hosts localhost:9200 \
    --pipeline benchmark-only \
    --test-procedure append-no-conflicts \
    --workload-params="number_of_replicas:0" \
    --user-tag="config:baseline" \
    --kill-running-processes \
    2>&1 | tee "$RESULTS_DIR/baseline-output.txt"

# ── 3. Capture baseline storage ──────────────────────────────────────────────
echo "=== Capturing baseline storage ==="
curl -s "localhost:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" | tee "$RESULTS_DIR/baseline-store-size.txt"
BASELINE_BYTES=$(curl -s "localhost:9200/_cat/indices?h=store.size&bytes=b" | awk '{sum+=$1} END {print sum}')
echo "" >> "$RESULTS_DIR/baseline-store-size.txt"
echo "Total store size: $BASELINE_BYTES bytes" >> "$RESULTS_DIR/baseline-store-size.txt"
echo "Baseline bytes: $BASELINE_BYTES"

# ── 4. Parquet benchmark ─────────────────────────────────────────────────────
echo "=== Running parquet benchmark ==="
$OSB execute-test \
    --workload http_logs \
    --target-hosts localhost:9200 \
    --pipeline benchmark-only \
    --test-procedure append-no-conflicts \
    --workload-params='number_of_replicas:0,index_settings:{"index.codec.doc_values.format":"parquet"}' \
    --user-tag="config:parquet" \
    --kill-running-processes \
    2>&1 | tee "$RESULTS_DIR/parquet-output.txt"

# ── 5. Capture parquet storage ───────────────────────────────────────────────
echo "=== Capturing parquet storage ==="
curl -s "localhost:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" | tee "$RESULTS_DIR/parquet-store-size.txt"
PARQUET_BYTES=$(curl -s "localhost:9200/_cat/indices?h=store.size&bytes=b" | awk '{sum+=$1} END {print sum}')
echo "" >> "$RESULTS_DIR/parquet-store-size.txt"
echo "Total store size: $PARQUET_BYTES bytes" >> "$RESULTS_DIR/parquet-store-size.txt"
echo "Parquet bytes: $PARQUET_BYTES"

# ── 6. Stop OpenSearch ───────────────────────────────────────────────────────
echo "=== Stopping OpenSearch ==="
kill "$(cat opensearch.pid)" 2>/dev/null || true
sleep 5

# ── 7. Compare runs ─────────────────────────────────────────────────────────
BASELINE_ID=$(grep -oP 'Test run ID: \K[a-f0-9-]+' "$RESULTS_DIR/baseline-output.txt" | tail -1)
PARQUET_ID=$(grep -oP 'Test run ID: \K[a-f0-9-]+' "$RESULTS_DIR/parquet-output.txt" | tail -1)
echo "Baseline run: $BASELINE_ID"
echo "Parquet run:  $PARQUET_ID"

$OSB compare \
    --baseline "$BASELINE_ID" \
    --contender "$PARQUET_ID" \
    --results-file "$RESULTS_DIR/comparison.md" \
    --results-format markdown

# ── 8. Generate summary ─────────────────────────────────────────────────────
echo "=== Generating summary ==="

# Check correctness (no errors in parquet output)
PARQUET_ERRORS=$(grep -ci "error\|exception\|fatal" "$RESULTS_DIR/parquet-output.txt" || true)
PARQUET_SUCCESS=$(grep -c "SUCCESS" "$RESULTS_DIR/parquet-output.txt" || true)

# Storage ratio
if [ "$BASELINE_BYTES" -gt 0 ] 2>/dev/null; then
    STORAGE_PCT=$(awk "BEGIN {printf \"%.1f\", ($PARQUET_BYTES / $BASELINE_BYTES) * 100}")
    STORAGE_REDUCTION=$(awk "BEGIN {printf \"%.1f\", 100 - ($PARQUET_BYTES / $BASELINE_BYTES) * 100}")
else
    STORAGE_PCT="N/A"
    STORAGE_REDUCTION="N/A"
fi

cat > "$RESULTS_DIR/summary.md" <<EOF
# Benchmark Summary

## Storage
- Baseline: $BASELINE_BYTES bytes
- Parquet:  $PARQUET_BYTES bytes
- Ratio:    ${STORAGE_PCT}% of baseline
- Reduction: ${STORAGE_REDUCTION}%

## Correctness
- Parquet errors: $PARQUET_ERRORS
- Parquet success: $PARQUET_SUCCESS

## Pass/Fail
| Criterion | Target | Result | Status |
|-----------|--------|--------|--------|
| Correctness | 0 errors | $PARQUET_ERRORS errors | $([ "$PARQUET_SUCCESS" -ge 1 ] && [ "$PARQUET_ERRORS" -eq 0 ] && echo "✅ PASS" || echo "❌ FAIL") |
| Storage | ≤30% of baseline | ${STORAGE_PCT}% | $(awk "BEGIN {exit ($PARQUET_BYTES <= $BASELINE_BYTES * 0.30) ? 0 : 1}" 2>/dev/null && echo "✅ PASS" || echo "❌ FAIL") |
| Ingestion | ≥2x baseline | See comparison.md | ⏳ CHECK |
| Query perf | ≤10% regression | See comparison.md | ⏳ CHECK |

## Run IDs
- Baseline: $BASELINE_ID
- Parquet:  $PARQUET_ID

## Files
- comparison.md: Full OSB comparison
- baseline-output.txt: Baseline run output
- parquet-output.txt: Parquet run output
- baseline-store-size.txt: Baseline index sizes
- parquet-store-size.txt: Parquet index sizes
EOF

echo "=== Done! See $RESULTS_DIR/summary.md ==="
cat "$RESULTS_DIR/summary.md"
