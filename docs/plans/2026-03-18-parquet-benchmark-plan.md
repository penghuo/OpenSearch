# Parquet DocValues Benchmark Validation Plan

> **For Kiro:** Use `#executing-plans` for sequential tasks.

**Goal:** Validate parquet doc_values format against baseline using opensearch-benchmark http_logs workload, confirming 4 completion criteria: correctness, 2x ingestion throughput, query latency within 10%, 70% storage reduction.

**Architecture:** Single shell script `benchmark.sh` starts OpenSearch with parquet feature flag, runs baseline then parquet benchmarks against the same instance using `--workload-params` to control index settings, captures storage via REST API, and uses `opensearch-benchmark compare` for the final report.

**Tech Stack:** Bash, opensearch-benchmark (Docker), OpenSearch localDistro, curl

---

## Task 1: Create the benchmark script

**Files:**
- Create: `benchmark.sh`

**Step 1: Create `benchmark.sh`**

```bash
#!/bin/bash
set -euo pipefail

# === Configuration ===
OS_HOME="/local/home/penghuo/oss/OpenSearch/distribution/archives/linux-tar/build/install/opensearch-3.6.0-SNAPSHOT"
OSB="/home/penghuo/osb.sh"
RESULTS_DIR="benchmark-results"
HEAP="16g"
TARGET="localhost:9200"

mkdir -p "$RESULTS_DIR"

# === Helper functions ===

wait_for_green() {
    echo "Waiting for cluster green..."
    for i in $(seq 1 120); do
        if curl -s "$TARGET/_cluster/health" | grep -q '"status":"green"'; then
            echo "Cluster is green."
            return 0
        fi
        sleep 5
    done
    echo "ERROR: Cluster did not turn green in 10 minutes"
    return 1
}

start_opensearch() {
    echo "Starting OpenSearch..."
    OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP}" \
        "$OS_HOME/bin/opensearch" \
        -Eopensearch.experimental.feature.parquet_doc_values.enabled=true \
        -Ediscovery.type=single-node \
        -d -p "$RESULTS_DIR/opensearch.pid"
    wait_for_green
}

stop_opensearch() {
    echo "Stopping OpenSearch..."
    if [ -f "$RESULTS_DIR/opensearch.pid" ]; then
        kill "$(cat "$RESULTS_DIR/opensearch.pid")" 2>/dev/null || true
        rm -f "$RESULTS_DIR/opensearch.pid"
        sleep 5
    fi
}

capture_storage() {
    local label="$1"
    echo "Capturing storage for $label..."
    curl -s "$TARGET/_cat/indices?v&h=index,docs.count,store.size&s=index" \
        > "$RESULTS_DIR/${label}_storage.txt"
    curl -s "$TARGET/_cat/indices?h=store.size&bytes=b" \
        | awk '{sum+=$1} END {print sum}' \
        > "$RESULTS_DIR/${label}_storage_bytes.txt"
    echo "Storage ($label):"
    cat "$RESULTS_DIR/${label}_storage.txt"
    echo "Total bytes: $(cat "$RESULTS_DIR/${label}_storage_bytes.txt")"
}

run_benchmark() {
    local label="$1"
    local params="$2"
    echo "=== Running $label benchmark ==="
    $OSB execute-test \
        --workload http_logs \
        --target-hosts "$TARGET" \
        --pipeline benchmark-only \
        --test-procedure append-no-conflicts \
        --workload-params="$params" \
        --user-tag="config:${label}" \
        --kill-running-processes \
        2>&1 | tee "$RESULTS_DIR/${label}_output.txt"
}

# === Main ===

# Cleanup from previous runs
stop_opensearch
rm -rf "$OS_HOME/data"

# Step 1: Start OpenSearch
start_opensearch

# Step 2: Run baseline benchmark (default lucene doc_values + _source enabled)
run_benchmark "baseline" "number_of_replicas:0"

# Step 3: Capture baseline storage (before next run deletes indices)
capture_storage "baseline"

# Step 4: Run parquet benchmark (parquet doc_values, _source skipped by preParse)
run_benchmark "parquet" 'number_of_replicas:0,index_settings:{"index.codec.doc_values.format":"parquet"}'

# Step 5: Capture parquet storage
capture_storage "parquet"

# Step 6: Stop OpenSearch
stop_opensearch

# Step 7: Extract test run IDs and run comparison
BASELINE_ID=$(grep -oP 'Test run ID: \K[a-f0-9-]+' "$RESULTS_DIR/baseline_output.txt" | tail -1)
PARQUET_ID=$(grep -oP 'Test run ID: \K[a-f0-9-]+' "$RESULTS_DIR/parquet_output.txt" | tail -1)

echo "=== Comparison ==="
echo "Baseline ID: $BASELINE_ID"
echo "Parquet ID:  $PARQUET_ID"

if [ -n "$BASELINE_ID" ] && [ -n "$PARQUET_ID" ]; then
    $OSB compare \
        --baseline "$BASELINE_ID" \
        --contender "$PARQUET_ID" \
        --results-file "$RESULTS_DIR/comparison.md" \
        --results-format markdown \
        2>&1 | tee -a "$RESULTS_DIR/comparison.md"
fi

# Step 8: Generate summary with pass/fail
BASELINE_BYTES=$(cat "$RESULTS_DIR/baseline_storage_bytes.txt")
PARQUET_BYTES=$(cat "$RESULTS_DIR/parquet_storage_bytes.txt")
STORAGE_RATIO=$(echo "scale=2; $PARQUET_BYTES * 100 / $BASELINE_BYTES" | bc)

cat > "$RESULTS_DIR/summary.md" << EOF
# Parquet DocValues Benchmark Summary

## Storage
- Baseline: $(numfmt --to=iec-i "$BASELINE_BYTES")
- Parquet:  $(numfmt --to=iec-i "$PARQUET_BYTES")
- Ratio:    ${STORAGE_RATIO}% of baseline
- Target:   ≤ 30% of baseline
- Result:   $([ "$(echo "$STORAGE_RATIO <= 30" | bc)" -eq 1 ] && echo "✅ PASS" || echo "❌ FAIL")

## Detailed Results
See comparison.md for throughput and latency comparison.
See baseline_output.txt and parquet_output.txt for full OSB reports.

## Completion Criteria
1. Correctness: Check parquet_output.txt for errors
2. Ingestion throughput: Check index-append throughput in comparison.md (target: 2x)
3. Query performance: Check query latencies in comparison.md (target: within 10%)
4. Storage size: ${STORAGE_RATIO}% of baseline (target: ≤ 30%)
EOF

echo ""
echo "=== Summary ==="
cat "$RESULTS_DIR/summary.md"
echo ""
echo "Full results in $RESULTS_DIR/"
```

**Step 2: Make it executable**

```bash
chmod +x benchmark.sh
```

**Step 3: Verify the script parses correctly (dry run)**

```bash
bash -n benchmark.sh
```

Expected: No output (no syntax errors).

**Step 4: Commit**

```bash
git add benchmark.sh
git commit -m "Add benchmark script for parquet doc_values validation"
```

---

## Task 2: Run the benchmark

**Step 1: Build localDistro (if stale)**

```bash
./gradlew localDistro
```

**Step 2: Run the benchmark script**

```bash
./benchmark.sh 2>&1 | tee benchmark-results/full_log.txt
```

This will take several hours (247M docs × 2 runs). Monitor progress in the output.

**Step 3: Review results**

```bash
cat benchmark-results/summary.md
cat benchmark-results/comparison.md
```

**Step 4: Commit results**

```bash
git add benchmark-results/
git commit -m "Add parquet benchmark results: http_logs workload"
```

---

## Task 3: Update fix_plan.md

**Step 1: Mark benchmark task complete in `.ralph/fix_plan.md`**

Change:
```
- [ ] Benchmark with opensearch-benchmark http_logs: ...
```
To:
```
- [x] Benchmark with opensearch-benchmark http_logs: ...
```

Include the actual numbers from the benchmark results.

**Step 2: Commit**

```bash
git add .ralph/fix_plan.md
git commit -m "Mark benchmark validation complete in fix_plan"
git push
```
