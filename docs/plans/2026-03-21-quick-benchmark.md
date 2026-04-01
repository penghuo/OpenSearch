# Quick Benchmark Implementation Plan

> **For Kiro:** Use `#executing-plans` for sequential tasks.

**Goal:** Replace `benchmark.sh` with a streamlined index-only benchmark that verifies storage reduction and ingestion throughput using 1% http_logs corpus in ~15 minutes.

**Architecture:** Rewrite `benchmark.sh` to use `append-no-conflicts-index-only` test procedure with inline `--workload-params`. Run baseline then parquet on fresh OS instances. Extract metrics from CSV result files. Print a summary table with pass/fail.

**Tech Stack:** Bash, opensearch-benchmark (Docker), OpenSearch localDistro, curl

---

## Task 1: Rewrite benchmark.sh

**Files:**
- Modify: `benchmark.sh`

**Step 1: Replace benchmark.sh with the new implementation**

The new script:
- Drops all existing modes (smoke, phase1, phase2) and param JSON file dependencies
- Uses `append-no-conflicts-index-only` (8 steps, no queries)
- Passes workload-params inline via `--workload-params` string (no mounted JSON files)
- Writes CSV results via `--results-format csv --results-file <path>`
- Captures store size via `_cat/indices`
- Extracts metrics from CSVs and prints a summary table

```bash
#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="benchmark-results"
INGEST_PCT="${1:-1}"

mkdir -p "$RESULTS_DIR"

start_opensearch() {
    echo "=== Cleaning data ==="
    rm -rf "$OS_HOME/data"
    echo "=== Starting OpenSearch ==="
    OPENSEARCH_JAVA_OPTS="-Xms16g -Xmx16g" \
        "$OS_HOME/bin/opensearch" \
        -Eopensearch.experimental.feature.parquet_doc_values.enabled=true \
        -Ediscovery.type=single-node \
        -d -p "$OS_HOME/opensearch.pid"
    echo "=== Waiting for cluster ==="
    for i in $(seq 1 120); do
        local status
        status=$(curl -s localhost:9200/_cluster/health 2>/dev/null \
            | grep -oP '"status"\s*:\s*"\K[^"]+' || true)
        if [ "$status" = "green" ] || [ "$status" = "yellow" ]; then
            echo "Cluster is $status"
            return 0
        fi
        sleep 5
    done
    echo "ERROR: cluster did not start" >&2; return 1
}

stop_opensearch() {
    echo "=== Stopping OpenSearch ==="
    kill "$(cat "$OS_HOME/opensearch.pid" 2>/dev/null)" 2>/dev/null || true
    sleep 5
}

capture_storage() {
    local label="$1"
    curl -s "localhost:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" >&2
    curl -s "localhost:9200/_cat/indices?h=store.size&bytes=b" \
        | awk '{sum+=$1} END {print sum}'
}

osb_run() {
    local tag="$1" params="$2" csv_file="$3"
    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      -v "$(pwd)/$RESULTS_DIR:/results" \
      opensearchproject/opensearch-benchmark:latest \
      run \
        --workload http_logs \
        --target-hosts localhost:9200 \
        --pipeline benchmark-only \
        --test-procedure append-no-conflicts-index-only \
        --workload-params="$params" \
        --user-tag="config:$tag" \
        --results-format csv \
        --results-file "/results/$(basename "$csv_file")" \
        --kill-running-processes \
        2>&1 | tee "$RESULTS_DIR/${tag}-output.txt"
}

# ── Baseline ─────────────────────────────────────────────────────────────────
start_opensearch
echo "=== Baseline run (${INGEST_PCT}% corpus) ==="
osb_run "baseline" \
    "number_of_replicas:0,ingest_percentage:${INGEST_PCT}" \
    "baseline.csv"
BASELINE_BYTES=$(capture_storage "baseline")
stop_opensearch

# ── Parquet ──────────────────────────────────────────────────────────────────
start_opensearch
echo "=== Parquet run (${INGEST_PCT}% corpus) ==="
osb_run "parquet" \
    "number_of_replicas:0,ingest_percentage:${INGEST_PCT},index_settings:{\"index.codec.doc_values.format\":\"parquet\"}" \
    "parquet.csv"
PARQUET_BYTES=$(capture_storage "parquet")
stop_opensearch

# ── Extract metrics from CSVs ────────────────────────────────────────────────
extract_metric() {
    local file="$1" metric="$2"
    grep "^$metric" "$file" | head -1 | awk -F',' '{print $2}'
}

B_MEDIAN_TP=$(extract_metric "$RESULTS_DIR/baseline.csv" "Median Throughput")
P_MEDIAN_TP=$(extract_metric "$RESULTS_DIR/parquet.csv" "Median Throughput")
B_INDEX_TIME=$(extract_metric "$RESULTS_DIR/baseline.csv" "Cumulative indexing time of primary shards")
P_INDEX_TIME=$(extract_metric "$RESULTS_DIR/parquet.csv" "Cumulative indexing time of primary shards")

pct_diff() {
    awk "BEGIN { if ($1 != 0) printf \"%.1f%%\", (($2 - $1) / $1) * 100; else print \"N/A\" }"
}

STORAGE_DIFF=$(pct_diff "$BASELINE_BYTES" "$PARQUET_BYTES")
TP_DIFF=$(pct_diff "$B_MEDIAN_TP" "$P_MEDIAN_TP")
TIME_DIFF=$(pct_diff "$B_INDEX_TIME" "$P_INDEX_TIME")

# ── Summary ──────────────────────────────────────────────────────────────────
cat <<EOF | tee "$RESULTS_DIR/summary.md"
# Benchmark Summary (${INGEST_PCT}% http_logs, index-only)

| Metric                    | Baseline       | Parquet        | Diff       |
|---------------------------|----------------|----------------|------------|
| Median Throughput (docs/s)| $B_MEDIAN_TP   | $P_MEDIAN_TP   | $TP_DIFF   |
| Store Size (bytes)        | $BASELINE_BYTES| $PARQUET_BYTES | $STORAGE_DIFF |
| Indexing Time (min)       | $B_INDEX_TIME  | $P_INDEX_TIME  | $TIME_DIFF |

## Pass/Fail
- Storage reduction: $STORAGE_DIFF (target: significant reduction)
- Ingestion throughput: $TP_DIFF (target: no major regression)
EOF
```

**Step 2: Verify script syntax**

Run: `bash -n benchmark.sh`
Expected: No output (no syntax errors).

**Step 3: Commit**

```bash
git add benchmark.sh
git commit -m "Rewrite benchmark.sh for fast index-only storage and throughput validation"
```

---

## Task 2: Run the benchmark

**Step 1: Build localDistro (if stale)**

```bash
./gradlew localDistro
```

**Step 2: Run the benchmark**

```bash
./benchmark.sh 1 2>&1 | tee benchmark-results/full_log.txt
```

The `1` argument means 1% corpus (~2.5M docs). Expected runtime: ~12-15 minutes.

**Step 3: Review results**

```bash
cat benchmark-results/summary.md
```

Verify:
- Parquet store size is significantly smaller than baseline
- Ingestion throughput has no major regression (within ~20%)

**Step 4: Commit results**

```bash
git add benchmark-results/summary.md
git commit -m "Add quick benchmark results: 1% http_logs index-only"
```
