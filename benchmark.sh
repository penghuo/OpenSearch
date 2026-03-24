#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="benchmark-results"
INGEST_PCT="${1:-1}"
MODE="${2:-all}"      # baseline, parquet, or all
CLEAN="${3:-no}"      # clean, no (default: no — reuse existing data)

mkdir -p "$RESULTS_DIR"
chmod 777 "$RESULTS_DIR"

start_opensearch() {
    if [ "$CLEAN" = "clean" ]; then
        echo "=== Cleaning data ==="
        rm -rf "$OS_HOME/data"
    else
        echo "=== Reusing existing data (pass 'clean' as 3rd arg to wipe) ==="
    fi
    echo "=== Starting OpenSearch ==="
    OPENSEARCH_JAVA_OPTS="-Xms32g -Xmx32g" \
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
    local pid
    pid=$(cat "$OS_HOME/opensearch.pid" 2>/dev/null) || return 0
    kill "$pid" 2>/dev/null || true
    for i in $(seq 1 60); do
        kill -0 "$pid" 2>/dev/null || { echo "OpenSearch stopped"; return 0; }
        sleep 2
    done
    echo "WARN: force-killing OpenSearch"
    kill -9 "$pid" 2>/dev/null || true
    sleep 2
}

capture_storage() {
    local label="$1"
    curl -s "localhost:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" >&2
    curl -s "localhost:9200/_cat/indices?h=store.size&bytes=b" \
        | awk '{sum+=$1} END {print sum}'
}

osb_run() {
    local tag="$1" params_file="$2" csv_file="$3"
    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      -v "$(pwd)/$RESULTS_DIR:/results" \
      opensearchproject/opensearch-benchmark:latest \
      run \
        --workload http_logs \
        --target-hosts localhost:9200 \
        --pipeline benchmark-only \
        --test-procedure append-no-conflicts-index-only \
        --workload-params="/results/$(basename "$params_file")" \
        --user-tag="config:$tag" \
        --results-format csv \
        --results-file "/results/$(basename "$csv_file")" \
        --kill-running-processes \
        2>&1 | tee "$RESULTS_DIR/${tag}-output.txt"
}

# ── Write param files ────────────────────────────────────────────────────────
cat > "$RESULTS_DIR/baseline-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}}
PARAMS

cat > "$RESULTS_DIR/parquet-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "source_enabled": false, "index_settings": {"index.codec.doc_values.format": "parquet"}}
PARAMS

# ── Baseline ─────────────────────────────────────────────────────────────────
if [ "$MODE" = "baseline" ] || [ "$MODE" = "all" ]; then
    rm -f "$RESULTS_DIR/baseline.csv"
    start_opensearch
    echo "=== Baseline run (${INGEST_PCT}% corpus) ==="
    osb_run "baseline" "$RESULTS_DIR/baseline-params.json" "baseline.csv"
    capture_storage "baseline"
    stop_opensearch
fi

# ── Parquet ──────────────────────────────────────────────────────────────────
if [ "$MODE" = "parquet" ] || [ "$MODE" = "all" ]; then
    rm -f "$RESULTS_DIR/parquet.csv"
    start_opensearch
    echo "=== Parquet run (${INGEST_PCT}% corpus) ==="
    osb_run "parquet" "$RESULTS_DIR/parquet-params.json" "parquet.csv"
    capture_storage "parquet"
    stop_opensearch
fi

# ── Extract metrics from CSVs ────────────────────────────────────────────────
extract_metric() {
    local file="$1" metric="$2"
    grep "^${metric}," "$file" | head -1 | awk -F',' '{print $3}'
}

if [ -f "$RESULTS_DIR/baseline.csv" ] && [ -f "$RESULTS_DIR/parquet.csv" ]; then
B_STORE_GB=$(extract_metric "$RESULTS_DIR/baseline.csv" "Store size")
P_STORE_GB=$(extract_metric "$RESULTS_DIR/parquet.csv" "Store size")
B_INDEX_TIME=$(extract_metric "$RESULTS_DIR/baseline.csv" "Cumulative indexing time of primary shards")
P_INDEX_TIME=$(extract_metric "$RESULTS_DIR/parquet.csv" "Cumulative indexing time of primary shards")

pct_diff() {
    awk "BEGIN { if ($1+0 != 0) printf \"%.1f%%\", (($2 - $1) / $1) * 100; else print \"N/A\" }"
}

STORAGE_DIFF=$(pct_diff "${B_STORE_GB:-0}" "${P_STORE_GB:-0}")
TIME_DIFF=$(pct_diff "${B_INDEX_TIME:-0}" "${P_INDEX_TIME:-0}")

# ── Summary ──────────────────────────────────────────────────────────────────
cat <<EOF | tee "$RESULTS_DIR/summary.md"
# Benchmark Summary (${INGEST_PCT}% http_logs, index-only)

| Metric                    | Baseline       | Parquet        | Diff       |
|---------------------------|----------------|----------------|------------|
| Store Size (GB)           | $B_STORE_GB    | $P_STORE_GB    | $STORAGE_DIFF |
| Indexing Time (min)       | $B_INDEX_TIME  | $P_INDEX_TIME  | $TIME_DIFF |

Note: Median Throughput not available (1% corpus completes during OSB warmup phase).

## Pass/Fail
- Storage reduction: $STORAGE_DIFF (target: significant reduction)
- Indexing time: $TIME_DIFF (target: no major regression)
EOF
else
    echo "=== Summary skipped (need both baseline and parquet CSVs) ==="
fi
