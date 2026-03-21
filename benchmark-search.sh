#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="benchmark-results"
INGEST_PCT="${1:-1}"

mkdir -p "$RESULTS_DIR"
chmod 777 "$RESULTS_DIR"

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
        --test-procedure append-no-conflicts \
        --workload-params="/results/$(basename "$params_file")" \
        --user-tag="config:$tag" \
        --results-format csv \
        --results-file "/results/$(basename "$csv_file")" \
        --kill-running-processes \
        2>&1 | tee "$RESULTS_DIR/${tag}-output.txt"
}

# ── Clean old search results ─────────────────────────────────────────────────
rm -f "$RESULTS_DIR/search-baseline.csv" "$RESULTS_DIR/search-parquet.csv" "$RESULTS_DIR/search-summary.md"

# ── Write param files ────────────────────────────────────────────────────────
cat > "$RESULTS_DIR/baseline-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}}
PARAMS

cat > "$RESULTS_DIR/parquet-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "index_settings": {"index.codec.doc_values.format": "parquet"}}
PARAMS

# ── Baseline ─────────────────────────────────────────────────────────────────
start_opensearch
echo "=== Baseline search run (${INGEST_PCT}% corpus) ==="
osb_run "search-baseline" "$RESULTS_DIR/baseline-params.json" "search-baseline.csv"
stop_opensearch

# ── Parquet ──────────────────────────────────────────────────────────────────
start_opensearch
echo "=== Parquet search run (${INGEST_PCT}% corpus) ==="
osb_run "search-parquet" "$RESULTS_DIR/parquet-params.json" "search-parquet.csv"
stop_opensearch

# ── Extract latency metrics and build summary ────────────────────────────────
B_CSV="$RESULTS_DIR/search-baseline.csv"
P_CSV="$RESULTS_DIR/search-parquet.csv"

# Get unique operation names that have latency metrics
ops=$(grep "^50th percentile latency," "$B_CSV" | awk -F',' '$2 != "" {print $2}' | sort)

{
    echo "# Search Benchmark Summary (${INGEST_PCT}% http_logs, append-no-conflicts)"
    echo ""
    echo "| Operation | Baseline p50 (ms) | Parquet p50 (ms) | p50 Diff | Baseline p90 (ms) | Parquet p90 (ms) | p90 Diff |"
    echo "|-----------|-------------------|-------------------|----------|-------------------|-------------------|----------|"

    while IFS= read -r op; do
        [ -z "$op" ] && continue
        b50=$(grep "^50th percentile latency,${op}," "$B_CSV" | head -1 | awk -F',' '{print $3}')
        p50=$(grep "^50th percentile latency,${op}," "$P_CSV" | head -1 | awk -F',' '{print $3}')
        b90=$(grep "^90th percentile latency,${op}," "$B_CSV" | head -1 | awk -F',' '{print $3}')
        p90=$(grep "^90th percentile latency,${op}," "$P_CSV" | head -1 | awk -F',' '{print $3}')

        diff50=$(awk "BEGIN { if (${b50:-0}+0 != 0) printf \"%.1f%%\", ((${p50:-0} - ${b50:-0}) / ${b50:-0}) * 100; else print \"N/A\" }")
        diff90=$(awk "BEGIN { if (${b90:-0}+0 != 0) printf \"%.1f%%\", ((${p90:-0} - ${b90:-0}) / ${b90:-0}) * 100; else print \"N/A\" }")

        printf "| %-9s | %-17s | %-17s | %-8s | %-17s | %-17s | %-8s |\n" \
            "$op" "${b50:-N/A}" "${p50:-N/A}" "$diff50" "${b90:-N/A}" "${p90:-N/A}" "$diff90"
    done <<< "$ops"

    echo ""
    echo "## Pass/Fail"
    echo "- Target: no operation regresses >20% on p90 latency"
} | tee "$RESULTS_DIR/search-summary.md"
