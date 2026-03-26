#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="benchmark-results"
INGEST_PCT="${1:-1}"
MODE="${2:-all}"      # baseline, parquet, or all
CLEAN="${3:-no}"      # clean, no (default: no — reuse existing data)
SCOPE="${4:-full}"    # full (index+search), search-only

if [ "$SCOPE" = "search-only" ] && [ "$CLEAN" = "clean" ]; then
    echo "ERROR: search-only mode cannot be used with clean (would wipe indexed data)" >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"
chmod 777 "$RESULTS_DIR"

start_opensearch() {
    # Kill any stale OS process
    local old_pid
    old_pid=$(cat "$OS_HOME/opensearch.pid" 2>/dev/null) || true
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
        echo "=== Killing stale OpenSearch (pid $old_pid) ==="
        kill -9 "$old_pid" 2>/dev/null || true
        sleep 2
    fi
    # Remove stale lock files
    find "$OS_HOME/data" -name "write.lock" -delete 2>/dev/null || true

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
    # Retry health check — node needs time to bind port after -d
    local health
    for attempt in $(seq 1 60); do
        health=$(curl -s --max-time 5 'localhost:9200/_cluster/health?wait_for_status=yellow&timeout=10s' 2>/dev/null) || true
        if echo "$health" | grep -qP '"status"\s*:\s*"(green|yellow)"'; then
            echo "Cluster is $(echo "$health" | grep -oP '"status"\s*:\s*"\K[^"]+')"
            return 0
        fi
        sleep 5
    done
    echo "ERROR: cluster did not start after 5 minutes" >&2; return 1
}

stop_opensearch() {
    echo "=== Stopping OpenSearch ==="
    local pid
    pid=$(cat "$OS_HOME/opensearch.pid" 2>/dev/null) || return 0
    kill "$pid" 2>/dev/null || true
    for i in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || { echo "OpenSearch stopped"; return 0; }
        sleep 1
    done
    echo "WARN: force-killing OpenSearch"
    kill -9 "$pid" 2>/dev/null || true
    sleep 2
}

osb_run() {
    local tag="$1" params_file="$2" csv_file="$3"
    local include_tasks_flag=""
    if [ "$SCOPE" = "search-only" ]; then
        include_tasks_flag='--include-tasks="match-all,term,range,status-200s-in-range,status-400s-in-range,hourly_agg,hourly_agg_with_filter,hourly_agg_with_filter_and_metrics,multi_term_agg,scroll,desc_sort_size,asc_sort_size,desc_sort_timestamp,asc_sort_timestamp,desc_sort_with_after_timestamp,asc_sort_with_after_timestamp,desc-sort-timestamp-after-force-merge-1-seg,asc-sort-timestamp-after-force-merge-1-seg,desc-sort-with-after-timestamp-after-force-merge-1-seg,asc-sort-with-after-timestamp-after-force-merge-1-seg"'
    fi
    # Kill stale OSB containers and remove stale daemon lock from volume
    docker ps -q --filter ancestor=opensearchproject/opensearch-benchmark:latest | xargs -r docker kill 2>/dev/null || true
    docker run --rm -v osb-data:/data alpine sh -c "rm -f /data/.benchmark/.rally.pid /data/.benchmark/.osb.pid" 2>/dev/null || true
    docker volume rm osb-data 2>/dev/null || true
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
        --on-error=abort \
        $include_tasks_flag \
        > "$RESULTS_DIR/${tag}-output.txt" 2>&1
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
        echo "ERROR: OSB run '$tag' failed with exit code $rc" >&2
        return 1
    fi
}

# ── Write param files ────────────────────────────────────────────────────────
cat > "$RESULTS_DIR/baseline-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "warmup_iterations": 1, "iterations": 3}
PARAMS

cat > "$RESULTS_DIR/parquet-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "warmup_iterations": 1, "iterations": 3, "source_enabled": false, "index_settings": {"index.codec.doc_values.format": "parquet"}}
PARAMS

# ── Run selected mode(s) ────────────────────────────────────────────────────
if [ "$MODE" = "baseline" ] || [ "$MODE" = "all" ]; then
    rm -f "$RESULTS_DIR/search-baseline.csv"
    start_opensearch
    echo "=== Baseline search run (${INGEST_PCT}% corpus) ==="
    osb_run "search-baseline" "$RESULTS_DIR/baseline-params.json" "search-baseline.csv"
    stop_opensearch
fi

if [ "$MODE" = "parquet" ] || [ "$MODE" = "all" ]; then
    rm -f "$RESULTS_DIR/search-parquet.csv"
    start_opensearch
    echo "=== Parquet search run (${INGEST_PCT}% corpus) ==="
    osb_run "search-parquet" "$RESULTS_DIR/parquet-params.json" "search-parquet.csv"
    stop_opensearch
fi

# ── Extract latency metrics and build summary (only when both CSVs exist) ───
B_CSV="$RESULTS_DIR/search-baseline.csv"
P_CSV="$RESULTS_DIR/search-parquet.csv"

if [ -f "$B_CSV" ] && [ -f "$P_CSV" ]; then
    rm -f "$RESULTS_DIR/search-summary.md"
    ops=$(grep "^50th percentile latency," "$B_CSV" | tr -d '\r' | awk -F',' '$2 != "" {print $2}' | sort)

    {
        echo "# Search Benchmark Summary (${INGEST_PCT}% http_logs, append-no-conflicts)"
        echo ""
        echo "| Operation | Baseline p50 (ms) | Parquet p50 (ms) | p50 Diff | Baseline p90 (ms) | Parquet p90 (ms) | p90 Diff |"
        echo "|-----------|-------------------|-------------------|----------|-------------------|-------------------|----------|"

        while IFS= read -r op; do
            [ -z "$op" ] && continue
            b50=$(grep "^50th percentile latency,${op}," "$B_CSV" | head -1 | tr -d '\r' | awk -F',' '{print $3}')
            p50=$(grep "^50th percentile latency,${op}," "$P_CSV" | head -1 | tr -d '\r' | awk -F',' '{print $3}')
            b90=$(grep "^90th percentile latency,${op}," "$B_CSV" | head -1 | tr -d '\r' | awk -F',' '{print $3}')
            p90=$(grep "^90th percentile latency,${op}," "$P_CSV" | head -1 | tr -d '\r' | awk -F',' '{print $3}')

            diff50=$(awk "BEGIN { if (${b50:-0}+0 != 0) printf \"%.1f%%\", ((${p50:-0} - ${b50:-0}) / ${b50:-0}) * 100; else print \"N/A\" }")
            diff90=$(awk "BEGIN { if (${b90:-0}+0 != 0) printf \"%.1f%%\", ((${p90:-0} - ${b90:-0}) / ${b90:-0}) * 100; else print \"N/A\" }")

            printf "| %-9s | %-17s | %-17s | %-8s | %-17s | %-17s | %-8s |\n" \
                "$op" "${b50:-N/A}" "${p50:-N/A}" "$diff50" "${b90:-N/A}" "${p90:-N/A}" "$diff90"
        done <<< "$ops"

        echo ""
        echo "## Pass/Fail"
        echo "- Target: no operation regresses >20% on p90 latency"
    } | tee "$RESULTS_DIR/search-summary.md"
else
    echo "=== Summary skipped (need both baseline and parquet CSVs) ==="
fi
