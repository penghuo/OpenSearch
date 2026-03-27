#!/usr/bin/env bash
set -euo pipefail

# ── Parquet benchmark harness ────────────────────────────────────────────────
# Usage: ./benchmark.sh <phase> [mode] [clean]
#
# Phases:
#   build    — build localDistro
#   test     — run yamlRestTestParquet (correctness gate)
#   bench    — run OSB benchmark (10 iterations, 1 warmup)
#   all      — build → test → bench (stops on failure)
#
# Mode (bench phase only):
#   parquet  — parquet run only (default)
#   baseline — baseline run only
#   both     — baseline then parquet
#
# Clean (bench phase only):
#   clean    — wipe data before indexing
#   no       — reuse existing data (default)
#
# Examples:
#   ./benchmark.sh all both clean    # full pipeline, both configs, fresh data
#   ./benchmark.sh bench parquet no  # benchmark only, reuse data
#   ./benchmark.sh test              # correctness check only
#   ./benchmark.sh build             # build only

REPO_ROOT="/local/home/penghuo/oss/OpenSearch"
OS_HOME="$REPO_ROOT/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="$REPO_ROOT/benchmark-results"
JAVA_HOME="/usr/lib/jvm/java-21-amazon-corretto"
INGEST_PCT=5
ITERATIONS=10
WARMUP=1
P90_THRESHOLD=10  # percent

PHASE="${1:-all}"
MODE="${2:-parquet}"
CLEAN="${3:-no}"

mkdir -p "$RESULTS_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*"; }
die() { log "FATAL: $*" >&2; exit 1; }

# ── Phase: build ─────────────────────────────────────────────────────────────
do_build() {
    log "Building localDistro..."
    cd "$REPO_ROOT"
    JAVA_HOME="$JAVA_HOME" ./gradlew localDistro -x test -x internalClusterTest 2>&1 | tail -5
    log "Build complete."
}

# ── Phase: test ──────────────────────────────────────────────────────────────
do_test() {
    log "Running yamlRestTestParquet..."
    cd "$REPO_ROOT"
    local out="/tmp/yamlrest-output.txt"
    JAVA_HOME="$JAVA_HOME" ./gradlew :rest-api-spec:yamlRestTestParquet > "$out" 2>&1
    local rc=$?
    if [ $rc -ne 0 ]; then
        log "yamlRestTestParquet FAILED (exit $rc)"
        tail -20 "$out"
        die "Correctness gate failed. Fix tests before benchmarking."
    fi
    local tests fails
    tests=$(grep -oP 'tests:\s*\K\d+' "$out" | tail -1) || tests="?"
    fails=$(grep -oP 'failures:\s*\K\d+' "$out" | tail -1) || fails="?"
    log "yamlRestTestParquet PASSED ($tests tests, $fails failures)"
}

# ── OpenSearch lifecycle ─────────────────────────────────────────────────────
start_opensearch() {
    local old_pid
    old_pid=$(cat "$OS_HOME/opensearch.pid" 2>/dev/null) || true
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
        kill -9 "$old_pid" 2>/dev/null || true; sleep 2
    fi
    find "$OS_HOME/data" -name "write.lock" -delete 2>/dev/null || true

    if [ "$CLEAN" = "clean" ]; then
        log "Cleaning data..."
        rm -rf "$OS_HOME/data"
    fi

    log "Starting OpenSearch (32GB heap)..."
    OPENSEARCH_JAVA_OPTS="-Xms32g -Xmx32g" \
        "$OS_HOME/bin/opensearch" \
        -Eopensearch.experimental.feature.parquet_doc_values.enabled=true \
        -Ediscovery.type=single-node \
        -d -p "$OS_HOME/opensearch.pid"

    for attempt in $(seq 1 60); do
        local health
        health=$(curl -s --max-time 5 'localhost:9200/_cluster/health?wait_for_status=yellow&timeout=10s' 2>/dev/null) || true
        if echo "$health" | grep -qP '"status"\s*:\s*"(green|yellow)"'; then
            log "Cluster ready ($(echo "$health" | grep -oP '"status"\s*:\s*"\K[^"]+'))"
            return 0
        fi
        sleep 5
    done
    die "Cluster did not start after 5 minutes"
}

stop_opensearch() {
    log "Stopping OpenSearch..."
    # Capture logs before stopping
    local logfile="$OS_HOME/logs/opensearch.log"
    if [ -f "$logfile" ]; then
        cp "$logfile" "$RESULTS_DIR/opensearch-${1:-unknown}.log"
        # Extract FilterRewrite diagnostics
        grep -i 'FilterRewrite' "$logfile" > "$RESULTS_DIR/filterrewrite-${1:-unknown}.log" 2>/dev/null || true
    fi

    local pid
    pid=$(cat "$OS_HOME/opensearch.pid" 2>/dev/null) || return 0
    kill "$pid" 2>/dev/null || true
    for i in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || { log "Stopped."; return 0; }
        sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true; sleep 2
}

# ── OSB runner ───────────────────────────────────────────────────────────────
osb_run() {
    local tag="$1" params_file="$2" csv_file="$3"
    docker ps -q --filter ancestor=opensearchproject/opensearch-benchmark:latest | xargs -r docker kill 2>/dev/null || true
    docker run --rm -v osb-data:/data alpine sh -c "rm -f /data/.benchmark/.rally.pid /data/.benchmark/.osb.pid" 2>/dev/null || true
    docker volume rm osb-data 2>/dev/null || true

    log "OSB run: $tag"
    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      -v "$RESULTS_DIR:/results" \
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
        > "$RESULTS_DIR/${tag}-output.txt" 2>&1
    local rc=$?
    if [ $rc -ne 0 ]; then
        log "OSB run '$tag' FAILED (exit $rc)"
        tail -20 "$RESULTS_DIR/${tag}-output.txt"
        return 1
    fi
    log "OSB run '$tag' complete."
}

# ── Phase: bench ─────────────────────────────────────────────────────────────
do_bench() {
    cd "$REPO_ROOT"

    cat > "$RESULTS_DIR/baseline-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "warmup_iterations": ${WARMUP}, "iterations": ${ITERATIONS}}
PARAMS

    cat > "$RESULTS_DIR/parquet-params.json" <<PARAMS
{"number_of_replicas": 0, "ingest_percentage": ${INGEST_PCT}, "warmup_iterations": ${WARMUP}, "iterations": ${ITERATIONS}, "source_enabled": false, "index_settings": {"index.codec.doc_values.format": "parquet"}}
PARAMS

    if [ "$MODE" = "baseline" ] || [ "$MODE" = "both" ]; then
        rm -f "$RESULTS_DIR/search-baseline.csv"
        start_opensearch
        osb_run "search-baseline" "$RESULTS_DIR/baseline-params.json" "search-baseline.csv"
        stop_opensearch "baseline"
        # After baseline, don't clean for parquet run
        CLEAN="no"
    fi

    if [ "$MODE" = "parquet" ] || [ "$MODE" = "both" ]; then
        rm -f "$RESULTS_DIR/search-parquet.csv"
        start_opensearch
        osb_run "search-parquet" "$RESULTS_DIR/parquet-params.json" "search-parquet.csv"
        # Measure storage
        measure_storage
        stop_opensearch "parquet"
    fi

    generate_summary
}

# ── Storage measurement ──────────────────────────────────────────────────────
measure_storage() {
    log "Measuring index storage..."
    local stats
    stats=$(curl -s 'localhost:9200/_cat/indices?h=index,store.size&bytes=b' 2>/dev/null) || true
    echo "$stats" > "$RESULTS_DIR/storage.txt"
    log "Storage:"
    echo "$stats"
}

# ── Summary generation ───────────────────────────────────────────────────────
generate_summary() {
    local B_CSV="$RESULTS_DIR/search-baseline.csv"
    local P_CSV="$RESULTS_DIR/search-parquet.csv"

    if [ ! -f "$B_CSV" ] || [ ! -f "$P_CSV" ]; then
        log "Summary skipped (need both baseline and parquet CSVs)"
        return 0
    fi

    local stamp
    stamp=$(date '+%Y%m%d_%H%M%S')
    local summary="$RESULTS_DIR/${stamp}_summary.md"

    # Get operation list from baseline p50
    local ops
    ops=$(grep "^50th percentile service time," "$B_CSV" | tr -d '\r' | awk -F',' '$2 != "" && $2 != "index-append" {print $2}' | sort)

    # Build summary into temp file, then tee
    local tmpsum
    tmpsum=$(mktemp)

    echo "# Benchmark Summary — $(date '+%Y-%m-%d %H:%M')" >> "$tmpsum"
    echo "" >> "$tmpsum"
    echo "Dataset: ${INGEST_PCT}% http_logs | Iterations: ${ITERATIONS} | Warmup: ${WARMUP}" >> "$tmpsum"
    echo "" >> "$tmpsum"
    echo "| # | Operation | B svc50 | P svc50 | svc Δ | B p90 | P p90 | p90 Δ | Status |" >> "$tmpsum"
    echo "|---|-----------|--------:|--------:|------:|------:|------:|------:|--------|" >> "$tmpsum"

    local pass=0 fail=0 total=0 n=0
    while IFS= read -r op; do
        [ -z "$op" ] && continue
        n=$((n + 1))
        total=$((total + 1))

        local b_svc50 p_svc50 b_p90 p_p90
        b_svc50=$(grep "^50th percentile service time,${op}," "$B_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true
        p_svc50=$(grep "^50th percentile service time,${op}," "$P_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true

        # Try p90 first, fall back to p100
        b_p90=$(grep "^90th percentile latency,${op}," "$B_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true
        p_p90=$(grep "^90th percentile latency,${op}," "$P_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true
        local metric="p90"
        if [ -z "$b_p90" ] || [ "$b_p90" = "0.00" ]; then
            b_p90=$(grep "^100th percentile latency,${op}," "$B_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true
            p_p90=$(grep "^100th percentile latency,${op}," "$P_CSV" | head -1 | tr -d '\r' | awk -F',' '{printf "%.2f", $3}') || true
            metric="p100"
        fi

        local svc_diff p90_diff
        svc_diff=$(awk "BEGIN { if (${b_svc50:-0}+0 != 0) printf \"%.1f%%\", ((${p_svc50:-0} - ${b_svc50:-0}) / ${b_svc50:-0}) * 100; else print \"N/A\" }")
        p90_diff=$(awk "BEGIN { if (${b_p90:-0}+0 != 0) printf \"%.1f%%\", ((${p_p90:-0} - ${b_p90:-0}) / ${b_p90:-0}) * 100; else print \"N/A\" }")

        local pct status
        pct=$(awk "BEGIN { if (${b_p90:-0}+0 != 0) printf \"%.1f\", ((${p_p90:-0} - ${b_p90:-0}) / ${b_p90:-0}) * 100; else print \"999\" }")
        if awk "BEGIN { exit ($pct <= $P90_THRESHOLD) ? 0 : 1 }"; then
            status="✅"
            pass=$((pass + 1))
        else
            status="❌ (${metric})"
            fail=$((fail + 1))
        fi

        printf "| %d | %s | %s | %s | %s | %s | %s | %s | %s |\n" \
            "$n" "$op" "$b_svc50" "$p_svc50" "$svc_diff" "$b_p90" "$p_p90" "$p90_diff" "$status" >> "$tmpsum"
    done <<< "$ops"

    echo "" >> "$tmpsum"
    echo "**Score: ${pass} PASS / ${fail} FAIL out of ${total} operations (target: ≤${P90_THRESHOLD}% p90 regression)**" >> "$tmpsum"

    # Storage
    if [ -f "$RESULTS_DIR/storage.txt" ]; then
        echo "" >> "$tmpsum"
        echo "## Storage" >> "$tmpsum"
        cat "$RESULTS_DIR/storage.txt" >> "$tmpsum"
    fi

    # FilterRewrite diagnostics
    if [ -f "$RESULTS_DIR/filterrewrite-parquet.log" ] && [ -s "$RESULTS_DIR/filterrewrite-parquet.log" ]; then
        echo "" >> "$tmpsum"
        echo "## FilterRewrite Diagnostics" >> "$tmpsum"
        echo '```' >> "$tmpsum"
        tail -30 "$RESULTS_DIR/filterrewrite-parquet.log" >> "$tmpsum"
        echo '```' >> "$tmpsum"
    fi

    cat "$tmpsum" | tee "$summary"
    cp "$summary" "$RESULTS_DIR/search-summary.md"
    rm -f "$tmpsum"

    log "Summary: $summary"
    log "Result: ${pass}/${total} PASS, ${fail}/${total} FAIL"
}

# ── Main ─────────────────────────────────────────────────────────────────────
case "$PHASE" in
    build)
        do_build
        ;;
    test)
        do_test
        ;;
    bench)
        do_bench
        ;;
    all)
        do_build
        do_test
        do_bench
        ;;
    summary)
        generate_summary
        ;;
    *)
        echo "Usage: $0 <build|test|bench|all|summary> [parquet|baseline|both] [clean|no]"
        exit 1
        ;;
esac

log "Done."
