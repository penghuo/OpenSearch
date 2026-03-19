#!/usr/bin/env bash
set -euo pipefail

OS_HOME="/local/home/penghuo/oss/OpenSearch/build/distribution/local/opensearch-3.6.0-SNAPSHOT"
RESULTS_DIR="benchmark-results"
PARAMS_DIR="$RESULTS_DIR"

osb_run() {
    local params_file="$1" tag="$2" output_file="$3"
    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      -v "$(cd "$PARAMS_DIR" && pwd)/$params_file:/params.json:ro" \
      opensearchproject/opensearch-benchmark:latest \
      run \
        --workload http_logs \
        --target-hosts localhost:9200 \
        --pipeline benchmark-only \
        --test-procedure append-no-conflicts \
        --workload-params="/params.json" \
        --user-tag="config:$tag" \
        --kill-running-processes \
        2>&1 | tee "$output_file"
}

capture_storage() {
    local label="$1" outfile="$2"
    echo "=== Storage: $label ==="
    curl -s "localhost:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" | tee "$outfile"
    local bytes
    bytes=$(curl -s "localhost:9200/_cat/indices?h=store.size&bytes=b" | awk '{sum+=$1} END {print sum}')
    echo "" >> "$outfile"
    echo "Total bytes: $bytes" >> "$outfile"
    echo "$bytes"
}

extract_run_id() {
    grep -oP 'Test run ID: \K[a-f0-9-]+' "$1" | tail -1
}

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
        status=$(curl -s localhost:9200/_cluster/health 2>/dev/null | grep -oP '"status"\s*:\s*"\K[^"]+' || true)
        if [ "$status" = "green" ] || [ "$status" = "yellow" ]; then
            echo "Cluster is $status"
            return 0
        fi
        sleep 5
    done
    echo "ERROR: cluster did not start" >&2
    return 1
}

stop_opensearch() {
    echo "=== Stopping OpenSearch ==="
    kill "$(cat "$OS_HOME/opensearch.pid" 2>/dev/null)" 2>/dev/null || true
    sleep 5
}

# ── Parse args ───────────────────────────────────────────────────────────────
PHASE="${1:-all}"  # phase1, phase2, or all

run_phase1() {
    echo "============================================"
    echo "  PHASE 1: 1% corpus (~2.5M docs)"
    echo "============================================"
    local dir="$RESULTS_DIR/phase1"
    mkdir -p "$dir"

    start_opensearch

    # Baseline 1%
    echo "=== Phase 1: Baseline run ==="
    osb_run "baseline-params-1pct.json" "baseline-1pct" "$dir/baseline-output.txt"
    local baseline_bytes
    baseline_bytes=$(capture_storage "baseline-1pct" "$dir/baseline-store.txt")

    # Parquet 1%
    echo "=== Phase 1: Parquet run ==="
    osb_run "parquet-params-1pct.json" "parquet-1pct" "$dir/parquet-output.txt"
    local parquet_bytes
    parquet_bytes=$(capture_storage "parquet-1pct" "$dir/parquet-store.txt")

    stop_opensearch

    # Compare
    local baseline_id parquet_id
    baseline_id=$(extract_run_id "$dir/baseline-output.txt")
    parquet_id=$(extract_run_id "$dir/parquet-output.txt")
    echo "Baseline ID: $baseline_id"
    echo "Parquet ID:  $parquet_id"

    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      opensearchproject/opensearch-benchmark:latest \
      compare --baseline "$baseline_id" --contender "$parquet_id" \
      --results-format markdown \
      2>&1 | tee "$dir/comparison.md"

    # Evaluate phase 1
    local parquet_errors baseline_ok parquet_ok
    parquet_errors=$(grep -ci "error" "$dir/parquet-output.txt" || true)
    baseline_ok=$(grep -c "SUCCESS" "$dir/baseline-output.txt" || true)
    parquet_ok=$(grep -c "SUCCESS" "$dir/parquet-output.txt" || true)

    local storage_pct="N/A"
    if [ "$baseline_bytes" -gt 0 ] 2>/dev/null; then
        storage_pct=$(awk "BEGIN {printf \"%.1f\", ($parquet_bytes / $baseline_bytes) * 100}")
    fi

    cat > "$dir/summary.md" <<EOF
# Phase 1 Summary (1% corpus)

## Correctness
- Baseline: $([ "$baseline_ok" -ge 1 ] && echo "✅ SUCCESS" || echo "❌ FAIL")
- Parquet:  $([ "$parquet_ok" -ge 1 ] && echo "✅ SUCCESS" || echo "❌ FAIL")

## Storage
- Baseline: $baseline_bytes bytes
- Parquet:  $parquet_bytes bytes
- Ratio:    ${storage_pct}% of baseline

## Pass/Fail (Phase 1: no downgrade)
| Criterion | Target | Result | Status |
|-----------|--------|--------|--------|
| Correctness | 0 errors | parquet_ok=$parquet_ok | $([ "$parquet_ok" -ge 1 ] && echo "✅" || echo "❌") |
| Storage | ≤ baseline | ${storage_pct}% | $(awk "BEGIN {exit ($parquet_bytes <= $baseline_bytes) ? 0 : 1}" 2>/dev/null && echo "✅" || echo "❌") |
| Ingestion | ≥ baseline | See comparison.md | ⏳ CHECK |
| Query perf | ≤ baseline | See comparison.md | ⏳ CHECK |

## Run IDs
- Baseline: $baseline_id
- Parquet:  $parquet_id
EOF

    echo "=== Phase 1 complete. See $dir/summary.md ==="
    cat "$dir/summary.md"
}

run_phase2() {
    echo "============================================"
    echo "  PHASE 2: Full corpus (247M docs)"
    echo "============================================"
    local dir="$RESULTS_DIR/phase2"
    mkdir -p "$dir"

    start_opensearch

    # Baseline full
    echo "=== Phase 2: Baseline run ==="
    osb_run "baseline-params.json" "baseline-full" "$dir/baseline-output.txt"
    local baseline_bytes
    baseline_bytes=$(capture_storage "baseline-full" "$dir/baseline-store.txt")

    # Parquet full
    echo "=== Phase 2: Parquet run ==="
    osb_run "parquet-params.json" "parquet-full" "$dir/parquet-output.txt"
    local parquet_bytes
    parquet_bytes=$(capture_storage "parquet-full" "$dir/parquet-store.txt")

    stop_opensearch

    # Compare
    local baseline_id parquet_id
    baseline_id=$(extract_run_id "$dir/baseline-output.txt")
    parquet_id=$(extract_run_id "$dir/parquet-output.txt")

    docker run --rm --network host \
      -v osb-data:/opensearch-benchmark/.benchmark \
      opensearchproject/opensearch-benchmark:latest \
      compare --baseline "$baseline_id" --contender "$parquet_id" \
      --results-format markdown \
      2>&1 | tee "$dir/comparison.md"

    # Evaluate phase 2
    local parquet_ok
    parquet_ok=$(grep -c "SUCCESS" "$dir/parquet-output.txt" || true)

    local storage_pct="N/A"
    if [ "$baseline_bytes" -gt 0 ] 2>/dev/null; then
        storage_pct=$(awk "BEGIN {printf \"%.1f\", ($parquet_bytes / $baseline_bytes) * 100}")
    fi

    cat > "$dir/summary.md" <<EOF
# Phase 2 Summary (Full corpus)

## Storage
- Baseline: $baseline_bytes bytes
- Parquet:  $parquet_bytes bytes
- Ratio:    ${storage_pct}% of baseline

## Pass/Fail (Phase 2 targets)
| Criterion | Target | Result | Status |
|-----------|--------|--------|--------|
| Correctness | 0 errors | parquet_ok=$parquet_ok | $([ "$parquet_ok" -ge 1 ] && echo "✅" || echo "❌") |
| Storage | ≤30% of baseline | ${storage_pct}% | $(awk "BEGIN {exit ($parquet_bytes <= $baseline_bytes * 0.30) ? 0 : 1}" 2>/dev/null && echo "✅" || echo "❌") |
| Ingestion | ≥2x baseline | See comparison.md | ⏳ CHECK |
| Query perf | ≤10% regression | See comparison.md | ⏳ CHECK |

## Run IDs
- Baseline: $baseline_id
- Parquet:  $parquet_id
EOF

    echo "=== Phase 2 complete. See $dir/summary.md ==="
    cat "$dir/summary.md"
}

case "$PHASE" in
    phase1) run_phase1 ;;
    phase2) run_phase2 ;;
    all)    run_phase1 && run_phase2 ;;
    *)      echo "Usage: $0 {phase1|phase2|all}"; exit 1 ;;
esac
