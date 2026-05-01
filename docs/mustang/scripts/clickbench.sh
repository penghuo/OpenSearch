#!/usr/bin/env bash
# ClickBench harness for the Mustang SQL + analytics-engine stack.
#
# Runs the 43 ClickBench PPL queries against a locally running OpenSearch
# cluster via the REST _plugins/_ppl endpoint.
#
# Inspired by integ-test/.../PPLClickBenchIT but via shell/REST only — no
# gradle, no JUnit, no Java process.
#
# Usage:
#   ./clickbench.sh setup       # create the 'hits' parquet-backed index + load sample data
#   ./clickbench.sh run         # run all 43 queries; print pass/fail/timings
#   ./clickbench.sh one <N>     # run a single query (N in 1..43)
#   ./clickbench.sh teardown    # delete the 'hits' index
#   ./clickbench.sh all         # setup + run (leaves index in place)
#
# Env vars:
#   ENDPOINT   cluster REST endpoint (default: http://localhost:9200)
#   INDEX      index name (default: hits)
#   PARQUET    1=create index with pluggable.dataformat=parquet settings (default: 1)
#   FORCE_ROUTING   1=set plugins.calcite.analytics.force_routing=true before running queries (default: 0)
#   OS_SQL_DIR path to os-sql repo (default: /home/penghuo/oss/os-sql)
#
# Exit codes:
#   0  all queries passed
#   1  any query failed
#   2  environment error (cluster down, missing files, etc.)

set -uo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:9200}"
INDEX="${INDEX:-hits}"
PARQUET="${PARQUET:-1}"
FORCE_ROUTING="${FORCE_ROUTING:-0}"
OS_SQL_DIR="${OS_SQL_DIR:-/home/penghuo/oss/os-sql}"

MAPPING_FILE="${OS_SQL_DIR}/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"
DATA_FILE="${OS_SQL_DIR}/integ-test/src/test/resources/clickbench/data/clickbench.json"
QUERIES_DIR="${OS_SQL_DIR}/integ-test/src/test/resources/clickbench/queries"

# ─── helpers ────────────────────────────────────────────────────────────────
log()   { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fatal() { log "ERROR: $*"; exit 2; }

check_cluster() {
    if ! curl -sSf --connect-timeout 3 "$ENDPOINT/" > /dev/null 2>&1; then
        fatal "cluster not reachable at $ENDPOINT"
    fi
}

# Extract just the query body from a .ppl file (strip the /* SQL */ comment header)
extract_ppl() {
    local file="$1"
    # Strip /* ... */ block (greedy multiline) then trim leading blank lines
    awk '
        BEGIN { in_comment=0 }
        /^\/\*/ { in_comment=1; next }
        in_comment && /\*\// { in_comment=0; next }
        in_comment { next }
        { print }
    ' "$file" | sed '/./,$!d'
}

# ─── setup ──────────────────────────────────────────────────────────────────
cmd_setup() {
    check_cluster
    [ -f "$MAPPING_FILE" ] || fatal "missing mapping file: $MAPPING_FILE"
    [ -f "$DATA_FILE" ]    || fatal "missing data file: $DATA_FILE"

    # Delete index if exists
    log "deleting existing index '$INDEX' (if any)"
    curl -sS -X DELETE "$ENDPOINT/$INDEX" > /dev/null 2>&1 || true

    # Build the create-index body. If PARQUET=1, augment settings with pluggable.dataformat per Quip doc.
    local body
    if [ "$PARQUET" = "1" ]; then
        log "creating index '$INDEX' with pluggable.dataformat=parquet settings"
        body=$(python3 - <<PY
import json
with open("$MAPPING_FILE") as f:
    m = json.load(f)
s = m.setdefault("settings", {})
# Drop sort.* settings: they conflict with single-shard parquet backend when data is minimal,
# and they're not required for the query tests.
if "index" in s:
    s["index"].pop("sort.field", None)
    s["index"].pop("sort.order", None)
s["number_of_shards"] = 1
s["number_of_replicas"] = 0
s["pluggable.dataformat.enabled"] = True
s["pluggable.dataformat"] = "composite"
s["composite.primary_data_format"] = "parquet"
print(json.dumps(m))
PY
)
    else
        log "creating index '$INDEX' with default (Lucene) settings"
        body=$(python3 - <<PY
import json
with open("$MAPPING_FILE") as f:
    m = json.load(f)
s = m.setdefault("settings", {})
if "index" in s:
    s["index"].pop("sort.field", None)
    s["index"].pop("sort.order", None)
s["number_of_shards"] = 1
s["number_of_replicas"] = 0
print(json.dumps(m))
PY
)
    fi

    # Create index
    local resp
    resp=$(curl -sS -X PUT "$ENDPOINT/$INDEX" -H 'Content-Type: application/json' -d "$body")
    if ! echo "$resp" | grep -q '"acknowledged":true'; then
        fatal "index creation failed: $resp"
    fi
    log "index created: $(echo "$resp" | head -c 120)"

    # Bulk index data
    log "bulk-indexing data from $DATA_FILE"
    resp=$(curl -sS -X POST "$ENDPOINT/$INDEX/_bulk?refresh=true" \
        -H 'Content-Type: application/x-ndjson' \
        --data-binary "@$DATA_FILE")
    local errors
    errors=$(echo "$resp" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("errors"))' 2>/dev/null || echo "?")
    if [ "$errors" = "True" ]; then
        log "WARNING: bulk had errors. Response: $resp"
        return 1
    fi

    # Report doc count
    local count
    count=$(curl -sS "$ENDPOINT/$INDEX/_count" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])' 2>/dev/null || echo "?")
    log "setup complete: $count doc(s) in '$INDEX'"
}

# ─── teardown ───────────────────────────────────────────────────────────────
cmd_teardown() {
    check_cluster
    curl -sS -X DELETE "$ENDPOINT/$INDEX" > /dev/null 2>&1
    log "deleted index '$INDEX'"
}

# ─── force-routing toggle ───────────────────────────────────────────────────
set_force_routing() {
    local val="$1"
    local resp
    resp=$(curl -sS -X PUT "$ENDPOINT/_cluster/settings" \
        -H 'Content-Type: application/json' \
        -d "{\"persistent\":{\"plugins.calcite.analytics.force_routing\":\"$val\"}}")
    if echo "$resp" | grep -q '"acknowledged":true'; then
        log "set plugins.calcite.analytics.force_routing=$val"
    else
        log "WARNING: setting force_routing=$val may have failed: $resp"
    fi
}

# ─── run a single query ─────────────────────────────────────────────────────
# Args: N (query number)
# Returns: 0 on success, 1 on failure
# Prints: tab-separated line "qN<TAB>status<TAB>duration_ms<TAB>summary"
run_one() {
    local n="$1"
    local file="$QUERIES_DIR/q${n}.ppl"
    if [ ! -f "$file" ]; then
        printf 'q%s\tMISSING\t0\tquery file not found\n' "$n"
        return 1
    fi

    local ppl
    ppl=$(extract_ppl "$file")

    # JSON-encode the query body
    local body
    body=$(python3 -c 'import json,sys; print(json.dumps({"query": sys.stdin.read()}))' <<< "$ppl")

    # Run, capture body + status + duration
    local start_ms end_ms status http_code out
    start_ms=$(date +%s%3N)
    out=$(curl -sS -o /tmp/clickbench_resp.json -w '%{http_code}' -X POST \
        "$ENDPOINT/_plugins/_ppl" \
        -H 'Content-Type: application/json' \
        -d "$body")
    http_code="$out"
    end_ms=$(date +%s%3N)
    local dur=$((end_ms - start_ms))

    # Interpret via a self-contained python one-liner reading the saved response
    local summary
    summary=$(HTTP_CODE="$http_code" python3 <<'PY'
import json, os
http = os.environ.get("HTTP_CODE","?")
try:
    with open("/tmp/clickbench_resp.json") as f:
        d = json.load(f)
except Exception as e:
    print(f"parse-err http={http}: {e}")
    raise SystemExit(0)

if http == "200":
    rows = d.get("total", len(d.get("datarows", [])))
    cols = len(d.get("schema", []))
    print(f"{rows} rows, {cols} cols")
else:
    e = d.get("error", {}) if isinstance(d, dict) else {}
    t = e.get("type", "?")
    r = (e.get("reason") or e.get("details") or "")
    # Take the meaty part - usually 'Exception <Throwable>: ...' pattern
    # Compact whitespace and truncate
    r = " ".join(r.split())[:160]
    print(f"{t}: {r}")
PY
)
    if [ "$http_code" = "200" ]; then
        printf 'q%s\tPASS\t%d\t%s\n' "$n" "$dur" "$summary"
        return 0
    else
        printf 'q%s\tFAIL\t%d\t%s\n' "$n" "$dur" "$summary"
        return 1
    fi
}

# ─── run all 43 ─────────────────────────────────────────────────────────────
cmd_run() {
    check_cluster
    [ -d "$QUERIES_DIR" ] || fatal "missing queries dir: $QUERIES_DIR"

    if [ "$FORCE_ROUTING" = "1" ]; then
        set_force_routing "true"
    else
        set_force_routing "false"
    fi

    local passed=0 failed=0 total=0
    local results_file="/tmp/clickbench_results.tsv"
    : > "$results_file"

    log "running queries 1..43 against index='$INDEX' force_routing=$FORCE_ROUTING"
    printf '\n%-6s %-6s %-10s %s\n' "Q" "STATUS" "TIME(ms)" "DETAIL"
    printf '%-6s %-6s %-10s %s\n' "--" "------" "--------" "------"
    for n in $(seq 1 43); do
        line=$(run_one "$n")
        echo "$line" >> "$results_file"
        # Pretty-print
        IFS=$'\t' read -r q st ms detail <<< "$line"
        printf '%-6s %-6s %-10s %s\n' "$q" "$st" "$ms" "$detail"
        total=$((total+1))
        if [ "$st" = "PASS" ]; then
            passed=$((passed+1))
        else
            failed=$((failed+1))
        fi
    done

    printf '\n─── Summary ─────────────────────────\n'
    printf 'Total:  %d\n' "$total"
    printf 'Passed: %d\n' "$passed"
    printf 'Failed: %d\n' "$failed"
    printf 'Results TSV: %s\n\n' "$results_file"

    [ "$failed" -eq 0 ]
}

# ─── run one ────────────────────────────────────────────────────────────────
cmd_one() {
    local n="${1:-}"
    [ -n "$n" ] || fatal "usage: $0 one <N>"
    check_cluster
    if [ "$FORCE_ROUTING" = "1" ]; then
        set_force_routing "true"
    fi
    local file="$QUERIES_DIR/q${n}.ppl"
    [ -f "$file" ] || fatal "missing query file: $file"

    echo "--- q${n}.ppl ---"
    cat "$file"
    echo "--- query body ---"
    extract_ppl "$file"
    echo "--- response ---"
    local ppl body
    ppl=$(extract_ppl "$file")
    body=$(python3 -c 'import json,sys; print(json.dumps({"query": sys.stdin.read()}))' <<< "$ppl")
    curl -sS -X POST "$ENDPOINT/_plugins/_ppl" \
        -H 'Content-Type: application/json' \
        -d "$body" | python3 -m json.tool 2>/dev/null || cat /tmp/clickbench_resp.json
    echo
}

# ─── main ───────────────────────────────────────────────────────────────────
case "${1:-}" in
    setup)    cmd_setup ;;
    run)      cmd_run ;;
    one)      shift; cmd_one "$@" ;;
    teardown) cmd_teardown ;;
    all)      cmd_setup && cmd_run ;;
    *)
        echo "Usage: $0 {setup|run|one <N>|teardown|all}" >&2
        echo ""
        echo "Env vars: ENDPOINT INDEX PARQUET FORCE_ROUTING OS_SQL_DIR" >&2
        exit 2 ;;
esac
