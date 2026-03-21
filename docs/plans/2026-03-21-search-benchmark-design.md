# Search/Aggregation Benchmark Design (Parquet vs Default)

## Goal
Measure search and aggregation latency (p50, p90) comparing parquet doc_values codec against default, using 1% http_logs corpus.

## Decisions
- Re-index as part of benchmark (self-contained, reproducible)
- Use built-in OSB `append-no-conflicts` test procedure (index + standard search/agg suite)
- New standalone script `benchmark-search.sh` (don't modify existing `benchmark.sh`)
- Report p50 and p90 latency per operation
- Pass/fail: no operation regresses >20% on p90

## Script: `benchmark-search.sh`

### Flow
1. Write params files (baseline + parquet)
2. For each config (baseline, parquet):
   - Clean data, start OpenSearch
   - Run OSB with `--test-procedure append-no-conflicts`
   - Save CSV results
   - Stop OpenSearch
3. Extract p50 and p90 latency per search operation from both CSVs
4. Generate `benchmark-results/search-summary.md`

### Output Files
- `benchmark-results/search-baseline.csv`
- `benchmark-results/search-parquet.csv`
- `benchmark-results/search-baseline-output.txt`
- `benchmark-results/search-parquet-output.txt`
- `benchmark-results/search-summary.md`

### Summary Table Format
```
| Operation | Baseline p50 | Parquet p50 | Diff | Baseline p90 | Parquet p90 | Diff |
```

### Key Differences from benchmark.sh
- `append-no-conflicts` instead of `append-no-conflicts-index-only`
- Extracts latency percentiles per operation instead of store size
- Separate output files prefixed with `search-`
