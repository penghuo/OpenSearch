# Parquet Doc Values Performance Plan

## Goal

Parquet doc_values format achieves ≤10% p90 latency regression vs baseline on ALL 21 search operations (OSB http_logs workload, 5% corpus, ~12M docs).

Secondary targets:
- yamlRestTestParquet: 0 failures (correctness preserved)
- Storage reduction ≥35%
- Ingestion: no regression

## Current Status

**Correctness:** yamlRestTestParquet — 687 tests, 0 failures, 36 skipped ✅

**Benchmark** (2026-03-27, 3 iterations, p100 metric):

| # | Operation | B svc50 | P svc50 | svc Δ | B p100 | P p100 | p100 Δ | Status |
|---|-----------|--------:|--------:|------:|-------:|-------:|-------:|--------|
| 1 | match-all | 12.17 | 13.02 | +7.0% | 17.95 | 14.65 | -18.4% | ✅ |
| 2 | term | 10.58 | 12.95 | +22.4% | 18.51 | 26.98 | +45.8% | ❌ |
| 3 | range | 6.24 | 5.98 | -4.2% | 9.78 | 7.99 | -18.3% | ✅ |
| 4 | status-200s-in-range | 23.08 | 6.31 | -72.7% | 107.42 | 7.69 | -92.8% | ✅ |
| 5 | status-400s-in-range | 11.03 | 4.29 | -61.1% | 14.95 | 6.13 | -59.0% | ✅ |
| 6 | hourly_agg | 25.36 | 82.31 | +224.6% | 34.30 | 89.09 | +159.7% | ❌ |
| 7 | hourly_agg_with_filter | 99.20 | 10.15 | -89.8% | 106.40 | 16.55 | -84.4% | ✅ |
| 8 | hourly_agg_with_filter_and_metrics | 174.64 | 10.52 | -94.0% | 187.26 | 16.80 | -91.0% | ✅ |
| 9 | multi_term_agg | 2132.44 | 16.14 | -99.2% | 2197.51 | 22.12 | -99.0% | ✅ |
| 10 | scroll | 156.41 | 328.67 | +110.1% | 167.11 | 340.48 | +103.7% | ❌ |
| 11 | desc_sort_size | 7.70 | 5.94 | -22.9% | 13.08 | 9.67 | -26.0% | ✅ |
| 12 | asc_sort_size | 7.51 | 4.49 | -40.2% | 10.89 | 7.08 | -35.0% | ✅ |
| 13 | desc_sort_timestamp | 8.86 | 4.81 | -45.8% | 12.13 | 8.11 | -33.1% | ✅ |
| 14 | asc_sort_timestamp | 5.14 | 4.44 | -13.7% | 8.25 | 8.20 | -0.6% | ✅ |
| 15 | desc_sort_with_after_timestamp | 5.64 | 5.76 | +2.0% | 8.88 | 8.85 | -0.4% | ✅ |
| 16 | asc_sort_with_after_timestamp | 3.87 | 4.15 | +7.1% | 6.64 | 7.64 | +15.1% | ❌ |
| 17 | desc-sort-ts-after-FM | 5.51 | 3.75 | -32.0% | 7.43 | 5.33 | -28.3% | ✅ |
| 18 | asc-sort-ts-after-FM | 4.24 | 3.42 | -19.3% | 5.83 | 4.94 | -15.3% | ✅ |
| 19 | desc-sort-with-after-ts-after-FM | 5.13 | 4.85 | -5.3% | 8.85 | 8.60 | -2.9% | ✅ |
| 20 | asc-sort-with-after-ts-after-FM | 3.44 | 3.94 | +14.4% | 6.24 | 7.00 | +12.2% | ❌ |

**Score: 15 PASS / 5 FAIL** (p100 with 3 iterations — noisy metric)

**Storage:** 0.897 GB → 0.585 GB (-34.8%) — borderline miss on ≥35% target

**5 failing operations:**
- `hourly_agg` (+159.7%) — FilterRewrite optimization not applied, major regression
- `scroll` (+103.7%) — 25K DerivedSource reconstructions, architectural overhead
- `term` (+45.8%) — p100 spike, svc50 only +22%, likely noise with 3 iterations
- `asc_sort_with_after_timestamp` (+15.1%) — borderline, svc50 only +7.1%
- `asc-sort-with-after-ts-after-FM` (+12.2%) — borderline, svc50 +14.4%

**Key insight:** Current benchmark uses only 3 iterations → p90 unavailable for 19/20 ops, p100 is noisy. The 2 borderline failures and `term` may pass with proper p90 from 10 iterations.

## How to Use Benchmark Tools

### Unified Harness: `benchmark.sh`

```bash
./benchmark.sh <phase> [mode] [clean]
```

| Phase | What it does | Est. Time |
|-------|-------------|-----------|
| `build` | `localDistro -x test` | 1-10 min |
| `test` | `yamlRestTestParquet` correctness gate — aborts on failure | 3-5 min |
| `bench` | OSB benchmark (10 iterations, 1 warmup, 5% http_logs) | 15-30 min per mode |
| `all` | build → test → bench (stops on failure) | 20-45 min |
| `summary` | Regenerate summary from existing CSVs | instant |

**Mode** (bench/all only): `parquet` (default), `baseline`, `both`
**Clean** (bench/all only): `clean` (wipe data, required after code changes), `no` (reuse, default)

### Common Workflows

```bash
# Full pipeline after code changes — build, test correctness, benchmark
./benchmark.sh all parquet clean

# Re-run both baseline and parquet from scratch
./benchmark.sh all both clean

# Quick re-benchmark without rebuild (no code changes, reuse data)
./benchmark.sh bench parquet no

# Correctness check only
./benchmark.sh test

# Regenerate summary table from existing CSV data
./benchmark.sh summary
```

### Output Files

| File | Contents |
|------|----------|
| `benchmark-results/YYYYMMDD_HHMMSS_summary.md` | Pass/fail table with ≤10% p90 target |
| `benchmark-results/search-summary.md` | Latest summary (copy of above) |
| `benchmark-results/search-baseline.csv` | Raw OSB metrics for baseline |
| `benchmark-results/search-parquet.csv` | Raw OSB metrics for parquet |
| `benchmark-results/opensearch-parquet.log` | Full OpenSearch log from parquet run |
| `benchmark-results/filterrewrite-parquet.log` | FilterRewrite diagnostic lines only |
| `benchmark-results/storage.txt` | Index sizes from `_cat/indices` |

### Reading the Summary

The summary table shows:
- **B svc50 / P svc50**: Median service time (baseline vs parquet) — stable metric
- **B p90 / P p90**: 90th percentile latency — target metric (falls back to p100 if p90 unavailable, labeled)
- **p90 Δ**: Percentage change — negative is faster, positive is slower
- **Status**: ✅ if ≤10% regression, ❌ if >10% (with metric label)

### Long-Running Command Rules

Benchmark runs take 15-45 minutes. When running via agent:

```bash
# Launch async
nohup bash -c 'cd /local/home/penghuo/oss/OpenSearch && ./benchmark.sh all parquet clean > /tmp/benchmark-output.txt 2>&1' &>/dev/null &

# Poll every 30s
tail -5 /tmp/benchmark-output.txt
```

### Legacy Script

`benchmark-search.sh` still exists for backward compatibility but uses 3 iterations and lacks the test gate. Prefer `benchmark.sh`.
