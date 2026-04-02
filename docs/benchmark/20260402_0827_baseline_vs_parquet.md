# Baseline vs Parquet — 2026-04-02 (Default Iterations)

Dataset: 5% http_logs | Default warmup (50-500) | Default iterations (100) | Mode: http_log_basic

## P50 Service Time (ms)

| # | Operation | Baseline | Parquet | Δ% | Status |
|---|-----------|--------:|--------:|---:|--------|
| 1 | match-all | 3.26 | 4.10 | +25.6% | ❌ |
| 2 | term | 2.43 | 3.83 | +57.7% | ❌ |
| 3 | range | 2.58 | 3.06 | +19.0% | ❌ |
| 4 | status-200s-in-range | 4.45 | 7.56 | +69.9% | ❌ |
| 5 | status-400s-in-range | 2.79 | 9.15 | +228.1% | ❌ |
| 6 | hourly_agg | 30.44 | 25.12 | -17.5% | ✅ |
| 7 | hourly_agg_with_filter | 68.87 | 66.12 | -4.0% | ✅ |
| 8 | hourly_agg_with_filter_and_metrics | 109.25 | 80.45 | -26.4% | ✅ |
| 9 | multi_term_agg | 1279.75 | 1017.28 | -20.5% | ✅ |
| 10 | scroll | 207.56 | 495.98 | +139.0% | ❌ |
| 11 | desc_sort_size | 4.15 | 12.42 | +199.2% | ❌ |
| 12 | asc_sort_size | 4.34 | 3.60 | -17.0% | ✅ |
| 13 | desc_sort_timestamp | 8.50 | 15.72 | +85.0% | ❌ |
| 14 | asc_sort_timestamp | 3.72 | 5.75 | +54.9% | ❌ |
| 15 | desc_sort_with_after_timestamp | 5.07 | 7.54 | +48.8% | ❌ |
| 16 | asc_sort_with_after_timestamp | 3.38 | 3.33 | -1.6% | ✅ |

## P90 Service Time (ms)

| # | Operation | Baseline | Parquet | Δ% | Status |
|---|-----------|--------:|--------:|---:|--------|
| 1 | match-all | 3.84 | 5.17 | +34.4% | ❌ |
| 2 | term | 2.77 | 7.24 | +161.5% | ❌ |
| 3 | range | 3.04 | 5.17 | +70.2% | ❌ |
| 4 | status-200s-in-range | 5.34 | 9.82 | +83.8% | ❌ |
| 5 | status-400s-in-range | 3.25 | 15.14 | +366.6% | ❌ |
| 6 | hourly_agg | 33.95 | 28.25 | -16.8% | ✅ |
| 7 | hourly_agg_with_filter | 79.36 | 76.83 | -3.2% | ✅ |
| 8 | hourly_agg_with_filter_and_metrics | 126.26 | 91.14 | -27.8% | ✅ |
| 9 | multi_term_agg | 1382.50 | 1180.78 | -14.6% | ✅ |
| 10 | scroll | 219.70 | 557.14 | +153.6% | ❌ |
| 11 | desc_sort_size | 4.54 | 17.75 | +290.8% | ❌ |
| 12 | asc_sort_size | 5.13 | 4.00 | -22.0% | ✅ |
| 13 | desc_sort_timestamp | 9.41 | 21.01 | +123.4% | ❌ |
| 14 | asc_sort_timestamp | 4.54 | 6.54 | +44.1% | ❌ |
| 15 | desc_sort_with_after_timestamp | 6.02 | 8.84 | +46.8% | ❌ |
| 16 | asc_sort_with_after_timestamp | 3.85 | 3.75 | -2.5% | ✅ |

**P90 Score: 6/16 PASS (target: ≤10% regression)**
