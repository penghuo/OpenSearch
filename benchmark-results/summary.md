# Benchmark Summary (1% http_logs, index-only)

| Metric                    | Baseline       | Parquet        | Diff       |
|---------------------------|----------------|----------------|------------|
| Store Size (GB)           | 0.18530631065368652    | 0.11119664087891579    | -40.0% |
| Indexing Time (min)       | 1.7388333333333332  | 1.6631333333333334  | -4.4% |

Note: Median Throughput not available (1% corpus completes during OSB warmup phase).

## Pass/Fail
- Storage reduction: -40.0% (target: significant reduction)
- Indexing time: -4.4% (target: no major regression)
