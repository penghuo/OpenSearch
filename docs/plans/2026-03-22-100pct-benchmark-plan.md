# 100% http_logs Parquet Benchmark Plan

## Goals
1. Search performance within 10% of baseline (p90 latency) on ALL operations
2. Ingestion throughput: no regression
3. Storage size: 40% smaller than baseline

## Approach
1. Modify benchmark scripts to support 100% corpus + saved baseline
2. Build OpenSearch
3. Run baseline ONCE with 100% http_logs, save results
4. Run parquet with 100% http_logs, compare against saved baseline
5. Analyze results — identify any operations exceeding 10% p90 regression
6. If regressions found: optimize ParquetDocValuesReader, rebuild, re-run parquet only
7. Iterate until all targets met

## Script Changes
- `benchmark-search.sh`: Default to 100% corpus, support `MODE=parquet` to skip baseline
- Save baseline results permanently (don't delete on re-run)

## Known Risk Areas (from 1% results)
- `asc_sort_size`: +19.1% p90 regression
- `desc_sort_timestamp`: +17.1% p90 regression  
- `asc_sort_with_after_timestamp`: +15.0% p90 regression
- All are sort operations → NumericDocValues iteration hot path
