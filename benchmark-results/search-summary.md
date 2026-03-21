# Search Benchmark Summary (1% http_logs, append-no-conflicts)

| Operation | Baseline p50 (ms) | Parquet p50 (ms) | p50 Diff | Baseline p90 (ms) | Parquet p90 (ms) | p90 Diff |
|-----------|-------------------|-------------------|----------|-------------------|-------------------|----------|
| asc_sort_size | 7.053894994896837 | 7.434412953443825 | 5.4%     | 7.3469428811222315 | 8.081067656166852 | 10.0%    |
| asc_sort_timestamp | 7.032771012745798 | 8.833430969389156 | 25.6%    | 7.6101348153315485 | 9.376489359419793 | 23.2%    |
| asc-sort-timestamp-after-force-merge-1-seg | 5.029240011936054 | 7.786795526044443 | 54.8%    | 5.460359001881443 | 9.082234400557354 | 66.3%    |
| asc_sort_with_after_timestamp | 6.367519992636517 | 5.990558973280713 | -5.9%    | 6.596251885639504 | 6.771238654619083 | 2.7%     |
| asc-sort-with-after-timestamp-after-force-merge-1-seg | 5.83824647765141  | 5.84544352022931  | 0.1%     | 6.234219085308723 | 6.383744347840548 | 2.4%     |
| desc_sort_size | 7.785114998114295 | 9.677391033619642 | 24.3%    | 8.584508407511748 | 10.640803206479179 | 24.0%    |
| desc_sort_timestamp | 9.13384699379094  | 11.832743999548256 | 29.5%    | 10.498945697327144 | 13.452455488732085 | 28.1%    |
| desc-sort-timestamp-after-force-merge-1-seg | 6.85969099868089  | 9.93715351796709  | 44.9%    | 8.01400019263383  | 10.773365205386655 | 34.4%    |
| desc_sort_with_after_timestamp | 7.357952505117282 | 7.9744769900571555 | 8.4%     | 7.703720513381995 | 9.202755492879078 | 19.5%    |
| desc-sort-with-after-timestamp-after-force-merge-1-seg | 6.673912997939624 | 9.176418971037492 | 37.5%    | 7.315515304799192 | 10.973699280293658 | 50.0%    |
| hourly_agg | 14.659628999652341 | 13.995726534631103 | -4.5%    | 16.80598940292839 | 15.806592942681164 | -5.9%    |
| hourly_agg_with_filter | 21.06172899948433 | 26.042871992103755 | 23.7%    | 24.01322149380576 | 28.29878187039867 | 17.8%    |
| hourly_agg_with_filter_and_metrics | 38.06881498894654 | 37.023646029410884 | -2.7%    | 39.75624848972075 | 44.13903754320927 | 11.0%    |
| match-all | 9.34924949251581  | 9.8620980134001   | 5.5%     | 10.655516196857207 | 12.761853600386532 | 19.8%    |
| multi_term_agg | 136.5916029899381 | 130.2164724911563 | -4.7%    | 152.63772217731457 | 142.82875356730077 | -6.4%    |
| range     | 5.741893008234911 | 5.449460499221459 | -5.1%    | 6.231572607066482 | 6.102865369757637 | -2.1%    |
| scroll    | 185.43715600389987 | 353.1745665241033 | 90.5%    | 191.4702136942651 | 362.8093557083048 | 89.5%    |
| status-200s-in-range | 10.143871084437706 | 512.0937270985451 | 4948.3%  | 44.54353438050023 | 673.2021834584884 | 1411.3%  |
| status-400s-in-range | 7.612373505253345 | 713.0920715280809 | 9267.5%  | 8.92691522021778  | 916.5769360086415 | 10167.6% |
| term      | 7.723050992353819 | 8.05589098308701  | 4.3%     | 8.613393199630082 | 9.000794467283413 | 4.5%     |

## Pass/Fail
- Target: no operation regresses >20% on p90 latency
- **VERDICT: FAIL** — 9 out of 20 operations regress >20% on p90

### Failing Operations (p90 regression >20%)
| Operation | p90 Diff |
|-----------|----------|
| asc_sort_timestamp | +23.2% |
| asc-sort-timestamp-after-force-merge-1-seg | +66.3% |
| desc_sort_size | +24.0% |
| desc_sort_timestamp | +28.1% |
| desc-sort-timestamp-after-force-merge-1-seg | +34.4% |
| desc-sort-with-after-timestamp-after-force-merge-1-seg | +50.0% |
| scroll | +89.5% |
| status-200s-in-range | +1411.3% |
| status-400s-in-range | +10167.6% |

### Passing Operations (p90 regression ≤20%)
| Operation | p90 Diff |
|-----------|----------|
| asc_sort_size | +10.0% |
| asc_sort_with_after_timestamp | +2.7% |
| asc-sort-with-after-timestamp-after-force-merge-1-seg | +2.4% |
| desc_sort_with_after_timestamp | +19.5% |
| hourly_agg | -5.9% |
| hourly_agg_with_filter | +17.8% |
| hourly_agg_with_filter_and_metrics | +11.0% |
| match-all | +19.8% |
| multi_term_agg | -6.4% |
| range | -2.1% |
| term | +4.5% |
