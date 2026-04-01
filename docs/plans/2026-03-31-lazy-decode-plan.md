# Parquet DocValues Performance Plan — Lazy Decode + Sort Optimization

## Goal

Achieve ≤10% p90 latency regression on sort operations by making Parquet DocValues decode lazy, leveraging the existing DocValuesSkipper to skip non-competitive blocks.

## Current State (7/20 pass)

- `loadBlockPackedData()` eagerly decodes ALL blocks into `long[docCount]` (96MB for 12M docs)
- DocValuesSkipper exists with per-block min/max — but values are already fully decoded before skipper is consulted
- Block size: 16384 docs (BLOCK_SHIFT=14), ~732 blocks per 12M-doc segment
- Block metadata: ~24KB (732 × 33 bytes) — cheap to load
- Decoded values: 96MB per numeric field — expensive and unnecessary for top-N queries

## Key Insight

ClickHouse on Parquet beats Elasticsearch by:
1. Streaming top-N with bounded memory (heap of size N, not full sort)
2. Lazy materialization (decode only sort column, then only N result rows)
3. Row group min/max pruning for ORDER BY

Lucene already has the infrastructure:
- `TopFieldCollector` uses priority queue with early termination
- `NumericComparator` uses `DocValuesSkipper` to skip non-competitive blocks
- Index sort enables segment-level early termination after N docs

The missing piece: **Parquet DocValues decodes everything eagerly, defeating all of Lucene's skip optimizations.**

## Plan

### Phase 1: Lazy Block Decode (Core Fix)

**What**: Change `loadBlockPackedData()` to decode block metadata eagerly but defer per-block value decoding until first access.

**Current flow**:
```
warmUp → loadNumeric → loadBlockPackedData → decode ALL 732 blocks → long[12M]
```

**Target flow**:
```
warmUp → loadNumeric → loadBlockMetadata (24KB) → populate DocValuesSkipper
                     ↓ (on access)
         advanceExact(docId) → find block → decode single block (16K values) → cache block
```

**Implementation**:
1. Split `loadBlockPackedData()` into two phases:
   - `loadBlockMetadata()` — read block headers (offset, min, max, gcd, bpv) into arrays. ~24KB. Populate skipperCache.
   - `decodeBlock(blockIdx)` — decode a single 16K-value block on demand. Cache decoded blocks.
2. Replace `CachedNumericPacked` (wraps flat `long[]`) with `LazyBlockNumeric` that:
   - Implements `NumericDocValues.advanceExact(int target)` — finds block, decodes if needed, returns value
   - Caches decoded blocks (LRU or simple array of `long[]` per block)
   - Thread-safe: use `volatile` + double-checked locking per block, or `AtomicReferenceArray<long[]>`

**Risk**: Previous ralph attempts at lazy decode failed due to synchronized per-block decode overhead. Mitigation: use lock-free `AtomicReferenceArray`, no synchronization on hot path after first decode.

### Phase 2: Verify DocValuesSkipper Integration

**What**: Confirm Lucene's `NumericComparator` actually uses our DocValuesSkipper during sort queries.

1. Add logging/metrics to `getSkipper()` to verify it's called during sort queries
2. Add logging to `DocValuesSkipper.advance()` to verify blocks are being skipped
3. Run benchmark with logging — count how many blocks are actually decoded vs skipped for each sort operation
4. If skipper is NOT being used, investigate why (missing `getDocValuesSkipper()` override in LeafReader?)

### Phase 3: Benchmark and Iterate

1. Run `./benchmark.sh all both clean` with lazy decode
2. Compare sort operations against current 7/20 baseline
3. Expected improvements:
   - `asc_sort_timestamp LIMIT 10`: decode ~1-2 blocks instead of 732 (if skipper works)
   - `asc-sort-timestamp-after-force-merge-1-seg`: same query on single segment, should benefit equally
   - `desc_sort_*`: reverse iteration still needs investigation
4. If sort improves but aggregations regress, add warmUp hint: eagerly decode all blocks for aggregation-heavy fields

### Phase 4: Index Sort (Config-Level Optimization)

**What**: Configure index sort on `@timestamp` for http_logs workload.

- Lucene's `TopFieldCollector` terminates after N docs when search sort matches index sort
- For `ORDER BY @timestamp ASC LIMIT 10`, only first 10 docs decoded — rest of segment skipped entirely
- This is the strongest optimization and requires zero code changes to Parquet DocValues
- Trade-off: index sort adds overhead at indexing time (merge must sort)

## Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Sort operations passing | 3/10 | ≥7/10 |
| Aggregation operations passing | 4/10 | ≥4/10 (no regression) |
| Overall score | 7/20 | ≥14/20 |
| Correctness | 687 tests, 0 failures | 687 tests, 0 failures |

## Execution Order

1. Phase 1 (lazy decode) — highest impact, addresses root cause
2. Phase 2 (verify skipper) — ensures Lucene's optimization actually fires
3. Phase 3 (benchmark) — measure impact
4. Phase 4 (index sort) — config-level win, independent of code changes
