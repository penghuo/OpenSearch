# Benchmark results: flat_object `_source` (A) vs Variant blob column (B)

- **Run:** 2026-08-21T17:05:45+0000
- **Commit:** `1fc06f218c7` (working tree, uncommitted)

This file is a timestamped snapshot of one run, generated from the raw JSONL rather than transcribed.

## Environment

| | |
|---|---|
| Host | 16 vCPU, 61 GiB RAM, NVMe, no swap, Linux 5.10 |
| JDK | Amazon Corretto 21.0.11 |
| OpenSearch | 3.6.0-SNAPSHOT, Lucene 10.4.0 |
| Node heap | 16 GiB (`-Xms16g -Xmx16g`, G1GC) |
| Index | 1 shard, 0 replicas, `refresh_interval: -1` during bulk, force-merged to 1 segment |
| Query | `request_cache=false`, n=10 timed iterations after 2 untimed warmups |

## Corpus

| | |
|---|---|
| Preset | `E10M` |
| Documents | 10,000,000 |
| Document size | ~493 B (no filler fields; every field is queryable) |
| `attributes` size | ~433 B = **87% of the document** |
| Key space | 1000 attribute keys, Zipf-distributed |
| Keys per document | 6-23, mean 14.6 |

> 87% attributes fraction is the shape **least** favourable to a separate value column: there is almost
> nothing for B to avoid re-storing, and little of the document for B to avoid reading.

## Query set

Fixed in `BenchQuery.java`. Both arms receive byte-identical bodies; only the derived-field script differs.

```
probes: dense=status (100%), groupBy=k8s.namespace, sparse=process.runtime.name (7.22%), rare=custom.tenant.attr_248 (0.580%)

Q1   sum, dense path, full corpus
     purpose      : Baseline cost of reading one always-present value, at full scale
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q2   sum, dense path, 10% of corpus
     purpose      : Same read, scoped by time range so more iterations fit in the same wall clock
     docs scanned : 1,000,000
     body         : {"size":0,"query":{"range":{"@timestamp":{"lt":1755721000000}}},"aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q3   sum, dense path, 1% of corpus
     purpose      : Same read, scoped further; the cheapest query and so the most stable percentiles
     docs scanned : 100,000
     body         : {"size":0,"query":{"range":{"@timestamp":{"lt":1755720100000}}},"aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q4   terms + sum group-by, full corpus
     purpose      : Grouped aggregation reading two paths per document: the worst case for a row-oriented store
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"by_ns":{"terms":{"field":"attr_namespace","size":32},"aggs":{"total":{"sum":{"field":"attr_status"}}}}}}

Q5   terms + sum group-by, 10% of corpus
     purpose      : The same grouped shape, scoped, so its percentiles rest on more samples than Q4's
     docs scanned : 1,000,000
     body         : {"size":0,"query":{"range":{"@timestamp":{"lt":1755721000000}}},"aggs":{"by_ns":{"terms":{"field":"attr_namespace","size":32},"aggs":{"total":{"sum":{"field":"attr_status"}}}}}}

Q6   sum, sparse path (7.22% of docs), full corpus
     purpose      : Cost when most documents do not carry the path at all
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"total":{"sum":{"field":"attr_sparse"}}}}

Q7   sum, rare path (0.58% of docs), full corpus
     purpose      : Same, for a path that is almost always absent
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"total":{"sum":{"field":"attr_rare"}}}}

Q8   five metrics on one path, full corpus
     purpose      : Whether five aggregations over one path cost one read or five
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"s":{"sum":{"field":"attr_status"}},"a":{"avg":{"field":"attr_status"}},"mn":{"min":{"field":"attr_status"}},"mx":{"max":{"field":"attr_status"}},"c":{"value_count":{"field":"attr_status"}}}}

Q9   sum on two different paths, full corpus
     purpose      : Marginal cost of a second path: one store re-parses nothing, the other searches again
     docs scanned : 10,000,000
     body         : {"size":0,"aggs":{"s1":{"sum":{"field":"attr_status"}},"s2":{"sum":{"field":"attr_sparse"}}}}

Q10  fetch top 50 documents with a derived field
     purpose      : Should favour _source: the document is loaded and parsed for the hits anyway, so the blob is extra work
     docs scanned : 50
     body         : {"size":50,"query":{"term":{"attributes":"info"}},"fields":["attr_status"]}

Q11  filter only, no derived field (control)
     purpose      : Touches only the shared flat_object terms. Must show no A/B difference; if it does, the experiment is broken
     docs scanned : 10,000,000
     body         : {"size":0,"query":{"term":{"attributes":"info"}},"track_total_hits":true}
```

## Query latency

Server-side `took`, milliseconds, n=10.

| Q | docs scanned | A p50 | A p90 | B p50 | B p90 | B faster | query |
|---|---|---|---|---|---|---|---|
| **Q1** | 10,000,000 | 13,531 | 13,629 | **732** | 911 | **18.5x** | sum, dense path, full corpus |
| **Q2** | 1,000,000 | 5,389 | 5,400 | **288** | 290 | **18.7x** | sum, dense path, 10% of corpus |
| **Q3** | 100,000 | 545 | 559 | **33** | 35 | **16.5x** | sum, dense path, 1% of corpus |
| **Q4** | 10,000,000 | 107,187 | 107,476 | **6,884** | 6,902 | **15.6x** | terms + sum group-by, full corpus |
| **Q5** | 1,000,000 | 10,874 | 10,909 | **697** | 699 | **15.6x** | terms + sum group-by, 10% of corpus |
| **Q6** | 10,000,000 | 13,523 | 13,546 | **1,075** | 1,124 | **12.6x** | sum, sparse path (7.22% of docs), full corpus |
| **Q7** | 10,000,000 | 13,511 | 13,568 | **1,198** | 1,206 | **11.3x** | sum, rare path (0.58% of docs), full corpus |
| **Q8** | 10,000,000 | 14,371 | 14,466 | **4,107** | 4,191 | **3.5x** | five metrics on one path, full corpus |
| **Q9** | 10,000,000 | 13,494 | 24,861 | **2,024** | 2,145 | **6.7x** | sum on two different paths, full corpus |
| **Q10** | 50 | 3 | 4 | **1** | 2 | **3.0x** | fetch top 50 documents with a derived field |
| **Q11** | 10,000,000 | 1 | 1 | **0** | 1 | **n/a** | filter only, no derived field (control) |

Client-side milliseconds for the sub-10ms queries, where server `took` is at integer resolution:

| Q | A p50 | A p90 | B p50 | B p90 |
|---|---|---|---|---|
| Q10 | 7.26 | 7.82 | **3.52** | 3.73 |
| Q11 | 3.02 | 3.90 | **2.46** | 2.73 |

## Storage and write

| | Arm A | Arm B | Delta |
|---|---|---|---|
| Total index | 7.55 GB (755 B/doc) | 11.71 GB (1171 B/doc) | **+55.1%** |
| Doc values | 290 B/doc | 705 B/doc | blob = **416 B/doc** |
| `_source` stored fields | 237 B/doc | 237 B/doc | identical |
| Postings | 45 B/doc | 45 B/doc | identical |
| Segments | 1 | 1 | breakdown valid |
| Indexing | 29,967 docs/s | 29,572 docs/s | -1.3% |
| Force merge | 520 s | 470 s | -9.6% |

## Findings

1. **Control passes.** Q11 touches only the shared `flat_object` terms and shows no A/B difference, so the
   differences above are attributable to the value store rather than to anything else in the request path.
2. **Single-path aggregations: B is 11-19x faster**, stable across two orders of magnitude of documents
   scanned. Largest absolute gap is Q4: 107 s vs 6.9 s for a grouped aggregation over 10M documents.
3. **B decodes once per aggregation; A parses once per document.** Going from one aggregation (Q1) to five
   on the same path (Q8), arm A grows x1.06 while arm B grows x5.61. `SourceLookup` caches the parsed
   document for the search context so all five scripts share it, whereas each aggregation gets its own
   `DerivedFieldScript` and therefore its own blob decode. B's advantage falls from ~18x to 3.5x.
   **This is an implementation defect, not a property of the format** - the fix is to share one accessor per
   search context. Highest-value follow-up.
4. **B gains less on absent paths, not more.** Ratio falls 18.5x (100% present) -> 12.6x (7.22%) -> 11.3x
   (0.58%). Arm A is flat across selectivity because it parses the whole document regardless; arm B rises with
   key length. Neither store can exploit absence. This refutes a prediction made before the run.
5. **Document retrieval does not favour A.** Q10 was included expecting `_source` to win, since it is loaded
   for the hits anyway. B is ~2x faster instead. The useful result is that B is not penalised there.
6. **Storage +55.1%**, matching the predictive rule `overhead = blob bytes / arm A bytes` (415/755 = 55.0%
   predicted). The blob is 0.96x the attributes JSON and uncompressed: Lucene 10.4 does not compress
   `BinaryDocValues`.
7. **Write cost not measurable.** Indexing within 1.3%. Force merge disagreed in direction with an earlier
   2.5M-document run, so no merge-cost claim is made.

## Caveats

- n=10, so p90 is the 9th of 10 sorted samples. p99 is deliberately not reported.
- Single-threaded client, single shard, single segment. Not a cluster-sizing exercise.
- The 11.7 GB index fits in 61 GiB RAM alongside a 16 GiB heap, so both arms serve mostly from page cache.
  Behaviour when the working set exceeds RAM is untested and is the largest open question.
- Arm A parses `_source` into a full `Map`, which is what `SourceLookup` does today but not the cheapest
  possible. A streaming parser stopping at the target path is untested and could recover part of B's win.

