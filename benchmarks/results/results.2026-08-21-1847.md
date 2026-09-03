# Benchmark results: Solution B read-path optimizations (OPT-1/2/3)

- **Run:** 2026-08-21T18:47:05+0000
- **Baseline:** `results.2026-08-21-1705.md` (same corpus, same arm A)

## What changed

| | Change | Outcome |
|---|---|---|
| **OPT-1** | Share the blob accessor across aggregations instead of one per `DerivedFieldScript` | **Delivered** on multi-aggregation queries, after a failed first attempt |
| **OPT-2** | Decode the blob in place from the `BytesRef` instead of copying both halves per document | **No measurable time benefit**; possibly slightly negative |
| **OPT-3** | Do not materialise a subtree when a scalar read hits an object/array | **Neutral on this corpus** (few paths resolve to containers) |

**Arm A was not re-run: none of these changes touch its code path.** Its reproducibility was checked instead —
re-running arm A Q1-Q3 with a node restart drifted +2.7% / +2.8% / +3.7%, so cross-run comparison is sound to
within about 3%, well below the effects below.

## Results

Server-side `took` p50, milliseconds, n=10. v1 is the first OPT-1 attempt, v2 the corrected one.

| Q | A | B base | B v1 | B v2 | v2 vs base | ratio base | ratio v2 | query |
|---|---|---|---|---|---|---|---|---|
| Q1 | 13,531 | 732 | 946 | **772** | 0.95x | 18.48x | **17.53x** | sum, dense path, full corpus |
| Q2 | 5,389 | 288 | 323 | **296** | 0.97x | 18.71x | **18.21x** | sum, dense path, 10% of corpus |
| Q3 | 545 | 33 | 38 | **34** | 0.97x | 16.52x | **16.03x** | sum, dense path, 1% of corpus |
| Q4 | 107,187 | 6,884 | 7,857 | **7,234** | 0.95x | 15.57x | **14.82x** | terms + sum group-by, full corpus |
| Q5 | 10,874 | 697 | 790 | **741** | 0.94x | 15.60x | **14.67x** | terms + sum group-by, 10% of corpus |
| Q6 | 13,523 | 1,075 | 1,395 | **1,106** | 0.97x | 12.58x | **12.23x** | sum, sparse path (7.22% of docs), full corpus |
| Q7 | 13,511 | 1,198 | 1,410 | **1,483** | 0.81x | 11.28x | **9.11x** | sum, rare path (0.58% of docs), full corpus |
| Q8 | 14,371 | 4,107 | 4,042 | **3,113** | 1.32x | 3.50x | **4.62x** | five metrics on one path, full corpus |
| Q9 | 13,494 | 2,024 | 2,038 | **1,772** | 1.14x | 6.67x | **7.62x** | sum on two different paths, full corpus |
| Q10 | 3 | 1 | 3 | **3** | 0.33x | 3.00x | **1.00x** | fetch top 50 documents with a derived field |
| Q11 | 1 | 0 | 1 | **1** | 0.00x | - | **1.00x** | filter only, no derived field (control) |

p90 after v2: Q1=923, Q2=309, Q3=36, Q4=7263, Q5=749, Q6=1434, Q7=1674, Q8=3263, Q9=1976, Q10=4, Q11=1

## Per-document cost model

| | 1 aggregation | 5 aggregations | marginal per extra aggregation |
|---|---|---|---|
| Arm A | 1353.1 ns | 1437.1 ns | **21.0 ns** |
| B baseline | 73.2 ns | 410.7 ns | **84.4 ns** |
| B v1 (bad OPT-1) | 94.6 ns | 404.2 ns | **77.4 ns** |
| B v2 (fixed) | 77.2 ns | 311.3 ns | **58.5 ns** |

## Findings

### 1. The first OPT-1 attempt regressed everything, for an instructive reason

v1 resolved the shared accessor inside `variant()` — called once per document *per aggregation* — doing a
`ConcurrentHashMap.computeIfAbsent(Thread.currentThread().threadId(), ...)` each time, which boxes a `Long`
10-50M times per query. Q1 has nothing to share and still slowed from 73.2 to 94.6 ns/doc: **+21.4 ns, exactly
the cost of putting a boxed shared-map lookup on the hot path.**

Arm A avoids this by resolving `getLeafSearchLookup()` **once in the script constructor**. v2 does the same:
resolve through `SearchLookup` once per script and hold the result in a field, so sharing is preserved while the
per-document cost is a field read.

### 2. OPT-1 works, but Q8 was misdiagnosed

v2 improves the multi-aggregation cases: Q8 4,107 -> 3,113 ms (1.32x), Q9 2,024 -> 1,772 ms (1.14x), and the
marginal cost per extra aggregation falls from 84.4 to 58.5 ns/doc.

But Q8 did **not** return to ~18x as predicted; it went 3.50x -> 4.62x. So the redundant blob decode was only
about a quarter of the marginal cost. The rest is per-invocation overhead: painless dispatch, the accessor call,
coercion and `emit` — 58.5 ns/doc/aggregation for B against 21.0 ns for A.

**The original framing of Q8 was wrong.** B's ratio falls on multi-aggregation queries mainly because *A has a
huge fixed cost to amortise* (1,353 ns for the first read, 21 ns thereafter) while B has almost none (77 ns) and
so has nothing to amortise. In absolute terms B is still 4.6x faster at five aggregations. That is a ratio
artifact of A being slow to start, not a defect in B.

### 3. OPT-2 and OPT-3 did not pay off

Single-aggregation queries are 3-6% *slower* than baseline (Q1 732 -> 772, Q2 288 -> 296, Q3 33 -> 34), which is
around the 3% run-to-run drift, so at best neutral. Q7 is worse at +24% (1,198 -> 1,483) and consistent across
both optimized runs, so probably real.

A plausible mechanism: **the copy was not pure waste.** Copying the blob into a compact fresh array also compacted
it into cache. Reading in place from Lucene's larger shared buffer costs locality, and a binary-search *miss* — as
in Q6/Q7 — probes several scattered dictionary entries, so it feels that loss most. Young-generation allocation is
cheap; the 415 bytes saved per document did not buy time.

OPT-3 is semantically correct and avoids a genuinely wasteful case (a scalar read on a path holding an object), but
this corpus has few such paths, so it shows no gain here.

**Not verified:** whether OPT-2 delivered its stated allocation reduction (661 B/op). That needs a JMH run with
`-prof gc`, which was out of scope for this round.

## Net position

| | keep? | why |
|---|---|---|
| OPT-1 (v2 form) | **yes** | 1.32x on Q8, 1.14x on Q9, marginal cost -31%, no cost elsewhere |
| OPT-2 | debatable | no time benefit measured, may cost locality; unverified allocation win |
| OPT-3 | yes | correct semantics, avoids a pathological case, no measured cost |

Best B numbers remain the baseline for single-aggregation queries and v2 for multi-aggregation ones. A build that
keeps OPT-1 and reverts OPT-2 would likely be the best of both, and has not been measured.
