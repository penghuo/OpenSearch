# Phase 1 as built: the three read routes measured against each other

Measured 2026-09-03 after implementing the phase 1 columnar read path. One question: the design claimed the script route
would get *faster* than `_source`, and estimated it without measuring. This settles that, and records a bug the
measurement itself exposed.

## 1. Setup

All three routes on **one index**, so nothing differs but the route.

```
index      dvtest-b -- flat_object with the blob columns AND _source enabled
documents  100,000 from the SVC_10K generator: ~100 attributes per document, 10,000 services
segments   1 (force-merged), 672.7 MB
node       OpenSearch 3.6.0-SNAPSHOT built from this branch, 16 GiB heap, 16 vCPU / 61 GiB / NVMe
query      sum over `status`, present in every document
method     request_cache=false, p50 of 3 timed iterations after 1 warmup, server-side `took`
```

All three returned **31,271,924** — the same answer by three different paths, which is what makes the latencies
comparable.

## 2. Results

| route | how it reads | p50 | vs `_source` |
|---|---|---|---|
| **native column** | `{"sum": {"field": "attributes.status"}}`, no script | **42 ms** | **256×** |
| **lazy map** | `doc['attributes'].value['status']` in a derived field | **86 ms** | **125×** |
| `_source` | `params._source.attributes['status']` in a derived field | 10,767 ms | 1× |

Iterations were stable: native 42 / 43 / 41, lazy map 93 / 86 / 62, `_source` 10,767 / 11,021 / 11,158.

**The design's §5 estimate was 2–5 s per million documents for the script route.** Measured, it is ~860 ms per million —
conservative by three to six times, in the safe direction. So the claim that "no user gets slower" holds, and by a wide
margin rather than narrowly.

The tier structure is what the design predicted, at a smaller ratio than feared: native is 2× the lazy map, not an order
of magnitude. The lazy view only reads the one key asked for, so the gap is the derived-field and painless overhead plus
one `lookupTerm` per distinct key per segment, not object reconstruction.

## 3. What the measurement exposed

Running it found a bug that seven parallel code readers and 1,400 unit tests had not.

On `svc10k-a` — 1,000,000 documents, built by the prototype **without** the column — the native aggregation returned
`0.0` rather than failing. The version gate had passed it: the index was created at 3.6.0, so
`indexCreatedVersion.onOrAfter(V_3_6_0)` was true, but that build wrote the column only when a mapping parameter asked
for it. Lucene answers an absent doc-values field with an empty iterator rather than an error, so the aggregation was
confidently wrong over a million documents.

That is precisely the failure the design's §6 exists to prevent, and the index-creation version turned out to be only a
*proxy* for "the column exists".

The fix separates the two cases that a missing column can mean:

| in this segment | means | answer |
|---|---|---|
| no column, and no terms for the field either | no document here has the field | empty, which is correct |
| no column, but the field's terms are present | the documents have the field and the column is missing | **refuse** |

Verified on the same 1M-document index, which now returns:

```
illegal_state_exception: [attributes] has documents in this segment but no [attributes._blob]
column to read them from, so a value cannot be returned. Reindex the field.
```

Pinned by `FlatObjectColumnarReadTests.testASegmentWithDocumentsButNoColumnIsRefused`, which builds the index with a
mapper that writes no columns and reads it with one that expects them — the shape of a prototype-era index opened by a
current node.

## 4. Also confirmed on the way

- **The retired mapping parameter works.** `svc10k-variant-enc`, whose stored mapping still says `variant_blob: true`
  and whose derived fields still call the deleted `variant()` function, opens **green**. Had the parameter been deleted
  rather than accepted-and-ignored, the shard would have failed allocation and the index could never have been repaired,
  since a mapping cannot be edited on an index that will not open.
- **`sortRequiresCustomComparator()` is load-bearing, by mutation.** Flipping it to `false` and re-running turns the
  multi-segment sort assertion from `[100, 200, 300, 400, 500]` into `[Infinity × 5]` — every document sorted as missing.
  The test has teeth; without that check it would have been a test that passes either way.

## 5. Limits

- One corpus, one document width (~100 attributes), 100k documents. The `_source` route's cost scales with document
  width and the columnar routes' does not, so the ratio is a function of the corpus and this one is attribute-heavy.
- Single shard, single node, no concurrency. The banked numbers from 2026-08-26 are not directly comparable: different
  scale, and that session established the absolute figures do not reproduce exactly across runs even when the ratios do.
- The multi-path case (§2.3 of the design) is still unmeasured. Reading two paths opens two leaves, each with its own
  blob cursor, so it should cost about twice — but that is reasoning, not a measurement.
