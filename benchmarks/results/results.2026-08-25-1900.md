# Removing the Variant blob's rank column: two columns instead of three

**Date:** 2026-08-25 19:00
**Scale:** 10,000,000 documents per index, 1 shard, 0 replicas, force-merged to 1 segment (verified)
**Host:** 16 vCPU, 61 GiB RAM, NVMe · OpenSearch 3.6.0-SNAPSHOT, Lucene 10.5.0, 16 GiB heap
**Query settings:** `request_cache=false`, n=10 timed iterations after 2 warmups, server-side `took`, p50/p90
**Arms:** `col3` (three columns) and `col3a` (two columns) built from **one binary**, so no rebuild sits between them.
Arm A is cited from [results.2026-08-25-0400.md](./results.2026-08-25-0400.md) and deliberately not re-run.

## Headline

Ordering the Variant's field ids by name lets a reader resolve a name from the name column's ordinals directly, which makes
the per-document rank column say nothing a reader does not already know. Removing it:

| | col3 | **col3a** |
|---|---|---|
| doc-values columns for the blob | 3 | **2** |
| blob bytes/doc (10K shapes) | 211.7 | **193.1** (−8.8%) |
| total vs `_source` (10K shapes) | −2.2% | **−4.6%** |
| total vs `_source` (super-test-set) | −2.6% | **−4.9%** |
| query sum, 11 queries (10K shapes) | 16,391 ms | **15,005 ms** (−8.5%) |
| query sum, 11 queries (super-test-set) | 16,717 ms | **14,547 ms** (−13.0%) |
| ingestion throughput | \-\- not distinguishable \-\- | |

**It roughly doubles the storage advantage over `_source` and is faster on all 18 query measurements the timer can
resolve.** It is the first change in this project that improved both axes at once; every earlier one traded them.

---

## 1. What changed

The blob stores key names in a `SortedSetDocValues` column that Lucene deduplicates across the segment, and hands a reader
each document's names as an ascending ordinal list — which is name order. The value tree refers to names by field id.

The two did not line up. The encoder numbers keys **as it meets them**, so field id order is insertion order, while the
ordinal list is in name order. The rank column was the permutation between them:

```
col3    field id --[rank column]--> position in ordinal list --> ordinal --> name
col3a   field id ---------------------------------------------> ordinal --> name
```

Numbering the keys by name instead makes that permutation the identity: field id *i* becomes the document's *i*-th smallest
name, which is exactly what the reader's *i*-th ordinal already is.

The encoder cannot number them that way as it goes, because a smaller name may still arrive. So the ids are permuted after
the fact, in place, at the width already written. That is safe while every permuted id still fits: with at most **256**
distinct keys in a document every id fits one byte, so every object's field-id width is one byte both before and after any
permutation. A document with more keys keeps its insertion-order ids and gets an explicit rank list — the layout every
document used before — which is also why a reader accepts both forms in one segment.

Relabelling additionally leaves each object's field ids numerically ascending. The format only requires them ordered by key
*string*, but the reader binary-searches by comparing resolved names, and with a name column those names arrive in field-id
order, so the two orderings have to be the same one.

**The column is gone, not smaller.** Read from the two 10M-document segments' `.fnm`:

```
col3    attributes  attributes._blob  attributes._blobmeta  attributes._blobnames  attributes._value  attributes._valueAndPath
col3a   attributes  attributes._blob                        attributes._blobnames  attributes._value  attributes._valueAndPath
```

---

## 2. Storage

Bytes per document. `blob` is derived as `.dvd(arm) − .dvd(A)`.

### 10,000 shapes — 15,306 distinct key sets

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` (cited) | 777.5 | 299.5 | 242.3 | — | — |
| col3 | 760.76 | 511.22 | 13.64 | 211.72 | −2.15% |
| **col3a** | **741.75** | 492.61 | 13.43 | **193.11** | **−4.60%** |

### super-test-set — 9,942,838 distinct key sets

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` (cited) | 755.1 | 289.5 | 237.1 | — | — |
| col3 | 735.87 | 493.93 | 13.43 | 204.43 | −2.55% |
| **col3a** | **718.21** | 475.80 | 13.66 | **186.30** | **−4.89%** |

### The saving is the same three times over

| corpus | docs | col3 `.dvd` | col3a `.dvd` | saving |
|---|---|---|---|---|
| 10K shapes | 200,000 | 535.87 | 517.36 | **18.51 B/doc** |
| 10K shapes | 10,000,000 | 511.22 | 492.61 | **18.61 B/doc** |
| super-test-set | 10,000,000 | 493.93 | 475.80 | **18.13 B/doc** |

Three independent measurements across a 50× change in scale and a 650× change in key-set cardinality, spread 2.6%. And it
is fully accounted for by the mechanism: a rank list is `1 + count` bytes, the corpus averages ~15.5 distinct keys per
document, so ~16.5 B/doc of payload plus roughly 2 B/doc for the column's own monotonic offset array. Nothing is left over
to explain.

### col3 reproduces its own earlier measurement

`variant_blob_rank_column` was written to recreate the three-column layout from the new binary. It does, to within noise of
the numbers taken three days earlier with the old one:

| | earlier round | this round | agreement |
|---|---|---|---|
| col3 blob, 10K shapes | 211.7 | 211.72 | 0.01% |
| col3 blob, super-test-set | 204.3 | 204.43 | 0.06% |

That is what makes the col3-vs-col3a comparison here trustworthy and lets arm A stay cited rather than re-measured.

### Where col3a sits against the layout it replaced

The two-column *key-set* layout — the one that collapsed 45× on high-cardinality corpora — stored 169.8 B/doc at 10K
shapes. col3a's 193.1 recovers **44% of the 41.9 B/doc gap** that cardinality-independence cost, while keeping that
independence: 650× more distinct key sets moves col3a's blob by 3.5% (193.11 → 186.30), the same flatness col3 had.

---

## 3. Reads

Server-side `took`, milliseconds, p50, n=10.

### 10,000 shapes

| Q | query | A (cited) | col3 | **col3a** | col3a vs col3 | col3a vs A |
|---|---|---|---|---|---|---|
| Q1 | sum, dense path, 10M | 13,145 | 827 | **770** | −6.9% | 17.1× |
| Q2 | sum, dense, 1M | 5,243 | 313 | **310** | −1.0% | 16.9× |
| Q3 | sum, dense, 100k | 533 | 36 | **35** | −2.8% | 15.2× |
| Q4 | terms + sum, 10M | 104,841 | 7,622 | **7,288** | −4.4% | 14.4× |
| Q5 | terms + sum, 1M | 10,846 | 782 | **743** | −5.0% | 14.6× |
| Q6 | sum, sparse path (6.8%) | 13,142 | 1,112 | **920** | **−17.3%** | 14.3× |
| Q7 | sum, rare path (0.23%) | 13,149 | 1,188 | **967** | **−18.6%** | 13.6× |
| Q8 | five metrics, one path | 14,252 | 2,913 | **2,548** | −12.5% | 5.6× |
| Q9 | sum, two paths | 13,515 | 1,595 | **1,423** | −10.8% | 9.5× |
| Q10 | fetch 50 docs | 3 | 2 | **1** | at the timer's floor | — |
| Q11 | filter only — *control* | 1 | 1 | 0 | at the timer's floor | — |
| | **sum** | | **16,391** | **15,005** | **−8.5%** | |

### super-test-set

| Q | A (cited) | col3 | **col3a** | col3a vs col3 | col3a vs A |
|---|---|---|---|---|---|
| Q1 | 12,685 | 833 | **758** | −9.0% | 16.7× |
| Q2 | 4,995 | 335 | **293** | −12.5% | 17.0× |
| Q3 | 508 | 38 | **34** | −10.5% | 14.9× |
| Q4 | 100,771 | 7,786 | **6,984** | −10.3% | 14.4× |
| Q5 | 10,429 | 784 | **718** | −8.4% | 14.5× |
| Q6 | 12,884 | 1,202 | **905** | **−24.7%** | 14.2× |
| Q7 | 12,853 | 1,248 | **962** | **−22.9%** | 13.4× |
| Q8 | 14,115 | 2,907 | **2,541** | −12.6% | 5.6× |
| Q9 | 13,269 | 1,582 | **1,351** | −14.6% | 9.8× |
| Q10 | 3 | 2 | **1** | at the timer's floor | — |
| Q11 | 1 | 0 | 0 | at the timer's floor | — |
| **sum** | | **16,717** | **14,547** | **−13.0%** | |

**col3a wins all 18 comparisons the timer can resolve** (Q1–Q9 on both corpora), and its p90 is better in 17 of those 18 —
most visibly where col3 was least stable: Q6 p90 1,336 → 923 and Q7 1,427 → 985 on the super-test-set. The exception is Q2
at 10K shapes, 320 → 322 ms.

Q10 and Q11 are reported for completeness but say nothing here: both arms land on 0–2 ms, which is the resolution of
server-side `took`. Reading them as −50% or −100% would be reading the timer.

---

## 4. Why it is faster, measured

Three separate probes, below the query layer, on the same two 10M-document segments.

### 4.1 The extra column costs 21.3 ns/doc

Driving the doc-values columns directly, adding one operation at a time, 200,000 documents, best of 6:

| stage | col3 | col3a |
|---|---|---|
| advance and read the value column | 38.0 | 35.1 |
| + advance the name column, drain its ordinals | 101.9 | 99.8 |
| + advance and read the rank column | **123.2** | — |

**Stage 3 − stage 2 = 21.3 ns/doc**, which is the whole cost of having the column. Stages 1 and 2 read the same columns in
both indices and agree to 2.9 and 2.1 ns/doc, which is the control: it says the two indices are comparable, so the stage-3
figure is attributable to the extra column rather than to the two segments differing.

### 4.2 The accessor-level saving is 54–80 ns/doc

Full reads through `VariantBlobValueAccessor`, one path per JVM, 2,000,000 documents, best of 6:

| path | presence | col3 | col3a | saving |
|---|---|---|---|---|
| `status` | 100% | 234.6 | 180.6 | **−54.0** (−23.0%) |
| `process.runtime.name` | 6.7% | 327.5 | 247.7 | **−79.8** (−24.4%) |
| `custom.tenant.attr_248` | 0.24% | 340.0 | 275.6 | **−64.4** (−18.9%) |

So 21.3 of the ~54–80 ns/doc is the raw column read. The remainder is the work the column forces above it: per document, a
`BytesRef` read and three validation branches; per binary-search probe, an extra unsigned read and bounds check inside name
resolution, because `ranks[fieldId] → ordinals[rank] → name` is one indirection longer than `ordinals[fieldId] → name`.

### 4.3 Sparse paths are dearer in *both* layouts, which is why Q6 and Q7 lead

The table above also explains a query result that looks backwards. Q6 and Q7 touch a value in 6.8% and 0.23% of documents
yet cost **more** than Q1, which touches one in every document — 1,202 and 1,248 ms against 833.

That is not the layout: it is the path, and both arms pay it. `custom.tenant.attr_248` costs col3 340.0 ns/doc against
`status`'s 234.6, because its name is 22 bytes rather than 6 — so every key comparison compares more bytes and every lookup
allocates a larger probe array — and because a binary search for a key that is *absent* runs to its full depth instead of
stopping at a hit.

col3a's advantage is largest there because those queries do the most name resolutions per document, which is the operation
the extra indirection sits inside.

### 4.4 Memory: same allocation, less work

`ThreadMXBean` allocation counters rather than heap deltas, 100,000 documents, path `status`, 10K-shapes corpus:

| | A (`_source`) | col3 | **col3a** |
|---|---|---|---|
| allocated opening the segment | 2,272 B | 71,336 B | **66,080 B** |
| allocated per document scanned | 5,152 B | **504 B** | **504 B** |
| latency in the same probe | 4,424 ns/doc | 437 ns/doc | **362 ns/doc** |

Per-document allocation is **identical** — col3a is not faster by allocating less, it is faster by doing less. The 5,256 B
it saves on opening is the rank column's reader, which it never constructs.

Arm A is included here because a local allocation probe is not the 45-minute benchmark that is settled; measuring it in the
same run makes the three internally consistent. Its figures differ from the earlier round's (9,710 B/doc, 10,303 ns/doc)
because that measurement used a different corpus, so the two should not be mixed.

---

## 5. Ingestion throughput: not distinguishable

Asked for, measured, and the answer is that this method cannot tell the two apart.

**10M-document runs disagree on the sign:**

| corpus | col3 | col3a | col3a vs col3 |
|---|---|---|---|
| 10K shapes | 31,995 docs/s | 27,943 docs/s | **−12.7%** |
| super-test-set | 28,317 docs/s | 32,859 docs/s | **+16.0%** |

**Three repetitions each at 2M documents, arms alternating** so host drift cannot be read as an arm difference:

| arm | runs (docs/s) | median | spread |
|---|---|---|---|
| col3 | 25,148 · 26,795 · 23,995 | 25,148 | 11.7% |
| col3a | 20,381 · 24,312 · 24,327 | 24,312 | 19.4% |

**Within-arm spread (11.7%, 19.4%) is several times the between-arm median difference (3.3%)**, and the 10M runs point
opposite ways. There is no effect here that this harness can resolve. That is a plausible outcome on the mechanism —
col3a stops building and adding a `BinaryDocValuesField` and instead walks the value tree once to permute ~15 field ids,
which are comparable amounts of work — but the measurement does not establish it either way, only that neither dominates.

Force-merge time is not reported: the harness times it with a 10-second polling loop, so its resolution is ±10 s and every
run landed in the same bucket.

---

## 6. Corrections

Both are mine, from this round.

| claim | status |
|---|---|
| "col3a is faster because name resolution costs less per binary-search probe, so queries whose searches run to full depth gain most" | **Refuted as stated, then partly recovered.** Probing two keys absent from every document showed the saving *flat* at 41–44 ns/doc rather than scaling with probe count, which kills the clean version of the claim. Measuring the real sparse and rare paths (§4.2) then showed a saving that does vary with path, 54 → 80 ns/doc. So there is a per-probe component, but the larger part is per-document, and the reason Q6/Q7 lead is mostly that those paths are dearer in **both** arms (§4.3) rather than that the layout delta is bigger there. I asserted the mechanism before measuring it, which is the same error as last round. |
| "single-threaded client, single shard, so per-document figures across probes and queries are comparable" — carried from the previous report | **Wrong about the search.** The client is single-threaded and there is one shard, but `search.concurrent_segment_search.mode` defaults to `auto` with `partition_strategy=balanced` and `max_slice_count=4`, and the segment is far over `partition_min_segment_size`, so a query is partitioned *within* the single segment. Measured directly: Q1 on col3a takes **769 ms** as benchmarked and **2,687–2,717 ms** with `mode=none`, a 3.5× difference. Both arms get it equally, so every A/B conclusion stands — but wall-clock per-document figures are ~3.5 slices wide and must not be compared with single-threaded probe numbers, which is exactly what I was about to do. |

Two figures also do not reconcile, and are recorded rather than smoothed over. Scaled for the 3.5× partitioning, the
end-to-end saving matches the accessor probe on the sparse and rare paths (Q6 ≈ 67, Q7 ≈ 77 CPU ns/doc against the probe's
79.8 and 64.4) but not on the dense path (Q1 ≈ 20 against the probe's 54.0). Q1 is the query that emits a value for every
document, so it does the most work outside the accessor; that would dilute the *ratio* but not the absolute difference, so
it is not an explanation. Unresolved.

---

## 7. Limits

- **The 256-key threshold is tested but not stressed.** Unit tests cover 255, 256, 257 and 600 keys and a segment mixing
  both forms, in both directions. No 10M-document corpus exercises the fallback: the measured corpora average ~15.5
  distinct keys per document, so **every document in every index measured here took the relabelled path.** The rank
  column's cost is therefore measured, and the fallback's is not.
- **The normal-test-set (1,531 key sets) is not measured for either arm.** col3a is flat across a 650× cardinality span,
  so ~193 B/doc is expected there too, but that is interpolation.
- **Working sets larger than RAM remain untested**, and remain the largest threat to every read result in this project.
  col3a reads one fewer column per document, so it should degrade better, but that is reasoning.
- **Ingestion throughput is bounded, not measured** (§5). A quieter harness — no force merge in the loop, a single indexing
  thread, more repetitions — could resolve a few percent. This one cannot.
- **Concurrent segment search was on throughout** (§6) and its slice count is not pinned, so a run-to-run change in
  partitioning is inside the query noise.
- **`.fdt` differs by up to 1.7% between arms** (13.43 vs 13.66 B/doc, and reversed between corpora) although both arms
  exclude the field from `_source` and should write identical stored fields. Eight concurrent indexing threads make
  document→docId assignment nondeterministic, which moves compression-block boundaries. It does not affect the blob
  derivation: the non-blob part of `.dvd` reproduces across rounds to 0.06%.
- **`variant_blob_rank_column` is benchmark scaffolding** and should be removed before this merges — subject to
  [I11](../../docs/flat_object/issues.md), which is that removing a blob mapping parameter strands every index that used
  it.
