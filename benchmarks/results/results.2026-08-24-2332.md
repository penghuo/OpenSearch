# Variant blob: two-column layout, and the corpus property that decides everything

**Date:** 2026-08-24 23:32
**Scale:** 10,000,000 documents per index, 1 shard, 0 replicas, force-merged to 1 segment (verified)
**Host:** 16 vCPU, 61 GiB RAM, NVMe · OpenSearch 3.6.0-SNAPSHOT, Lucene 10.5.0, 16 GiB heap
**Settings:** `request_cache=false`, n=10 timed iterations after 2 warmups, server-side `took`, p50/p90

## Headline

Splitting the Variant blob into two doc-values columns — key metadata as `SortedDocValues`, value tree as
`BinaryDocValues`, which is the shape Parquet uses — makes the blob column **59% smaller** and leaves a blob-only index
**7.5% smaller than the `_source`-backed baseline**, while reading **5.5–18.5× faster**. Both axes win.

That result depends entirely on a corpus property that was not identified until this round, and which invalidates the
framing of every earlier storage measurement in this project.

---

## 1. The variable that governs the result

Deduplicating key metadata requires documents to **share a key set**. How often that happens is combinatorial, not
gradual, and it is a property of the corpus, not of the data volume.

| | how keys are chosen | possible key sets | distinct key sets over 10M docs | dedup |
|---|---|---|---|---|
| **super-test-set** | drawn independently per document from a 1000-key Zipf pool | C(1000,15) ≈ 7 × 10³² | **9,942,838** (99.4% unique) | **0%** |
| **normal-test-set** | 1,000 fixed shapes, one per simulated service | 1,000 | **1,531** | near-perfect |
| **10K shapes** | 10,000 fixed shapes | 10,000 | **15,306** | near-perfect |

Distinct counts are read directly from the index (`SortedDocValues.getValueCount()`), not estimated.

Field names (1,007) and keys per document (~15) are realistic in both. **The independent sampling is not** — real
telemetry emits a fixed key set per service. Distinct counts exceed the shape count because a nested object draws 2 or 3
fields per document, so one shape yields a few metadata variants (1,000 → 1,531 is a 1.53× inflation).

Everything measured before this round used the super-test-set only, which is why several earlier conclusions invert
below.

---

## 2. Storage

Bytes per document, 10M documents, single segment. `blob` is derived as `.dvd(arm) − .dvd(A)`, valid because every other
file is byte-identical between arms (`.fdt`, `.tim`, `.tip`, `.doc`, `.kdd` — verified within 0.05%).

### super-test-set (key sets all distinct)

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` only | 755.1 | 289.5 | 237.1 | — | — |
| **B** 1 column, `_source`+blob | 1,171.2 | 705.4 | 237.1 | 415.9 | +55.1% |
| **B′** 1 column, blob only | 947.4 | 705.2 | 13.5 | 415.7 | +25.5% |
| **B** 2 column, `_source`+blob | 998.9 | 533.3 | 237.1 | 243.8 | +32.3% |
| **B′** 2 column, blob only | 775.1 | 533.1 | 13.4 | 243.6 | +2.7% |

### normal-test-set (1,000 shapes)

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` only | 784.9 | 302.7 | 244.1 | — | — |
| **B** 2 column, `_source`+blob | 956.1 | 474.2 | 244.0 | 171.5 | +21.8% |
| **B′** 2 column, blob only | **725.8** | 474.2 | 13.6 | **171.5** | **−7.5%** |

### 10,000 shapes

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` only | 777.5 | 299.5 | 242.3 | — | — |
| **B′** 2 column, blob only | **718.5** | 469.3 | 13.4 | **169.8** | **−7.6%** |

### The blob column across every configuration measured

```
415.9 B/doc   1 column,  super-test-set
243.6 B/doc   2 column,  super-test-set        -41%
171.5 B/doc   2 column,  1,000 shapes          -59%
169.8 B/doc   2 column,  10,000 shapes         -59%
```

**Storage is insensitive to shape count once the distinct count saturates.** Ten times the shapes moved the blob by
1.7 B/doc, because the dictionary is a fixed per-segment cost: 3.95 MB over 10M documents is 0.4 B/doc.

**Write cost is not measurable.** 27,045–30,690 docs/s across all five indices, a 13% band with no consistent ordering
between arms; force merge 450–580 s, likewise unordered.

---

## 3. Reads

Server-side `took`, milliseconds, p50, n=10.

### normal-test-set (1,000 shapes)

| Q | query | A | B′ 2col | **B faster** |
|---|---|---|---|---|
| Q1 | sum, dense path, 10M docs | 13,030 | **705** | **18.5×** |
| Q2 | Q1 scoped to 1M docs | 5,147 | **305** | 16.9× |
| Q3 | Q1 scoped to 100k docs | 528 | **62** | 8.5× |
| Q4 | terms + sum, 10M docs | 101,854 | **6,892** | 14.8× |
| Q5 | Q4 scoped to 1M docs | 10,587 | **742** | 14.3× |
| Q6 | sum on a sparse path | 12,966 | **1,030** | 12.6× |
| Q7 | sum on a rare path | 13,051 | **1,089** | 12.0× |
| Q8 | five metrics on one path | 13,947 | **2,527** | 5.5× |
| Q9 | sum on two paths | 13,100 | **1,543** | 8.5× |
| Q10 | fetch 50 docs with a derived field | 3 | 3 | 1.0× |
| Q11 | filter only, no derived field — *control* | 0 | 0 | — |

### 10,000 shapes

| Q | A | B′ 2col | **B faster** |
|---|---|---|---|
| Q1 | 13,145 | **1,186** | **11.1×** |
| Q2 | 5,243 | **662** | 7.9× |
| Q3 | 533 | **261** | 2.0× |
| Q4 | 104,841 | **7,620** | 13.8× |
| Q5 | 10,846 | **1,125** | 9.6× |
| Q6 | 13,142 | **1,542** | 8.5× |
| Q7 | 13,149 | **1,633** | 8.1× |
| Q8 | 14,252 | **2,953** | 4.8× |
| Q9 | 13,515 | **2,018** | 6.7× |
| Q10 | 3 | 4 | 0.8× |
| Q11 | 1 | 1 | — |

The control (Q11) shows no difference, so the filtering path is genuinely identical and the differences above are
attributable to the value store.

---

## 4. Arm A is a settled baseline

Arm A's code is untouched by this work and never reads the blob. Measured across all three corpora:

| Q | super | normal | 10K shapes | spread |
|---|---|---|---|---|
| Q1 | 12,685 | 13,030 | 13,145 | 3.6% |
| Q2 | 4,995 | 5,147 | 5,243 | 5.0% |
| Q3 | 508 | 528 | 533 | 4.9% |
| Q4 | 100,771 | 101,854 | 104,841 | 4.0% |
| Q5 | 10,429 | 10,587 | 10,846 | 4.0% |
| Q6 | 12,884 | 12,966 | 13,142 | 2.0% |

The residual is not noise. Arm A parses the whole `_source`, so its cost tracks stored-field bytes, and the ratio is
constant to 1.5%:

| corpus | `.fdt` B/doc | Q1 ms | ms per B/doc |
|---|---|---|---|
| super | 237.1 | 12,685 | 53.5 |
| normal | 244.1 | 13,030 | 53.4 |
| 10K shapes | 242.3 | 13,145 | 54.3 |

This is an end-to-end confirmation of "A ∝ whole-document bytes", previously measured only at the accessor level.
**Arm A is not re-run in future rounds**; cite these numbers.

---

## 5. The read regression this round diagnosed and fixed

The two-column layout, as first written, was **73× slower** than the single column and **4.2× slower than arm A**. The
cause was attributed by direct measurement, not inference — each stage adds one operation to the previous:

| read step | ns/doc |
|---|---|
| value column only | 35.2 |
| + metadata ordinal | 41.0 |
| **+ `lookupOrd(ord)`** | **14,969.2** |
| + copy and cache | 15,167.0 |
| single-column equivalent (metadata read in place) | 40.0 |

`lookupOrd` is **365× everything else combined.** Lucene stores sorted terms in LZ4-compressed blocks of 16 (verified
from the bytecode: `>>> 4`, `& 15`), so one lookup costs a random seek plus a whole-block decompress to extract one term.
Ordinals are ordered by term, not by document, so consecutive documents seek to unrelated blocks: no locality, and 15/16
of each decompressed block wasted.

Arm A's comparable cost — decompressing a 16 KB stored-fields block — is amortised over ~30 documents read in docId
order, which is why arm A wins when the metadata dictionary cannot be held in memory.

**The fallback is a cliff, not a slope:**

| | distinct | dictionary | Q1 |
|---|---|---|---|
| 10K shapes | 15,306 | 3.95 MB | 48,453 ms |
| super-test-set | 9,942,838 | ~2,500 MB | 53,503 ms |

A 600× smaller dictionary buys 9%. Cost is dominated by per-lookup mechanics, so being barely over the cap is nearly as
expensive as being far over it.

**Fix:** index the segment's key sets by ordinal, filled on demand, capped at 65,536 entries.

| | 10K shapes Q1 | 1K shapes Q10 |
|---|---|---|
| eager fill, 1 MB byte cap | 48,453 | 9 |
| **lazy fill, 65,536-entry cap** | **1,186** (41× better) | **3** (67% better) |

---

## 6. What is still wrong

**Q3 costs a fixed ~25 ms more with lazy filling.** Eager filling resolved ordinals 0,1,2,… in order, which is
sequential in the term dictionary — one block decompress yields 16 entries. Lazy filling resolves them in document order,
which is random in ordinal space, so nearly every lookup decompresses its own block: ~16× more decompressions for the
same 1,531 entries.

The cost is fixed, so its visibility depends only on the denominator:

| | docs scanned | eager | lazy | Δ absolute | Δ % |
|---|---|---|---|---|---|
| Q1 | 10,000,000 | 733 | 705 | −28 ms | −4% |
| Q2 | 1,000,000 | 281 | 305 | **+24 ms** | +9% |
| Q3 | 100,000 | 35 | 62 | **+27 ms** | **+77%** |

Q2 and Q3 gained the same absolute amount. Every other query moved within the ±11% run-to-run band, with mixed signs
(Q8 −6%, Q9 −6%), so **only Q3 is a real regression**.

Fix, not yet built: after the first N lazy misses, switch to a sequential full fill. Small queries keep paying only for
what they touch; scans recover the cheap sequential pass.

---

## 7. Corrections to earlier conclusions

Recorded because several were stated confidently before being measured.

| claim | status |
|---|---|
| "The blob costs ~1.00× the JSON text; Variant's savings and overheads cancel" | **Superseded.** True for the single column. The two-column layout on a realistic corpus is 0.40× the JSON. |
| "+55% storage is this corpus shape's price, not a regression" | **Superseded.** It was the *layout's* price. A blob-only index is now 7.5% *smaller* than arm A. |
| "A shared dictionary in the mapping would make B′ storage-positive" | **Withdrawn as an approach.** Storing the key list in the mapping is unsafe — ids are positional, so any change silently returns values under wrong names. Reverted. The column split achieves the same end without it. |
| "Parquet dedups identical metadata dictionaries across rows" | **Incomplete, and the omission mattered.** Parquet reads a dictionary page into memory and indexes it as an array, which it can do *because the page has a ~1 MB cap — above it Parquet abandons dictionary encoding entirely.* Applying the dictionary unconditionally was the defect. |
| "3-column (name-level dictionary) is the robust answer" | **Refuted by measurement.** ~51 B/doc metadata always, against 2-column's ~3.4 B/doc when the dictionary saturates — 15× worse. It only wins on the super-test-set. |
| "2-column wins only below ~8,000 shapes" | **Wrong, my own arithmetic error.** I divided the dictionary by the 50,000-document sample instead of the segment. Corrected: 2-column wins by 15–33× at every bounded shape count, and by more as segments grow. |
| Encoder is not spec-minimal | **Refuted.** Zero non-minimal encodings over 20,000 documents: no over-wide integers, no long-form strings under 64 bytes, no over-wide field-id or offset widths, no unnecessary 4-byte counts. |

---

## 8. Where the bytes are — a larger lever than anything above

Read from the index directly:

| column | distinct terms | ords/doc |
|---|---|---|
| `attributes` (path parts) | 932 | 29.8 |
| `attributes._value` | **70,215,713** | 16.7 |
| `attributes._valueAndPath` | **118,087,692** | 17.0 |

Arm A's ~300 B/doc of doc-values is dominated by `_value` and `_valueAndPath`, which exist for *filtering*. **The blob
replaces neither** — B′'s 725.8 B/doc still carries both in full. A deployment that needs blob-based value access but not
term filtering on values could drop them and save far more than any metadata encoding change. That is a functional
trade-off, not an encoding one, but it is where the bytes actually are.

Separately: `attributes` stores path *components* split on `.` (so `k8s.namespace` becomes `attributes.k8s` and
`attributes.namespace`), at 29.8 ordinals per document. A column of *full* key names would cost roughly half that
(~15 ordinals) and carry strictly more information — it could replace path parts rather than add to it. That change
alters root-field query semantics, so it is a larger decision.

---

## 9. Limits

- **Not measured beyond 10M documents per segment.** The dictionary is a fixed per-segment cost, so its per-document
  share falls as segments grow — the two-column result should improve with scale, but this is untested.
- **Working sets larger than RAM.** All indices fit in 61 GiB alongside a 16 GiB heap, so both arms serve mostly from
  page cache. This remains the largest untested threat to the read result.
- **Q4–Q11 not measured for the two-column eager configurations.** Both runs were stopped after Q3 once the cliff was
  established; Q4 alone would have taken ~90 minutes at that speed.
- **The 65,536-entry cap is not stress-tested at its boundary.** It is known to work at 15,306 entries and to reject
  9.9M. Behaviour between those, and the memory actually resident under lazy filling, are not measured.
- **Single-threaded client, single shard, one field with a blob.** Concurrency and multi-field costs untested.
- **Nested-object variation inflates distinct counts by ~1.5×** on this corpus. A workload with more optional fields
  would inflate further — five optional attributes per service would multiply shapes by up to 32 — and could exceed the
  cap where this corpus does not. Untested.
- **Optimising arm A is out of scope by design.** Arm A parses `_source` into a full map, which is what derived fields do
  today. It is the status quo, not a strawman, but a lazier `_source` reader would narrow the gap and is not evaluated.
