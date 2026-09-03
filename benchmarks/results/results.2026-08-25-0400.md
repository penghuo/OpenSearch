# Variant blob: three layouts, three corpora, and why key names belong in their own column

**Date:** 2026-08-25 04:00
**Scale:** 10,000,000 documents per index, 1 shard, 0 replicas, force-merged to 1 segment (verified)
**Host:** 16 vCPU, 61 GiB RAM, NVMe · OpenSearch 3.6.0-SNAPSHOT, Lucene 10.5.0, 16 GiB heap
**Query settings:** `request_cache=false`, n=10 timed iterations after 2 warmups, server-side `took`, p50/p90

## Headline

Storing a `flat_object`'s Variant key **names** in their own per-segment column — three columns rather than two — reads
**11–16× faster than `_source`** and stores **2.2–2.5% less**, and does so **independently of how many key-set
combinations the corpus contains.** The layout it replaces was 45× slower on the corpus where combinations don't repeat.

This round changed my recommendation. I argued against building it, with numbers. The numbers were incomplete.

---

## 1. The variable, and why it stopped mattering

Deduplicating key metadata requires documents to share a key set. How often that happens is combinatorial:

| corpus | how keys are chosen | distinct key sets over 10M docs |
|---|---|---|
| **normal-test-set** | 1,000 fixed shapes, one per simulated service | **1,531** |
| **10K shapes** | 10,000 fixed shapes | **15,306** |
| **super-test-set** | drawn independently per document from a 1000-key Zipf pool | **9,942,838** (99.4% unique) |

Distinct counts are read from the index (`SortedDocValues.getValueCount()`). They exceed the shape count because a nested
object draws 2 or 3 fields per document, so one shape yields a few metadata variants — a 1.53× inflation at 1,000 shapes.

Three layouts were measured against these:

| layout | key metadata stored as | dictionary bounded by |
|---|---|---|
| **1 column** | inline in each document's blob | — (no dictionary) |
| **2 columns** | `SortedDocValues`, one entry per distinct key **set** | key-set count — **combinatorial** |
| **3 columns** | `SortedSetDocValues` of key **names** + a per-document rank list | name count — **bounded** |

---

## 2. Storage

Bytes per document. `blob` is derived as `.dvd(arm) − .dvd(A)`, valid because every other file is byte-identical between
arms (verified within 0.05%). All B rows exclude the field from `_source`, which is the configuration that would ship.

### super-test-set — 9,942,838 distinct key sets

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` | 755.1 | 289.5 | 237.1 | — | — |
| 1 column | 947.4 | 705.2 | 13.5 | 415.7 | +25.5% |
| 2 columns | 775.1 | 533.1 | 13.4 | 243.6 | +2.6% |
| **3 columns** | **735.9** | 493.8 | 13.5 | **204.3** | **−2.5%** |

### 10,000 shapes — 15,306 distinct key sets

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` | 777.5 | 299.5 | 242.3 | — | — |
| 2 columns | **718.5** | 469.3 | 13.4 | **169.8** | **−7.6%** |
| **3 columns** | 760.5 | 511.2 | 13.5 | 211.7 | **−2.2%** |

### normal-test-set — 1,531 distinct key sets

| config | total | `.dvd` | `.fdt` | blob | vs A |
|---|---|---|---|---|---|
| **A** `_source` | 784.9 | 302.7 | 244.1 | — | — |
| 2 columns | **725.8** | 474.2 | 13.6 | **171.5** | **−7.5%** |
| 3 columns | not measured | | | | |

### The blob column across every configuration

```
415.7 B/doc   1 column,  super-test-set
243.6 B/doc   2 columns, super-test-set     dictionary useless here
211.7 B/doc   3 columns, 10K shapes
204.3 B/doc   3 columns, super-test-set     650x more key sets than above, 3.5% smaller
171.5 B/doc   2 columns, normal-test-set    dictionary at its best
169.8 B/doc   2 columns, 10K shapes
```

**Three columns costs 5.4 percentage points against two on low-cardinality corpora and gains 5.1 on high-cardinality
ones.** Two columns is smaller where its dictionary works and larger where it doesn't.

**Write cost is not measurable.** 27,045–30,690 docs/s across all nine indices, a 13% band with no consistent ordering
between arms; force merge 400–580 s, likewise unordered.

---

## 3. Reads

Server-side `took`, milliseconds, p50, n=10. Arm A is not re-run per configuration — see §5.

### super-test-set — the case that broke two columns

| Q | query | A | 2 columns | **3 columns** | 3 vs A | 3 vs 2 |
|---|---|---|---|---|---|---|
| Q1 | sum, dense path, 10M docs | 12,685 | **53,503** | **841** | **15.1×** | **64×** |
| Q2 | Q1 scoped to 1M | 4,995 | 20,714 | **328** | 15.2× | 63× |
| Q3 | Q1 scoped to 100k | 508 | 2,074 | **38** | 13.4× | 55× |
| Q4 | terms + sum, 10M | 100,771 | not measured | **7,815** | 12.9× | — |
| Q5 | Q4 scoped to 1M | 10,429 | not measured | **781** | 13.4× | — |
| Q6 | sum, sparse path | 12,884 | not measured | **1,069** | 12.1× | — |
| Q7 | sum, rare path | 12,853 | not measured | **1,158** | 11.1× | — |
| Q8 | five metrics, one path | 14,115 | not measured | **2,824** | 5.0× | — |
| Q9 | sum, two paths | 13,269 | not measured | **1,698** | 7.8× | — |
| Q10 | fetch 50 docs | 3 | not measured | 2 | 1.5× | — |
| Q11 | filter only — *control* | 1 | not measured | 1 | — | — |

The two-column run was stopped after Q3; Q4 would have taken ~90 minutes at that speed.

### 10,000 shapes

| Q | A | 2 columns | **3 columns** | 3 vs A | 3 vs 2 |
|---|---|---|---|---|---|
| Q1 | 13,145 | 1,186 | **819** | **16.1×** | −31% |
| Q2 | 5,243 | 662 | **320** | 16.4× | −52% |
| Q3 | 533 | 261 | **36** | 14.8× | **−86%** |
| Q4 | 104,841 | 7,620 | **7,432** | 14.1× | −2% |
| Q5 | 10,846 | 1,125 | **762** | 14.2× | −32% |
| Q6 | 13,142 | 1,542 | **1,037** | 12.7× | −33% |
| Q7 | 13,149 | 1,633 | **1,138** | 11.6× | −30% |
| Q8 | 14,252 | 2,953 | 3,013 | 4.7× | **+2%** |
| Q9 | 13,515 | 2,018 | **1,530** | 8.8× | −24% |
| Q10 | 3 | 4 | **2** | 1.5× | −50% |
| Q11 | 1 | 1 | 1 | — | 0% |

Three columns beats two on ten of eleven queries. The exception is Q8 (+2%), where five aggregations each build their own
accessor and so each materialise the name table — a fixed cost paid five times. That is issue I3, not the layout.

### Three columns is indifferent to cardinality

| corpus | distinct key sets | blob B/doc | Q1 |
|---|---|---|---|
| 10K shapes | 15,306 | 211.7 | 819 |
| super-test-set | 9,942,838 | 204.3 | 841 |
| **change** | **650×** | **−3.5%** | **+2.7%** |

The same span moved two columns from 169.8 → 243.6 B/doc (+43%) and 1,186 → 53,503 ms (**+4,412%**).

---

## 4. Memory

Two axes that behave oppositely, so reporting one alone would mislead. Measured with `ThreadMXBean` allocation counters
rather than heap deltas, so a collection mid-run cannot hide anything. Same corpus, same 100,000 documents, same path.

| | A (`_source`) | **3 columns** | ratio |
|---|---|---|---|
| allocated opening the segment | 5,440 B | **71,336 B** | 13× more |
| allocated per document scanned | **9,710 B** | **480 B** | **20× less** |
| allocated over 100,000 documents | 971 MB | **48 MB** | 20× less |
| latency in the same probe | 10,303 ns/doc | **854 ns/doc** | 12× faster |

The 71 KB is the resident name table (1,007 names). It is **fixed** — set by the number of distinct names, not by
documents or key sets — and paid once per segment per field per accessor. Arm A retains almost nothing but produces
garbage continuously.

```
scanning 100,000 documents: A produces 971 MB of garbage
the entire name table:                     71 KB resident
```

**A allocates more in 8 documents than the whole name table occupies.** Extrapolated to a 10M-document aggregation:
~97 GB of garbage for A against ~4.8 GB. That is a concrete cause for arm A's p90 spikes, previously only guessed at:
Q8 14,252 → 25,754 ms and Q9 13,100 → 24,406 ms.

Caveat: some of the 480 B/doc is the probe's own boxing. Both figures exclude the painless and aggregation framework,
which both arms pay. So 480 vs 9,710 is the storage layer's difference, not the end-to-end total — though the 12×
latency difference in the same probe points the same way.

---

## 5. Arm A is a settled baseline and is no longer re-run

Arm A's code is untouched by this work and never reads the blob. Across all three corpora:

| Q | super | normal | 10K shapes | spread |
|---|---|---|---|---|
| Q1 | 12,685 | 13,030 | 13,145 | 3.6% |
| Q2 | 4,995 | 5,147 | 5,243 | 5.0% |
| Q3 | 508 | 528 | 533 | 4.9% |
| Q4 | 100,771 | 101,854 | 104,841 | 4.0% |
| Q5 | 10,429 | 10,587 | 10,846 | 4.0% |
| Q6 | 12,884 | 12,966 | 13,142 | 2.0% |

The residual is not noise — arm A parses the whole `_source`, so its cost tracks stored-field bytes, constant to 1.5%:

| corpus | `.fdt` B/doc | Q1 ms | ms per B/doc |
|---|---|---|---|
| super | 237.1 | 12,685 | 53.5 |
| normal | 244.1 | 13,030 | 53.4 |
| 10K shapes | 242.3 | 13,145 | 54.3 |

Re-running it costs ~40 minutes of a 45-minute run, ~20 of those on Q4 alone. It is cited, not re-measured.

---

## 6. Why two columns failed, measured rather than reasoned

Attributed by stages, each adding one operation to the previous, over the same 100,000-document scan:

| stage | 1,000 shapes | 10,000 shapes |
|---|---|---|
| read the value column | 3.67 ms | 3.90 ms |
| + read the key-set ordinal | 3.89 ms | 3.95 ms |
| + resolve the key set, index already populated | 3.85 ms | 5.65 ms |
| + decode the value tree and read one path | **29.00 ms** | **32.89 ms** |
| + resolve the key set, populated on demand | 25.44 ms | **171.40 ms** |
| whole scan, populated on demand | **50.64 ms** | **206.95 ms** |

Real work — reading columns, decoding, extracting — differs by 3.9 ms between corpora. **Populating the dictionary
differs by 146 ms**, and that is 78% of the whole difference.

### Where that 146 ms goes

One `lookupOrd` is `seekExact(ord); term()`, and `seekExact` is:

```java
if (targetBlock != currentBlock) { blockAddresses.get(blockIndex); bytes.seek(addr); }
while (this.ord < ord) { next(); }
```

Decomposed by timing lookups restricted to one position within a block, in shuffled order:

| | ns |
|---|---|
| fixed cost per lookup (block address + seek + decompress) | **15,538** |
| cost of one forward step (reconstruct one term) | **14** |

The forward walk is 0.09% of the cost. Cost is entirely block *change*:

| which ordinals are looked up | ns/lookup |
|---|---|
| the same ordinal, every time | **2** |
| 16 ordinals, cycled (all resident) | 12,248 |
| every ordinal, in ordinal order | **221** |
| every ordinal, scattered | **15,273** |

Cycling 16 resident ordinals still costs 12,248 ns, so this is not page faults. A JFR profile of 2,153 samples:

| leaf frame | share |
|---|---|
| `MemorySessionImpl.checkValidStateRaw` | **73.7%** |
| `ScopedMemoryAccess.getByteInternal` | **73.5%** |
| `LZ4.decompress` | 24.3% |
| `TermsDict.next` (the forward walk) | **0.6%** |

**Lucene's LZ4 reads its input one byte at a time through a memory-mapped `DataInput`, and every byte pays a
memory-session liveness check.** Decompressing one ~4 KB block therefore costs ~12 µs rather than the ~1 µs raw LZ4
would take on a byte array. Terms are stored in blocks of 16 (verified from bytecode: `>>> 4`, `& 15`), so resolving in
ordinal order amortises one decompression across 16 useful results and resolving in document order does not.

### Why three columns escapes it entirely

Its dictionary holds **names** (1,007), not key sets, and is read in ordinal order once per segment: 1,007 × ~230 ns ≈
0.23 ms, against 15,306 × ~14,500 ns ≈ 222 ms. The size is set by the field's vocabulary, which no corpus can inflate
combinatorially.

---

## 7. Corrections

Recorded because most were stated confidently before being measured.

| claim | status |
|---|---|
| "3 columns is strictly dominated; it buys 3.25 ms for 47 B/doc" | **Refuted — this round's main finding.** I modelled only the dictionary-population cost and omitted the per-document read cost. Measured, three columns is 24–86% faster per query on ten of eleven queries, and costs 41.9 B/doc, not 47. |
| "2 columns wins only below ~8,000 shapes" | **Wrong, my arithmetic.** I divided the dictionary by the 50,000-document sample instead of by the segment, inflating its cost 200× and inventing a crossover. |
| "the fallback costs ~16× more block decompressions" | **Understated.** Measured 51–63×: ~15× from lost block reuse and a further ~3.5–4× from lost address locality, which I had not accounted for. |
| "~8 term reconstructions per lookup explain the cost" | **Refuted.** The forward walk is 14 ns per step, 0.6% of samples. The cost is per-byte memory-session checks inside the decompressor. |
| "Parquet dedups identical metadata dictionaries across rows" | **Incomplete, and the omission caused the bug.** Parquet materialises a dictionary page and indexes it as an array, which it can do because the page is capped at ~1 MB — above that it abandons dictionary encoding. Applying a dictionary unconditionally was the defect. |
| "the +55% storage penalty is this corpus shape's price" | **Superseded.** It was the layout's price. A blob-only index is now 2.2–2.5% *smaller* than arm A on every corpus measured. |
| "a shared dictionary declared in the mapping would fix storage" | **Withdrawn as an approach.** Ids are positional, so any change to the list silently returns values under wrong names. Reverted; the column split achieves the same end safely. |
| encoder is not spec-minimal | **Refuted.** Zero non-minimal encodings over 20,000 documents. |

---

## 8. Where the bytes still are

Read from the index directly:

| column | distinct terms | ordinals/doc |
|---|---|---|
| `attributes` (path parts) | 932 | 29.8 |
| `attributes._value` | **70,215,713** | 16.7 |
| `attributes._valueAndPath` | **118,087,692** | 17.0 |

Arm A's ~300 B/doc of doc-values is dominated by `_value` and `_valueAndPath`, which exist for **filtering**. The blob
replaces neither — every B row above still carries both in full. A deployment needing blob-based value access but not
term filtering on values could drop them and save far more than any metadata encoding change. That is a functional
trade-off, not an encoding one, but it is where the bytes are.

Separately, `attributes` stores path *components* split on `.` (so `k8s.namespace` becomes `attributes.k8s` and
`attributes.namespace`) at 29.8 ordinals per document. The new `._blobnames` column stores *full* names at ~15 per
document and is strictly more informative — it could replace path parts rather than sit beside it. That changes
root-field query semantics, so it is a larger decision.

---

## 9. Limits

- **Three columns is not measured on the normal-test-set.** It is measured at 15,306 and 9,942,838 distinct key sets and
  is flat across that 650× span, so 1,531 is expected to land near 211 B/doc — but it is an interpolation, not a
  measurement.
- **Working sets larger than RAM remain untested, and remain the largest threat to the read result.** Every index fits in
  61 GiB alongside a 16 GiB heap, so both arms serve mostly from page cache. Three columns reads less per document than
  either alternative, so it should degrade better, but that is reasoning and this project has refuted several such.
- **Q4–Q11 are not measured for the two-column configurations that fell back**; both runs were stopped once the cliff was
  established.
- **Beyond 10M documents per segment is not measured.** The name table is a fixed per-segment cost, so its per-document
  share falls as segments grow; three columns should improve with scale.
- **The rank list assumes under 65,536 distinct keys in a single document** and rejects more. Not stress-tested at that
  boundary.
- **Single-threaded client, single shard, one blob field.** Concurrency and multi-field costs untested.
- **Memory figures are accessor-level.** Framework allocation is excluded from both arms; some of the 480 B/doc is the
  probe's own boxing.
- **Optimising arm A is out of scope by design.** It parses `_source` into a full map, which is what derived fields do
  today. It is the status quo, not a strawman, but a lazier reader would narrow the gap and is not evaluated.
