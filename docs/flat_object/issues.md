# Open issues — `flat_object` Variant blob column

## Corpus shapes: normal-test-set vs super-test-set

Every result below depends on one variable that was not identified until 2026-08-24: **how many distinct key-set
combinations the corpus contains.** Not how many distinct field names — how many distinct *sets* of them.

| preset | shapes | distinct key sets over 10M docs | dedup effectiveness |
|---|---|---|---|
| `E10M_NORMAL` — **normal-test-set** | 1,000 (one per service, fixed key set) | **1,531** | near-perfect |
| `SHAPES_10K` | 10,000 | **15,306** | near-perfect |
| `E10M` — **super-test-set** | none; keys drawn per document | **9,942,838** (99.4% unique) | **0%** |

The distinction is combinatorial, not a matter of degree. With keys drawn independently per document from a 1000-key Zipf
pool, the number of *possible* key sets is C(1000,15) ≈ 6.9 × 10³² against 10⁷ documents, so two documents never collide.
Real telemetry is the opposite: a given service emits a fixed key set, so a corpus of 1000 services has ~1000 shapes and
each recurs ~6,500 times. Distinct counts exceed the shape count because a nested object draws 2 or 3 fields per document,
so one shape yields a few metadata variants.

**All three are now measured** ([results.2026-08-24-2332.md](../../benchmarks/results/results.2026-08-24-2332.md)), and
the conclusions do invert: the same layout is +2.7% storage and 4× slower than solution A on the super-test-set, and −7.5%
storage with up to 18.5× faster reads on the normal-test-set. Field names (1,007) and keys per document (~15) are
realistic in all three; only the independent sampling is not.

**Anything measured before 2026-08-24 used the super-test-set only** and should be read as describing that extreme, not
the expected case. The generator now supports both, and shape count is a benchmark dimension
(`CorpusConfig.shapeSweep()`).

Tracks what is known-broken, known-suboptimal, and deliberately deferred. Every claim here is backed by a measurement in
[benchmarks/results](../../benchmarks/results); where something is a projection rather than a measurement it says so.

Status vocabulary: **open** (real, unaddressed) · **deferred** (deliberately not doing yet) · **closed-negative**
(investigated, answer was "don't") · **blocked** (needs a decision).

---

## I1 — The blob re-stores the key dictionary in every document · RESOLVED 2026-08-24

Resolved by the two-column split (I9), not by any of the approaches considered below. Key metadata now lives in its own
deduplicated column, taking the blob from 415.9 to 171.5 B/doc on a realistic corpus. The analysis below is kept because
it identified the cost correctly; only its proposed remedies were wrong.

<details>
<summary>Original analysis</summary>

### I1 (original) — The blob re-stores the key dictionary in every document

**60.5% of the blob is a per-document key dictionary**, and each distinct key name is stored ~308 times across a
20,000-document sample.

| | B/doc | share of blob |
|---|---|---|
| metadata (key dictionary) | 249.7 | **60.5%** |
| — raw key name bytes | 223.1 | 54.0% |
| value tree (the actual data) | 159.3 | 38.6% |

This is the single largest cost in the design and the reason the blob column costs +55.1% (or B′ +25.5%) in storage. The encoder
itself is not at fault: `VariantEncodingAuditTests` asserts across 20,000 documents that every value uses the narrowest
form the Parquet spec offers — zero over-wide integers, zero long-form strings under 64 bytes, zero over-wide field-id or
offset widths.

Measured alternatives (`benchmarks/results/results.2026-08-21-2200.md`):

| Layout | B/doc | vs JSON |
|---|---|---|
| as implemented | 413.0 | −3.0% |
| value column only, per-doc dictionary | 159.3 | −62.6% |
| value column, segment-shared dictionary | 175.3 | −58.9% |
| value column, segment-shared dictionary + LZ4 | 148.9 | **−23.5%** |

The last row reproduces the 22–31% figure published for Parquet VARIANT versus raw JSON strings, confirming the format
delivers what is claimed and that this implementation's per-document framing is what does not.

**What it would take:** per-segment dictionary construction and persistence, a reader that loads and validates it, merge
handling when segments with different dictionaries combine, and a policy for dictionary growth mid-segment.
`VariantBuilder.presetDictionary` exists (package-private) but is called only from the audit test — nothing in production
writes a shared dictionary.

**Projected, not measured:** if the modelled 175.3 B/doc held, B′ would land near 706 B/doc against solution A's 755 — i.e.
storage-*positive* rather than +25.5%. Two earlier projections in this work were later refuted by measurement, so treat
this as a hypothesis to test, not a result.

**Unquantified read cost:** field ids widen from 1 byte to 2 (+15.9 B/doc, already included above), and the reader must
hold a segment-level dictionary rather than one it can bounds-check inside the document's own bytes. The binary search
itself is unaffected.

</details>

---

## I2 — `fields` retrieval on a `flat_object` returns the whole object as `Map.toString()` · open · bug, pre-existing

Independent of the Variant blob — this is current `flat_object` behaviour on `main`. Verified live against a 10M-document
index:

```
GET e10m-a/_search  {"size":1,"_source":false,"fields":["attributes.status"]}

"attributes.status" : [
  "{k8s.namespace=ns-14, level=debug, aws.dynamodb.table_names=-130327,
    server.address={f0=-108597, f1=8o05z, f2=null}, duration_ns=3037818611, ...}"
]
```

The requested subpath is **ignored**, and the value is a Java `HashMap.toString()` — `=` instead of `:`, no quoting, not
parseable as JSON. `fields: ["attributes"]` returns the identical string.

**Cause:** `FlatObjectFieldType.valueFetcher()` (`FlatObjectFieldMapper.java:301`) builds a `SourceValueFetcher` on the
*root* field name, and its `parseSourceValue` calls `value.toString()` on whatever `_source` holds — the entire object.

**Consequence for the design:** this was scoped as "Tier 1, blob-backed `ValueFetcher`" on the assumption it was an
optimization of working behaviour. It is not; there is no sensible existing output to stay compatible with. Both the
full-path form (`attributes.attr_status`) and the prefix form (`attributes.resource`, where `attributes.resource.host` is
a leaf) need building either way.

**Blocked on one decision:** should retrieval return typed values (`200`) or strings (`"200"`)? Typed uses what the blob
knows; strings match the rest of `flat_object`'s stringify-everything model. Deferred by the user pending the storage
work.

---

## I3 — The blob is decoded once per aggregation, not once per document · open · read latency

The most actionable read finding. Five metric aggregations on one path:

| | one aggregation (Q1) | five aggregations (Q8) | growth |
|---|---|---|---|
| Solution A | 13,531 ms | 14,371 ms | ×1.06 |
| the blob column | 732 ms | 4,107 ms | **×5.61** |

A barely notices the extra four, because `SourceLookup` caches the parsed document and all five scripts share it. B
scales nearly linearly, because each aggregation gets its own `DerivedFieldScript`, accessor, doc-values iterator, and
decode. B's advantage degrades from ~18× to ~3.5× on the common dashboard pattern of several aggregations over one field.

OPT-1 (caching the resolved accessor per script rather than resolving through the shared map per document) improved Q8
from 4,107 → 3,113 ms, a 31% marginal reduction, but the scaling shape remains. A v1 of OPT-1 that resolved inside
`variant()` **regressed everything by +21 ns/doc** by boxing a `Long` into a `ConcurrentHashMap` lookup 10–50M times —
worth remembering before attempting this again.

**Cheapest untested experiment:** revert OPT-2 while keeping OPT-1. OPT-2 (zero-copy decode) and OPT-3 measured 3–6%
*slower* on single-aggregation queries, plausibly because the copy they removed was also compacting the blob into cache.
That combination has never been measured.

---

## I4 — Per-document blob compression · closed-negative

LZ4-compressing each document's blob saved only ~6% (416 → 392 B/doc). The audit explains why: the metadata column
compresses −71% (key names are highly redundant) while the value column compresses only −10% (binary scalars have little
redundancy left). Per-document framing denies the compressor any cross-document window for the part that would compress.

Worse, block-compressed at 16 KB the blob is **larger** than block-compressed JSON: +17.7% (LZ4), +44.8% (deflate),
because JSON text is far more compressible than binary scalars.

**Do not pursue.** An earlier projection that a compressing blob column would reach ~−3% total storage is withdrawn. The
`variant_blob_compress` mapping option and the `b-zip-blobonly` benchmark arm are retained only so the negative result
stays reproducible.

---

## I5 — Reconstructing `_source` from the blob · deferred

Serving `_source` output for the field from the blob, so the field need not be stored twice. Explicitly on hold per the
user; not designed, not estimated.

---

## I6 — Derived fields are rejected on `_source`-disabled indices regardless of what the script reads · open

The design claimed the blob column keeps working where A cannot. Storage-wise that holds — the blob writes fine with `_source`
off. But `DerivedFieldType.getDerivedFieldLeafFactory` rejects *any* derived field on a `_source`-disabled index, even
when the script only touches the blob. Storing the blob is necessary but not sufficient, so the claimed functional
advantage is not currently reachable.

---

## I7 — `Paths.get` forbidden-API violation on `main` blocks `precommit` · open · unrelated

`./gradlew :server:precommit` fails on HEAD (`1fc06f218c7`, the `_parquet_export` commit):

```
Forbidden class/interface use: java.nio.file.Paths [Use org.opensearch.common.io.PathUtils.get() instead.]
  in TransportParquetExportAction (TransportParquetExportAction.java:107)
```

**Not a one-line fix.** The obvious substitution is also banned in `server`:

```
Forbidden method invocation: org.opensearch.common.io.PathUtils#get(String, String[])
  [Don't try reading from paths that are not configured in Environment, resolve from Environment instead]
```

The rule is pointing at something real: `_parquet_export` writes to a caller-supplied filesystem path with no
confinement. Fixing it properly means resolving the export path against `Environment` (as snapshot repositories do with
`repoFiles()`) and rejecting anything outside it — a design decision about export path policy, not a rename.
`@SuppressForbidden` would silence the check while leaving the unconfined write in place, which is the wrong trade for a
security-relevant rule.

Consequence: nothing in this branch can be validated by a full `:server:precommit` until this is resolved. The Variant
blob work was instead checked with `spotlessJavaCheck`, compilation, and its own unit and integration tests.

---

## I10 — Blob layout · RESOLVED 2026-08-25 · key names in their own column

**Resolved, and the layout is now the only one written.** Storing key names as a `SortedSetDocValues` column plus a
per-document rank list reads 11–16x faster than `_source` and stores 2.2–2.5% less, on every corpus measured. Full
results: [results.2026-08-25-0400.md](../../benchmarks/results/results.2026-08-25-0400.md).

| distinct key sets | layout | blob B/doc | vs `_source` | sum over 10M docs |
|---|---|---|---|---|
| 1,531 | key sets | 171.5 | −7.5% | 705 ms |
| 15,306 | key sets | 169.8 | −7.6% | 1,186 ms |
| 15,306 | **names** | 211.7 | **−2.2%** | **819 ms** |
| 9,942,838 | key sets | 243.6 | +2.6% | **53,503 ms** |
| 9,942,838 | **names** | 204.3 | **−2.5%** | **841 ms** |

The last two rows are the point. Bounding the dictionary by name count rather than key-set count makes the layout
indifferent to cardinality: 650x more distinct key sets moves the blob 3.5% and the query 2.7%, where the previous layout
lost a factor of 45. This also removes I1's cardinality cap, its cliff, and the unmeasured question of how much optional
fields inflate cardinality — none of them apply any more.

Memory: 20x less allocation than reading `_source` (480 B/doc against 9,710) for 71 KB of resident name table per
segment. A 10M-document scan produces ~4.8 GB of garbage instead of ~97 GB, which is where solution A's p90 spikes came from.

**I recommended against building this, with numbers, and was wrong.** I modelled only the dictionary-population cost and
omitted the per-document read cost, which turned out to be the larger term. See §7 of the report.

**Remaining:** Q8 (five aggregations on one path) is 2% slower than the previous layout, because each aggregation builds
its own accessor and so materialises the name table five times. That is I3, not the layout.

---

## I12 — The rank column · RESOLVED 2026-08-25 · removed by ordering field ids

Numbering the Variant's keys by name rather than by first appearance makes field id *i* the document's *i*-th smallest
name, which is exactly what the name column's ordinal list already hands a reader. The per-document rank column then
records nothing new and is not written. Full results:
[results.2026-08-25-1900.md](../../benchmarks/results/results.2026-08-25-1900.md).

| | col3 (3 columns) | **col3a (2 columns)** |
|---|---|---|
| blob B/doc, 10K shapes | 211.7 | **193.1** (−8.8%) |
| total vs `_source`, 10K shapes | −2.2% | **−4.6%** |
| total vs `_source`, super-test-set | −2.6% | **−4.9%** |
| query sum, 10K shapes | 16,391 ms | **15,005 ms** (−8.5%) |
| query sum, super-test-set | 16,717 ms | **14,547 ms** (−13.0%) |

Faster on all 18 query measurements the timer can resolve, and better at p90 in 17 of them. The first change in this work
to improve storage and reads together. Cardinality-independence is kept: 650× more distinct key sets moves the blob 3.5%.

The saving measured 18.13–18.61 B/doc across three runs spanning a 50× scale change, and is fully accounted for — a rank
list is `1 + count` bytes at ~15.5 keys per document, plus ~2 B/doc for the column's own offset array. The read saving is
21.3 ns/doc for the column itself plus per-probe indirection, 54–80 ns/doc at accessor level.

**Ingestion throughput is not distinguishable.** Within-arm spread over three repetitions (11.7%, 19.4%) is several times
the between-arm median difference (3.3%), and the two 10M runs disagree on the sign.

**Not stress-tested where it matters least but could matter.** Documents with more than 256 distinct keys keep their
insertion-order ids and still get a rank list, so a reader accepts both forms in one segment. Unit tests cover 255/256/257/600
keys and mixed segments, but no measured corpus contains such a document — every document in every index benchmarked took
the relabelled path.

---

## I13 — The name table has no cap, and vocabulary is unbounded · open · MEASURED 2026-08-26

The reader materialises every distinct key name in the segment into a `byte[][]`, per accessor, per segment. Nothing caps
it. Measured on a corpus of 10,000 services with per-service attribute names
([results.2026-08-26-0100.md](../../benchmarks/results/results.2026-08-26-0100.md)):

| vocabulary | segment open | resident | per name |
|---|---|---|---|
| 1,007 names | 0.38 ms | 66 KB | 65.6 B, 377 ns |
| **761,007 names** | **21.62 ms** | **27.4 MB** | **36.0 B, 28.4 ns** |

Linear in vocabulary, and multiplied by accessors: five aggregations on one field build five tables (137 MB, 108 ms); ten
segments × five aggregations is 1.37 GB. Extrapolating the same corpus to 10M distinct names gives ~360 MB per accessor,
which the reader would simply attempt.

**Already visible in query results.** `fetch 50 documents` regressed from 1 ms to 17 ms and is now **2.8× slower than
`_source`** — the 21.62 ms open *is* the whole query. Five aggregations on one path get 103× where one gets 206×.

Realistic trigger: keys that embed an identifier (`{"tenant_a1b2c3.latency": …}`) or metric names with inlined dimensions.
`flat_object` exists partly so such data does not explode the mapping, so pointing it at exactly that data is expected.

**Every measurement before 2026-08-26 held vocabulary at ~1,000 names**, including the super-test-set — that corpus
randomised which *combination* of names each document got, not the names themselves. So this was the least-tested dimension
in the whole project until now.

Three fixes, none free:

1. **Share the table per field per segment** across accessors. Removes the multiplier and also fixes [I3]; does not reduce
   the 27.4 MB. Unambiguously worth doing.
2. **Cap it and resolve names per lookup.** Bounds memory at a known cliff: scattered `lookupOrd` measured ~14,000 ns
   against ~28 ns amortised in a sequential sweep — the same cliff shape that sank the key-set layout (I9).
3. **Build it lazily in vocabulary order.** Helps small queries like the 50-document fetch; does nothing for scans.

**Not fixed.** 2 and 3 want the fallback's cost measured before either is chosen; this round established the cost of
*having* the table, not of not having it.

---

## I11 — Removing a mapping parameter strands every index that used it · open · found 2026-08-25

Deleting `variant_blob_shared_names` when its layout became the default left the two indices measured with it
(`s10k-names`, `esup-names`, 10M documents each) **permanently unopenable**:

```
failed to update mapping for index, failure MapperParsingException[Failed to parse mapping [_doc]:
Mapping definition for [attributes] has unsupported parameters:  [variant_blob_shared_names : true]]
```

The shard fails allocation, retries five times, and goes red. Nothing is wrong with the data — the segments are fine and
the layout still reads — it is the *stored mapping* that no longer parses, and there is no way to edit a mapping on an
index that will not open.

Nothing user-facing is broken, because the parameter only ever existed on this branch. The lesson is about what happens
at merge time: `flat_object`'s type parser rejects any parameter it does not recognise, so **any future removal of a blob
parameter is a data-loss-grade breaking change** unless the parameter is kept as a deprecated no-op. Both
`variant_blob_compress` and `variant_blob_rank_column` are on that list now.

**Not fixed.** Accepting the dead parameter as a no-op would unstrand the indices and is semantically exact — it meant
"use the name column", which is now unconditional — but it puts branch archaeology into production code for scaffolding
that never shipped. The measurements those indices produced were taken before the removal and are recorded in
[results.2026-08-25-0400.md](../../benchmarks/results/results.2026-08-25-0400.md), so nothing is lost but the ability to
re-query them. The layout's read compatibility is covered instead by writing it fresh through
`variant_blob_rank_column`.

---

## I9 — The two-column metadata split · SUPERSEDED by I10 · kept for the reasoning

**Resolved.** Splitting key metadata into a `SortedDocValues` column and indexing the segment's key sets by ordinal makes
the blob 59% smaller and a blob-only index 7.5% smaller than solution A, while reading 5.5-18.5x faster. Full results:
[results.2026-08-24-2332.md](../../benchmarks/results/results.2026-08-24-2332.md).

| corpus | distinct key sets | blob B/doc | B' vs A storage | reads vs A |
|---|---|---|---|---|
| 1,000 shapes | 1,531 | 171.5 | **-7.5%** | 5.5-18.5x faster |
| 10,000 shapes | 15,306 | 169.8 | **-7.6%** | 2.0-13.8x faster |
| keys unique per doc | 9,942,838 | 243.6 | +2.7% | falls back, see below |

Two defects were found on the way, both recorded because the reasoning matters more than the fix.

**The original two-column version was 73x slower than the single column and 4.2x slower than solution A.** Attributed by
direct measurement: `lookupOrd` cost 14,969 ns/doc against 35 ns/doc for everything else combined, 365x. Lucene stores
sorted terms in LZ4-compressed blocks of 16, so one lookup is a random seek plus a whole-block decompress for one term,
and ordinals are ordered by term rather than by document, so consecutive documents seek to unrelated blocks. Fixed by
indexing the segment's key sets by ordinal, so `lookupOrd` runs once per ordinal instead of once per document.

**The fallback is a cliff, not a slope.** A 3.95 MB dictionary and a 2,500 MB one ran the same query in 48.5 s and
53.5 s -- 9% apart. Cost is per-lookup rather than size-dependent, so being barely over the cap is nearly as bad as being
600x over. The cap therefore wants to sit well clear of realistic cardinalities: it is now 65,536 entries, filled on
demand.

**Remaining, minor:** lazy filling costs a fixed ~25 ms because it resolves ordinals in document order (random in ordinal
space, ~16x more block decompresses) where eager filling went sequentially. Invisible on a 10M-document scan (Q1 -4%),
+77% on a 100k-document one (Q3, 35 -> 62 ms). Fix not built: after N lazy misses, switch to a sequential full fill.

<details>
<summary>Original finding, kept for the record</summary>

### I9 (original) — The two-column metadata split: -41% storage, 73x slower reads

Storing key metadata as `SortedDocValues` and the value tree as `BinaryDocValues` — the two columns Parquet uses —
measured at 10M documents on the **super-test-set**:

| | blob B/doc | total vs A | Q1 (sum, 10M) |
|---|---|---|---|
| 1 column | 415 | +25.5% (blob only) | **732 ms** |
| 2 columns | **243** | **+2.7%** (blob only) | **53,503 ms** |

Storage improved 41%. Reads regressed 73× and became **4.2× slower than reading `_source`**. Attributed by direct
measurement, not inference:

| read step | ns/doc |
|---|---|
| value column only | 35.2 |
| + metadata ordinal | 41.0 |
| **+ `lookupOrd(ord)`** | **14,969.2** |
| + copy and cache | 15,167.0 |
| single-column equivalent (metadata read in place) | 40.0 |

`lookupOrd` is 365× everything else combined. Lucene's sorted term dictionary is stored in LZ4-compressed blocks with
prefix sharing, so one lookup costs a random seek into a 5.3 GB file plus a whole-block decompress to extract one term.
Ordinals are ordered by term, not by document, so consecutive documents seek to unrelated blocks: no locality, and
15/16 of each decompressed block wasted. Solution A's comparable cost — decompressing a 16 KB stored-fields block — is
amortised over ~30 documents read in order, which is why A wins.

**The saving and the cost are the same mechanism.** Block compression with prefix sharing is what shrinks the column by
41% and what makes single-term random access expensive.

**Both effects are super-test-set artifacts.** With ~1000 shapes the term dictionary holds ~1000 terms (~250 KB), stays
resident, and the per-ordinal cache hits ~100%; projected metadata cost falls to ~1.3 B/doc (blob ~161 B/doc) at
roughly no read cost. That projection is untested and this work has had two projections refuted already.

**Where Parquet differs, and where I described it wrongly:** Parquet reads the whole dictionary page into memory once
and then indexes it as an array, which it can do because a dictionary page has a size limit (~1 MB) — over that,
Parquet **abandons dictionary encoding and falls back to PLAIN**. Lucene's `SortedDocValues` never materialises; it
seeks per lookup. Applying the dictionary unconditionally is the defect. The correct design is adaptive on distinct
count, which is what the size limit accomplishes.

Also a real defect in the committed code independent of latency: the per-ordinal cache is an unbounded
`HashMap<Integer, VariantMetadata>` that grows toward one entry per document (~GBs retained per segment) and never
hits when key sets are unique.

Commit `2d5b3f6d8d8` justifies the split on storage alone; reads had not been measured when it was written.

</details>

---

## I8 — Not measured

These bound how far the conclusions travel:

- **Working sets larger than RAM.** The 11.7 GB index fits in 61 GiB alongside a 16 GiB heap, so both arms serve mostly
  from page cache. This is the largest remaining threat to the read result.
- **Beyond 10M documents.** Per-document costs held constant from 10k to 10M, but 100M was abandoned as confirmation
  rather than new information (~50 min indexing plus over an hour of force merge per arm).
- **Concurrency.** Single-threaded client, single shard throughout.
- **A raw-JSON-in-a-binary-column third arm**, which would separate columnar *location* from Variant *format*. Since the
  blob is ~1.00× the JSON text uncompressed, such a column would cost about the same to store, so this would isolate how
  much of the read win comes from the format rather than from the location.
- **Variant shredding** (typed sub-columns) — named by both the Parquet spec and Databricks as *the* read optimization,
  and excluded by this design's non-goals. Untouched.

---

## I14 — `doc['attributes.status']` ignores the key in the subscript

Measured 2026-09-02. Asking a `flat_object` for one path through `doc[]` returns **every** attribute in the
document, byte-identical to asking for the whole `_valueAndPath` column:

```
doc['attributes']         -> [attributes.200, attributes.info, attributes.ns-1]     (values, no keys)
doc['attributes.status']  -> [attributes.attributes.k8s.namespace=ns-1,
                              attributes.attributes.level=info,
                              attributes.attributes.status=200]                     (everything)
```

So there is no per-path read through `doc[]` today, and values arrive as text. Pre-existing on `main`, unrelated
to the Variant work. It matters here in two ways: it is why a script has to prefix-match and string-parse, and it
means giving that subscript correct semantics would fix a defect rather than break an interface.

Related: a `terms` aggregation directly on `attributes.status` returns three buckets, two rendered as
`java.lang.Object@...`, because `FlatObjectDocValueFormat` formats only the entry matching the path prefix and
passes the rest through raw. Also pre-existing. Worth filing upstream separately.

---

## I15 — Search-time derived fields cost the blob route 3.6×

Measured 2026-09-02, identical script over an identical index, 1M documents:

| | p50 |
|---|---|
| derived field declared in the mapping | 331 ms |
| same script passed in the search body | 1,193 ms |

Consistent with I13: the accessor is cached per thread per field, not per segment, so anything that multiplies
script instances multiplies the 21.62 ms / 27.4 MB name-table bind. Not isolated further. Moving the table into
`IndexFieldData` and the shard-level fielddata cache should remove the sensitivity entirely — see
`plan-columnar-flat-object.md` §2.1.

---

## I16 — The column costs +26.3% when `_source` keeps the field

Measured 2026-09-02. Two fresh indices, the same 100,000 documents, both with `_source` enabled, differing only
in whether the blob is written:

| | bytes/doc |
|---|---|
| plain `flat_object` | 5,587.1 |
| `+ variant_blob` | 7,054.6 |
| delta | **+1,467.5 (+26.3%)** |

Earlier runs reported index size going *down* 2.07% with the column enabled. That was net of excluding the field
from `_source`. The two decisions are separate, and the −2.07% figure should never be quoted without saying which
`_source` configuration produced it.

---

## Deliberately out of scope

**Optimising solution A.** Solution A parses `_source` into a full `Map`, which is exactly what `SourceLookup` and derived fields
do today. That is the point: A is the status-quo baseline, and the question is whether B is worth building against
current behaviour. A lazier `_source` reader is separate work and is not treated as a gap here.
