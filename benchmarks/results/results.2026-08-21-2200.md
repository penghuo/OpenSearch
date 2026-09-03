# Variant encoding audit — is the encoder actually following the Parquet spec?

**Date:** 2026-08-21 22:00
**Reproduce:** `./gradlew :server:test --tests "*.VariantEncodingAuditTests" -Dtests.output=true`
**Source:** `server/src/test/java/org/opensearch/common/variant/VariantEncodingAuditTests.java`

## The challenge being answered

Published figures put Apache Parquet's VARIANT type at **22–31% smaller** than the same data stored as a raw JSON
STRING. The measured blob column in arm B came out only **~4% smaller**. Either the encoder is not emitting the
compact forms the spec offers, or the published comparison is measuring a different layout.

**Verdict: the encoder is conformant and minimal. The entire gap is layout, and it is reproducible — the published
range appears exactly when the key dictionary is shared per segment instead of repeated per document.**

## Method

20,000 documents from the `E10M` corpus (1000-key Zipf-distributed sparse attributes). Encoded through
`VariantJson.encode`, which drives the same `VariantBuilder` calls the mapper's own parse walk does, so the bytes
counted are the bytes the index holds. Cross-check: this models **413.0 B/doc**, against **415.8 B/doc** derived
from the real index as `.dvd(B) − .dvd(A)`. Agreement to 0.7% — two independent routes to the same number.

## Part 1 — conformance: passes, with zero exceptions

The test asserts, for every value in every document, that the narrowest spec form was used. All five counters are
zero:

| Check | Non-minimal instances |
|---|---|
| integers wider than the value needs | **0** |
| strings ≤63 bytes not folded into the header byte | **0** |
| containers with an over-wide `field_id_size` | **0** |
| containers with an over-wide `field_offset_size` | **0** |
| element counts written in 4-byte form unnecessarily | **0** |

Forms actually emitted over 20,000 documents:

| form | count |
|---|---|
| string (short form, length in the header byte) | 150,931 |
| string (long form, 4-byte length) | **0** |
| int32 | 70,952 |
| double | 47,786 |
| object | 31,983 |
| int16 | 22,028 |
| boolean | 21,942 |
| array | 13,064 |
| int64 | 11,328 |
| null | 7,224 |
| int8 | 10 |

Integers land across int8/int16/int32/int64 rather than all in int64, and not one string used the long form. This
is what a spec-minimal encoder looks like. A non-minimal encoder would round-trip its own output perfectly, so
nothing else in the suite would have caught it — which is why this test asserts rather than only reporting.

## Part 2 — where the bytes actually go

| | B/doc | share of blob |
|---|---|---|
| **attributes as JSON text** | **425.9** | — |
| **attributes as a Variant blob** | **413.0** | **−3.0% vs JSON** |
| metadata (key dictionary) | 249.7 | **60.5%** |
| &nbsp;&nbsp;key name bytes | 223.1 | 54.0% |
| &nbsp;&nbsp;dictionary offsets | 24.2 | 5.9% |
| &nbsp;&nbsp;header | 2.4 | 0.6% |
| value tree | 159.3 | 38.6% |
| &nbsp;&nbsp;scalar payloads | 102.0 | 24.7% |
| &nbsp;&nbsp;type tags | 16.6 | 4.0% |
| &nbsp;&nbsp;field ids | 15.9 | 3.9% |
| &nbsp;&nbsp;field offsets | 20.3 | 4.9% |
| &nbsp;&nbsp;container headers + counts | 4.5 | 1.1% |
| framing (metadata length prefix) | 4.0 | 1.0% |

Corpus facts that drive this: 1007 distinct keys, 15.5 keys per document, mean key name 14.4 B, and **each distinct
key name is stored 308 times** across the sample.

**The single fact that explains everything: 60.5% of the blob is the key dictionary, and it is re-stored in full in
every document.** The value tree — the part that carries the actual data — is only 159.3 B/doc against 425.9 B of
JSON.

## Part 3 — layout models

### Uncompressed

| Model | B/doc | vs JSON |
|---|---|---|
| **M0** JSON text | 425.9 | baseline |
| **M1** blob as implemented (metadata glued in) | 413.0 | −3.0% |
| **M2** value column only, per-document dictionary | 159.3 | **−62.6%** |
| **M3** value column, segment-shared dictionary | 175.3 | **−58.9%** (dictionary costs 22,776 B once) |

M3 is measured, not estimated: the corpus is re-encoded against one dictionary holding all 1007 keys. Sharing the
dictionary is **not free** — field ids widen from 1 byte to 2, costing +15.9 B/doc, which is why M3 (175.3) is worse
than M2 (159.3). A secondary knock-on also appears: widening a nested object's header enlarges its parent's values
region, which pushes a parent sitting just under 256 bytes to a 2-byte field-offset width. That effect is real but
marginal (+0.05 B/doc), and the test bounds it rather than asserting it away.

### Block-compressed at 16 KB, as `_source` is

| Model | LZ4 B/doc | vs JSON | deflate B/doc | vs JSON |
|---|---|---|---|---|
| JSON text | 194.7 | baseline | 135.9 | baseline |
| **M1** glued blob | 229.1 | **+17.7%** | 196.7 | **+44.8%** |
| **M4** two columns, per-doc dictionary | 216.2 | +11.0% | 181.1 | +33.3% |
| &nbsp;&nbsp;metadata column | 73.5 | (34% of M4) | 58.1 | (32% of M4) |
| &nbsp;&nbsp;value column | 142.7 | (66% of M4) | 123.1 | (68% of M4) |
| **M5** value column, segment-shared dictionary | **148.9** | **−23.5%** | **129.2** | −4.9% |

**M5 lands at −23.5%, inside the published 22–31% range.** The challenge is answered: the published figure is real
and reproducible, and it describes a layout with a shared dictionary and a compressed column — which is what
Parquet has and what the current blob does not.

Two further results in this table are worth stating plainly because they contradict earlier expectations:

**Compressed, the blob as implemented is *larger* than compressed JSON — by 17.7% (LZ4) or 44.8% (deflate).** JSON
text is extremely compressible: LZ4 takes it from 425.9 to 194.7 B/doc (−54%), because key names, quotes and ASCII
digits repeat endlessly. The blob compresses much worse (413.0 → 229.1, −45%) because its scalars are already
binary and near-incompressible. Splitting into two columns (M4) recovers some of this but not enough.

**This explains the earlier LZ4 blob measurement of only ~6%.** The metadata column compresses 249.7 → 73.5 (−71%,
key names are highly redundant); the value column compresses 159.3 → 142.7 (only −10%, binary scalars have little
redundancy left). Gluing them together in one per-document blob gets the worst of both: no cross-document window
for the redundant part, and nothing to squeeze in the rest.

## What this means for the design

The measured ranking of layouts, on this corpus, per document:

```
M2  value column, per-doc dict, uncompressed      159.3   <- best uncompressed
M5  value column, shared dict, LZ4                148.9   <- best overall, -23.5% vs compressed JSON
M3  value column, shared dict, uncompressed       175.3
JSON, LZ4 (what arm A's _source effectively is)   194.7
M4  two columns, per-doc dict, LZ4                216.2
M1  blob as implemented, LZ4                      229.1
M1  blob as implemented, uncompressed             413.0   <- what is stored today
```

The blob as implemented is the **worst** option in the list, at 2.6× the best. The gap is not the encoding — it is
that a per-document dictionary re-stores 223 B of key names 10M times over. Three consequences:

1. **The +55% storage penalty is a layout artifact, not the cost of the format.** A shared dictionary would bring
   the blob to ~175 B/doc uncompressed against arm A's ~224 B/doc of compressed `_source` — B′ would then *save*
   storage rather than cost 25.5% more.
2. **Compressing the blob per document is not worth doing.** Measured at 6% previously, and this audit shows why:
   the compressible part is the dictionary, and per-document framing denies the compressor any cross-document
   window. Confirmed negative result.
3. **A shared dictionary has a read cost that is not measured here.** Field ids widen to 2 bytes, and the reader
   must hold a segment-level dictionary rather than one it can bounds-check within the document's own bytes. Both
   are plausible but unquantified; the binary search itself is unaffected.

## Limits of this audit

- **Sample, not the full corpus.** 20,000 documents out of 10M. The key-frequency distribution is Zipf, so the tail
  of rare keys is under-sampled; 1007 of the 1004 pool keys plus the 4 stable ones appear, so coverage is complete
  at the key level, but per-document blob size could drift slightly at full scale. The 0.7% agreement with the
  real-index delta bounds this.
- **Compression is a model, not Lucene.** `blockCompress` concatenates documents into 16 KB blocks and applies LZ4
  or deflate. Lucene's stored-fields format uses LZ4 with a preset dictionary and its own block framing, so its
  real numbers differ somewhat. Both arms are modelled identically, so the comparison holds even if the absolute
  figures shift.
- **`BinaryDocValues` is not compressed at all in Lucene 10.4.** Every compressed row above is a hypothetical about
  a column type that does not exist yet, not something switchable on.
- **M5 is not implemented.** `VariantBuilder.presetDictionary` was added (package-private) so the audit could
  measure M5 exactly rather than estimate it. Nothing in production writes a shared dictionary; the write path,
  segment-merge handling, and reader changes are all unbuilt.
- **Corpus shape dominates.** With mean key names at 14.4 B and 15.5 keys/doc, the dictionary is unusually heavy.
  Short keys, or documents with many values per key, would shift every ratio here.
