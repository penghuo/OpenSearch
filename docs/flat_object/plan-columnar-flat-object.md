# Plan: make a flat_object's value a first-class column

## 0. Where this comes from

A `flat_object` field is searchable but not readable: the only way to get a value back is `_source`, which means
decompressing a stored-fields block and parsing the whole document to reach one path. A prototype fixed the
reading part by writing the value as a Parquet-Variant blob in doc values, and it works — 331 ms against
39,991 ms for a `sum` over one attribute across a million documents.

But its user interface was wrong in every respect: a `variant_blob` mapping parameter the user had to know about,
a new `variant()` painless function, and no way to aggregate without hand-writing a derived field per path. This
plan replaces that interface. The layers underneath it — the Variant codec, `PathResolver`, `ValueCoercion` — are
unchanged and already tested.

## 1. The target interface

```
mapping   {"attributes": {"type": "flat_object"}}                 // the column is written; no parameter
agg       {"aggs":{"s":{"sum":{"field":"attributes.status"}}}}    // native, no script
script    doc['attributes'].value['status']                       // the whole object as a Map
storage   {"_source":{"excludes":["attributes"]}}                 // separate lever; the field is rebuilt from the column
```

Four properties this has to hold to:

- **No new mapping parameter.** The column is written for every `flat_object`. Nothing to opt into, nothing to
  remove later. (Shipping a parameter we intend to delete would strand indices — already observed: dropping
  `variant_blob_shared_names` left two benchmark indices unopenable.)
- **No new painless function.** `variant()` is deleted.
- **No inner-field access through `doc[]`.** `doc['attributes']` is the only supported subscript.
- **`_source` handling is orthogonal**, exactly as it is for numeric doc values. The column is written whether or
  not the field is in `_source`.

`_source` rebuilt from the column is **normalised, not byte-preserved**: keys come back sorted, and number
spellings the JSON parser already collapsed (`2e2` and `200.0`) come back in one form. Accepted.

---

## 2. Phase 1 — the read path

Full design in **`phase1-columnar-read-path.md`**. Summary:

- Fielddata over the blob column for a `flat_object`'s keyed subfields, so `attributes.status` is aggregatable and
  sortable with no script and no derived field.
- **The segment name table is eliminated, not relocated.** The prototype resolved a key by binary-searching an
  object's field ids and turning each candidate into a *name*, which is why a segment's entire name table had to
  be materialised — 27.4 MB and 21.62 ms at 761,007 distinct names, per accessor, unaccounted (I13). Because field
  id `i` is the document's `i`-th smallest name and the name column returns a document's ordinals ascending, a name
  can be turned into a field id without reading any name back: one `lookupTerm` per path per segment, then integer
  comparisons. I13 disappears, and a key absent from a segment now skips the segment entirely.
- `doc['attributes']` returns `ScriptDocValues<Map<String,Object>>`; `.value` is the object.
- Aggregatability is decided **per index** from the mapping, not per segment, so an index without the column gives
  today's clear error rather than a silently short answer.

Read-side only. The write path does not change, so there is no format risk in this phase.

**Known Phase 1 limitation.** `value_type` selects the values-source *shape* (numeric, bytes, boolean, …) and
takes precedence over the field's own type, but it does **not** convey numeric width: `ValueType.LONG` and
`ValueType.DOUBLE` both map to `CoreValuesSourceType.NUMERIC`. Aggregations therefore get `DOUBLE`, losing
precision above 2^53. Sorting is unaffected — `numeric_type` on the sort clause does carry width. The real fix is
a declared type per path, which is Phase 2.

---

## 3. Phase 2 — Variant shredding

Promote frequently-read paths out of the blob into their own typed doc-values columns, keeping the blob for
everything else. Named by the Parquet Variant specification as *the* read optimisation, and excluded from the
prototype's non-goals.

Why it is Phase 2 rather than later:

- **It answers the type question Phase 1 defers.** A shredded path has a declared type, so it gets exact `long`
  fielddata instead of `DOUBLE`-with-a-caveat. §2's limitation stops being a limitation for any path that matters.
- **It changes the on-disk layout.** Flipping the default (Phase 4) commits every new index to a layout. Shredding
  before that means users get one stable layout instead of a migration.
- **It is where the remaining performance is.** Phase 1 reads one value out of a document's blob; a shredded path
  reads a native numeric column and never touches the blob at all.

Design deliberately not settled here — see §7.

---

## 4. Phase 3 — rebuilding `_source` from the column

Needed so that excluding the field is a real option rather than a data-loss decision. Same machinery as
`doc['attributes'].value`, applied at three sites: `_source` fetch, `fields` retrieval, and reindex.

- **Fidelity rules** as stated in §1: keys sorted, number spellings normalised. Document them next to the setting.
- **`fields` retrieval currently returns `Map.toString()`** rather than structured values — recorded, not
  verified. Fix under this phase.
- **Per-document fallback.** Today a document that cannot be encoded fails the whole document. With the column
  written by default that is unacceptable: write no blob for that document, record its absence, and read it from
  `_source` instead. The only cases are more than 65,535 distinct keys in one document, and a top-level attribute
  named `_blob` / `_blobnames` / `_blobmeta`. Both are pathological and neither should cost a user their write.
  (Duplicate keys are *not* in this set — plain `flat_object` already rejects those, so nothing changes there.)

## 5. Phase 4 — defaults

Write the column for every `flat_object`. Keep an explicit escape hatch for anyone who wants byte-exact `_source`.

Storage, measured on an attribute-dominated corpus (2,785 B of attributes in a 2,846 B document):

| | bytes/doc |
|---|---|
| plain `flat_object` | 5,587.1 |
| with the column, field kept in `_source` | 7,054.6 (**+26.3%**) |
| with the column, field excluded from `_source` | ~5,470 (**−2.07%**) |

+26.3% is the default cost, and it is the same trade numeric doc values already make — written regardless of
`_source`, duplication accepted. It is the high end of the range: where attributes are a small part of the
document the delta is proportionally smaller. Users who want the space back exclude the field.

Force merge is the other real cost: **+61%** (260 s to 420 s at 1M documents), from two more doc-values columns
and a 761,007-entry name dictionary to union and remap on every merge. Shredding will change this figure in both
directions and it should be re-measured after Phase 2.

---

## 6. What carries over from the prototype

Unchanged and already tested — 178 unit tests plus an integration suite:

- `common/variant/*` — the Parquet Variant codec, its builder, metadata and JSON bridge
- `index/mapper/flatobject/PathResolver` — dotted-path resolution with the longest-prefix rule
- `index/mapper/flatobject/ValueCoercion`, `ValueType` — the coercion table Phase 1 depends on
- the two-column write path in `FlatObjectFieldMapper`, minus the mapping parameter

Deleted: the `variant_blob` parameter, `variant()`, `ScriptVariantAccess`, `VariantFieldAccess`, the painless
whitelist entry, `SearchLookup.variantFieldAccess` with its thread-keyed map, and `VariantBlobValueAccessor`'s
name table and per-thread caching.

## 7. Open decisions

Phase 1's four are in `phase1-columnar-read-path.md` §9. Phase 2's are open pending design:

1. **Who chooses what to shred** — declared in the mapping, or inferred from observed key frequency at flush time?
2. **What the declaration looks like**, given that not declaring keys is the whole point of `flat_object`.
3. **How a shredded path and the blob stay consistent** across merges of segments that shredded different paths.
4. **What happens when a shredded path holds a value of the wrong type** in some documents.

## 8. Measurement debts

- The `doc['attributes'].value` Map path is an estimate (2–5 s per million documents). Measure it once built.
- Absolute latencies from 2026-08-26 do not reproduce exactly — `_source` within 9%, the blob ~1.9× slower in a
  later session. Ratios within one session are sound; absolute figures should be re-measured before being quoted
  anywhere durable.
- The claim that `fields` retrieval returns `Map.toString()` is recorded but unverified.
