# Phase 1 — reading a flat_object path as a column

Design for the first shippable slice: make `attributes.status` aggregatable, sortable and script-readable straight
from the Variant column, with no derived field, no mapping parameter and no new painless function.

Phases 2–4 (shredding, `_source` reconstruction, defaults) are out of scope here and live in
`plan-columnar-flat-object.md`.

---

## 1. User-facing surface after this phase

```json
{"mappings": {"properties": {"attributes": {"type": "flat_object"}}}}
```

```json
{"aggs": {"s": {"sum": {"field": "attributes.status"}}}}
{"sort": [{"attributes.duration_ns": {"order": "desc", "numeric_type": "long"}}]}
```

```painless
def attrs = doc['attributes'].value;          // the whole object as a Map
if (attrs.status != null) { emit(attrs.status); }
```

Nothing is added to the mapping API and nothing is added to the painless API. `variant()` is deleted.

---

## 2. The key idea: no name table

The prototype's reader resolves a key by binary-searching an object's `field_ids` and, at each step, turning the
candidate id into a **name**:

```java
int fieldId = readUnsigned(value, fieldIdsStart + mid * fieldIdSize, fieldIdSize);
int comparison = metadata.compareKey(fieldId, probe);   // <- needs every name in the segment
```

That is the sole reason a segment's whole name table has to be materialised — measured at **27.4 MB and 21.62 ms
at 761,007 distinct names**, paid per accessor, unaccounted by any circuit breaker, and the cause of the one
regression this design has (a fifty-document query pays it in full). Issue I13.

It is avoidable. The write path already guarantees that **field id `i` is the document's `i`-th smallest key
name**, and the name column hands a reader that document's ordinals **ascending** — the same order. So a name can
be turned into a field id without ever reading a name back:

```
once per segment      targetOrd = names.lookupTerm("status")        one term-dictionary seek
                      targetOrd < 0  ->  no document in this segment has this key; serve empty

once per document     drain the document's ordinals (ascending) into a reused int[]
                      fieldId = binarySearch(ordinals, targetOrd)
                      not found  ->  the document has no such key

once per path         binary-search the object's field_ids for fieldId  (integer compares, no names)
                      decode exactly that one value
```

Consequences:

- **No name table, no fixed per-segment memory, and I13 disappears** rather than being relocated into the
  fielddata cache. Nothing to account against the breaker because there is nothing held.
- **Integer comparisons replace byte-string comparisons** in the hot binary search.
- **A whole segment can be skipped** when `lookupTerm` misses — a rare key now costs one seek per segment instead
  of a decode per document. The prototype had no such short-circuit.
- The per-segment state that remains is one `long` per path, so caching it **per path** — which is exactly what
  `IndexFieldDataService` does, keying caches by `fieldType.name()` — is now the correct granularity rather than a
  problem to work around.

### 2.1 What this needs from the codec

One addition to `Variant`:

```java
/** Binary-searches this object's field ids for {@code fieldId}, comparing ids rather than resolved names. */
public Variant objectGetByFieldId(int fieldId);
```

Same loop as `objectGet`, with `metadata.compareKey(...)` replaced by `Integer.compare(candidate, fieldId)`. The
existing `objectGet(String)` stays for `getAll` and `_source` reconstruction, which do need names.

### 2.2 Nested paths

Field ids are document-global and name-ordered, and every container's `field_ids` are sorted, so the same trick
works at depth. `nested.deep.value` resolves three ords once per segment, then per document does one ordinal
binary search per segment of the path and one `field_ids` binary search per level.

The dotted-key ambiguity is unchanged: `k8s.namespace` may be a literal key or a nested path, and `PathResolver`'s
longest-matching-prefix-without-backtracking rule still decides. Each candidate prefix needs its own `targetOrd`,
resolved once per segment.

### 2.3 Multi-path reads duplicate work, and this design makes that worse

Each path gets its own `IndexFieldData` and therefore its own `LeafFieldData` — the fielddata cache is keyed by
`fieldType.name()`, so `attributes.status` and `attributes.level` are separate entries with no shared state. Each
one independently advances the blob iterator, materialises the document's `BytesRef`, parses the Variant header,
and drains the document's ordinals.

So a two-path aggregation does that work twice per document. The prototype avoided it by sharing one accessor per
field per thread — which is exactly the `SearchLookup.variantFieldAccess` map this phase deletes. Removing the
name table removes the reason that map held anything *large*, but not the reason it existed.

Scope of the problem:

- **Single-path queries are unaffected**, and that is the common case and the benchmarked one.
- A two- or three-path query pays 2–3× the per-document blob work, which still leaves it far ahead of `_source`.

Deliberately not solved in Phase 1. If measurement says it matters, the fix is a per-segment shared cursor holding
`{docId, blob BytesRef, drained ordinals}` for the parent field, hung off the segment's core cache key with a
closed-listener — the `BitsetFilterCache` pattern. That is small, transient state, unlike the name table, so it
does not reintroduce I13. Listed in §11.

---

## 3. Fielddata

`FlatObjectFieldType.keyedFieldType(path)` returns a `FlatObjectFieldType` carrying the parent's name as
`rootFieldName`, whose `fielddataBuilder` currently produces `SortedSetOrdinalsIndexFieldData` over the
`_valueAndPath` column. It gains a fielddata implementation over the blob instead:

```
FlatObjectBlobIndexFieldData   implements IndexNumericFieldData
  - fieldName        the parent field, e.g. "attributes"   (the Lucene columns to read)
  - path             the requested path, e.g. "status"
  - numericType      see §4

FlatObjectBlobLeafFieldData    implements LeafNumericFieldData
  - targetOrd        resolved once, at load time, from the name column
  - getLongValues() / getDoubleValues() / getBytesValues() / getScriptValues()
  - ramBytesUsed()   ~0; nothing is materialised
```

`load(context)` resolves `targetOrd` and returns leaf data that reads the two columns directly. A missing
`targetOrd` yields an empty leaf, which is both correct and the cheapest possible answer.

`isAggregatable()` returns true — see §6 for which indices that is safe on.

Two things this shape depends on, both checked rather than assumed:

- **One implementation can serve both numeric and string aggregations.** `CoreValuesSourceType.BYTES.getField`
  falls back to `new ValuesSource.Bytes.FieldData(indexFieldData)` when the fielddata is not an
  `IndexOrdinalsFieldData`, and that path needs only `getBytesValues()`. So declaring
  `IndexNumericFieldData` does not preclude `terms` with `value_type: string`.
- **`numeric_type` works despite the field not being a numeric field type.** `FieldSortBuilder` gates it on
  `fieldData instanceof IndexNumericFieldData`, not on `fieldType.typeName()` — the type name appears only in the
  error message — and passes the resolved `NumericType` into `numericFieldData.sortField(resolvedType, ...)`. So a
  sort can request exact `long` ordering from a `flat_object` path.

---

## 4. Types

### 4.1 What `value_type` conveys, and what it does not

`ValuesSourceConfig.internalResolve` honours the hint ahead of the field's own type:

```java
if (userValueTypeHint != null) {
    // If the user gave us a type hint, respect that.
    valuesSourceType = userValueTypeHint.getValuesSourceType();
}
```

So `value_type` selects the **values-source shape** — numeric, bytes, boolean, date, ip, geo_point. That is
genuinely useful here: it decides whether `terms` buckets numbers or strings.

It does **not** convey numeric width. `ValueType.LONG` and `ValueType.DOUBLE` both map to
`CoreValuesSourceType.NUMERIC`, so `value_type: long` and `value_type: double` are indistinguishable by the time
the config is built. Width comes from `IndexNumericFieldData.getNumericType()`, which is ours to choose.

### 4.2 The width decision

`DOUBLE` for aggregations, with the limit documented: integers above 2^53 lose precision. `sum` and `avg` return
doubles regardless, so the exposure is `max`/`min` over large integer identifiers.

Sorting is better served: `numeric_type` on the sort clause (`FieldSortBuilder.NUMERIC_TYPE`, accepting
`long, double, date, date_nanos`) does carry width, so a sort can ask for `long` and get exact ordering.

This is a real Phase 1 limitation, not a solved problem. The proper fix is a **declared type per path**, which is
what Phase 2's shredding schema introduces — a path declared `long` gets exact long fielddata. Phase 1 ships the
documented default; Phase 2 removes the limitation for declared paths.

### 4.3 Coercion, and what happens to values that do not fit

`ValueCoercion` already implements this and the behaviour is three-way, not two. For `value_type: long` over a
path holding mixed data:

| stored | result |
|---|---|
| `200` | `200` |
| `"200"` | `200` — coerced, matching a numeric field's `coerce: true` default |
| `"200.7"` | `200` — parsed as a double, truncated toward zero |
| `[80, 443]` | **both**, as two doc values — see §9.2 |
| `"OK"` | **skipped**, `coercionFailures` increments |
| `true`, object | skipped |
| path absent | skipped, not counted as a failure |

**Lenient by default**: an aggregation over a million documents must not fail because one document holds `"OK"`.
Strict mode is deliberately deferred; if it is wanted later it is a request-level flag, not a change of default.

But lenient must not be silent. When a path skips at least one value, the response carries a warning header, so a
partial answer never looks like a complete one:

```
Warning: aggregation on [attributes.code] skipped values that could not be read as numeric
```

`HeaderWarning.addWarning` writes through the request-scoped `ThreadContext` and propagates from data nodes to the
client, and identical header values are deduplicated, so this costs one header however many documents were
affected. The **exact** count is deferred — see §9.1.

---

## 5. `doc['attributes']`

`LeafDocLookup implements Map<String, ScriptDocValues<?>>` and `ScriptDocValues<T> extends AbstractList<T>`, so
whatever `doc[...]` returns must be a `ScriptDocValues`, and every `ScriptDocValues` is a `List`.

`doc['attributes']` returns `ScriptDocValues<Map<String, Object>>` — a list with exactly one element, since the
mapper already rejects more than one object per document for this field. `.value` yields the `Map`:

```painless
doc['attributes'].value['status']
doc['attributes'].value.status
```

**Only `doc['attributes']` is supported.** `doc['attributes.status']` is not — see §7 for what it does today.

Cost: this decodes every attribute in the document, so it is roughly an order of magnitude behind native per-path
aggregation and an order of magnitude ahead of `_source`. Estimated 2–5 s per million documents against a
measured 39,991 ms for `_source` — **an estimate**, to be measured once built. No user gets slower.

The object is what `VariantBlobValueAccessor.getAll(docId)` already produces.

---

## 6. Indices without the column

The existing convention is to ignore a missing column. `AbstractIndexOrdinalsFieldData.load`:

```java
if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
    // If a field can't be found then it doesn't mean it isn't there,
    // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
    return AbstractLeafOrdinalsFieldData.empty();
}
```

Following it blindly is wrong here. For an ordinary field a missing column means those documents **have no
value**; here the value exists in `_source` and is merely invisible to the column, so "ignore" returns a
plausible **wrong number** rather than an empty one.

Decide at the index level instead of the segment level, from the **index creation version**:

- Phase 1 writes the column for every `flat_object`, unconditionally — there is no mapping parameter to consult
  (§8). So "does this index have the column?" is answered by whether the index was created at or after the version
  that introduced it, which is a check OpenSearch already makes routinely.
- Created before → `isAggregatable()` is false → the user gets today's clear
  `does not support doc_value in root field` error, never a silently short answer. Reindex to opt in.
- Created after → every segment has the column, so per-segment absence cannot arise and no mixed-segment case
  exists to handle.
- Per-document absence — a document the encoder could not handle — is a Phase 3 concern needing a per-document
  fallback. Phase 1 has no such documents because the write path still rejects them outright.

---

## 7. `doc['attributes.status']` today

Measured, and worth stating so the change is understood as a fix rather than a break. Asking for one path returns
**every** attribute in the document, byte-identical to asking for the whole `_valueAndPath` column:

```
doc['attributes']         -> [attributes.200, attributes.info, attributes.ns-1]
doc['attributes.status']  -> [attributes.attributes.k8s.namespace=ns-1,
                              attributes.attributes.level=info,
                              attributes.attributes.status=200]
```

Recorded as I14, together with a related defect: a `terms` aggregation directly on `attributes.status` returns
buckets rendered as `java.lang.Object@...` because `FlatObjectDocValueFormat` formats only the entry matching the
path prefix. Both pre-existing on `main`. Phase 1 gives the subscript meaning for aggregation and sort; whether
`doc['attributes.status']` should also start working is left open (§9).

---

## 8. What is added, reused and deleted

**Added**

- `Variant.objectGetByFieldId(int)` — §2.1
- `FlatObjectBlobIndexFieldData` / `FlatObjectBlobLeafFieldData` — §3
- `ScriptDocValues<Map<String,Object>>` for the parent field — §5
- An index-creation-version gate on `isAggregatable()` — §6

**Changed**

- The column is written for **every** `flat_object`, unconditionally. There is no parameter, so nothing decides
  per index except when the index was created. Phase 4 is therefore only about the `_source` default, not about
  whether the column exists.

**Reused unchanged** — already covered by 178 unit tests plus an integration suite

- `common/variant/*` — the codec, builder, metadata, JSON bridge
- `PathResolver` — dotted-path resolution and the longest-prefix rule
- `ValueCoercion`, `ValueType` — §4.3 depends on them exactly as written
- the two-column write path in `FlatObjectFieldMapper`

**Deleted**

- the `variant_blob` mapping parameter
- `variant()`, `ScriptVariantAccess`, `VariantFieldAccess`, the painless whitelist entry
- `SearchLookup.variantFieldAccess` and its thread-keyed map
- `VariantBlobValueAccessor`'s name table, per-thread caching and `setNextReader` bind — the reader becomes
  `LeafFieldData` and §2 removes the state it was caching

---

## 9. Decisions

### 9.1 Skipped values are reported, but not counted, in Phase 1

A `terms` or `sum` over a path where some documents hold an unreadable value returns a partial answer. That must
be visible, so the response carries the warning header in §4.3 whenever a path skips anything.

The exact number is deferred, because there is nowhere request-scoped to accumulate it. `LeafFieldData` is cached
per segment and shared across requests, so a counter there would accumulate across unrelated searches.
`IndexFieldData` *is* built per request, so a per-request wrapper around the cached leaf could carry one — but
nothing retains that object after the aggregation runs, so there is no one to read it back. Reporting a number
means introducing a request-scoped accumulator into the fielddata path, which deserves its own design rather than
being bolted onto this phase.

So: the warning tells a user their numbers exclude something. Finding out how much is later work.

### 9.2 An array contributes every element

A path holding `[80, 443]` contributes both values, exactly as a real `long` field does with the same JSON.
`SortedNumericDocValues` supports several values per document natively via `docValueCount()`, so this needs no new
machinery.

This **changes the prototype's behaviour**, which treats any container as unreadable — `getLong('ports')` returns
null today. Since §10 asserts the native and script routes agree, `ValueCoercion` has to be aligned so both expand
arrays. That alignment is work this phase owns, not a free consequence.

### 9.3 `missing` cannot distinguish absent from unreadable

Document A has no `code` key. Document B has `code: "OK"`, which a numeric aggregation cannot read. With
`"missing": 0` both are treated as missing and both contribute `0`.

Excluding B instead — on the grounds that B has a value and calling it `0` invents data — is the better semantics
and is **not reachable in this phase**. `missing` substitution happens in `ValuesSourceType.replaceMissing`, so
distinguishing the two cases means supplying our own `ValuesSourceType`. That is possible, since
`getMappingFromRegistry` reads the type straight off our fielddata:

```java
return fieldContext.indexFieldData().getValuesSourceType();
```

but it breaks everything downstream. Aggregations register implementations *against* a values-source type, and
`ValuesSourceRegistry.getAggregator` throws `<field> is not supported for aggregation [sum]` when nothing is
registered. `SumAggregationBuilder` registers against `CoreValuesSourceType.NUMERIC`; a new type would need every
aggregation registered against it, including aggregations from plugins we cannot register. It would also be
bypassed whenever a user passes `value_type`, since the hint overrides the field's type in `internalResolve`.

The divergence is narrow: the two behaviours differ **only** when `missing` is specified. Without it, absent and
unreadable values are both excluded, which is already the preferred semantics. So Phase 1 treats both as missing
and documents it in one sentence; the better behaviour needs a change to `MissingValues` and the aggregation
framework, which is a separate proposal.

### 9.4 `doc['attributes.status']` stays unsupported

It is broken today (§7) and fixing it would be nearly free once fielddata exists, but the interface rule is that
`doc['attributes']` is the only supported subscript. Leaving it alone keeps one rule instead of two, and its
current output is not something worth preserving either — so it is neither fixed nor relied upon.

---

## 10. Test plan

- **Equivalence.** Every path and type read through fielddata must equal what `_source` returns, over the existing
  `RICH_DOC`, `UNSORTED_KEY_DOCS` and the random generated corpus. This is `AccessorEquivalenceTests` extended to
  a third reader rather than new tests.
- **The ordinal-to-field-id invariant** is now load-bearing for correctness rather than for size. Existing tests
  cover it (`testOrderedFieldIdsMatchSource`, `testRelabelledIdsAreAscendingWithinEachObject`,
  `testWideDocumentsAreReencodedNotRanked`); add one asserting `objectGetByFieldId` and `objectGet(name)` agree
  for every key of every document.
- **Segment skip.** A key absent from a whole segment must serve empty without decoding any document — assert via
  a counter, not by timing.
- **Merge.** Aggregation results must be identical before and after `forceMerge(1)`, since ordinals are
  reassigned by the merge and the invariant has to survive it.
- **Mixed types.** `sum` over a path holding numbers, numeric strings and words, asserting the value and that the
  warning header is present. A path with no skips must produce no warning.
- **Arrays.** `sum` over a path holding `[80, 443]` must contribute both values, and must agree with the script
  route once `ValueCoercion` is aligned (§9.2).
- **`missing`.** A document with an absent path and a document with an unreadable value must both take the
  `missing` substitution (§9.3), pinned so a future framework change is a deliberate one.
- **Aggregation parity with the script route.** The same aggregation via native fielddata and via a derived field
  reading `doc['attributes'].value` must agree.
- **Empty and absent.** No column, empty object, path absent from every document, path absent from some segments.

---

## 11. Deferred, with the reason

- **Per-path duplication of the per-document blob work** (§2.3). Measure a two- and three-path aggregation first;
  the fix is a per-segment shared cursor if the numbers justify the machinery.
- **Exact numeric width for aggregations** (§4.2). `DOUBLE` with a documented 2^53 limit until Phase 2 gives paths
  a declared type.
- **Strict coercion mode** (§4.3). Lenient is the only behaviour; a strict flag is additive if asked for.
- **An exact count of skipped values** (§9.1). Needs a request-scoped accumulator in the fielddata path.
- **Excluding unreadable values from `missing` substitution** (§9.3). Needs `MissingValues` and the aggregation
  framework to support a three-state values source.
- **`_source` reconstruction and the `_source` default** — Phases 3 and 4.
