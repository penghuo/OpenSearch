# Phase 1 — reading a flat_object path as a column

Make `attributes.status` aggregatable, sortable and script-readable straight from the Variant column, with no derived
field, no mapping parameter and no new painless function.

Phases 2–4 (shredding, `_source` reconstruction, defaults) live in `plan-columnar-flat-object.md`.

> **Revision.** This is the second version. The first was written from the design outward and got several load-bearing
> facts about the code wrong — two of which would have shipped silently wrong query results. §12 lists what changed and
> why, so the corrections are auditable rather than quietly folded in. Sections are marked **[done]** or **[open]**.

---

## 1. User-facing surface

```json
{"mappings": {"properties": {"attributes": {"type": "flat_object"}}}}
```

```json
{"aggs": {"s": {"sum": {"field": "attributes.status"}}}}
{"sort": [{"attributes.duration_ns": {"order": "desc", "numeric_type": "long"}}]}
```

```painless
doc['attributes'].value['status']
```

Nothing is added to the mapping API, and no painless *function* is added — `variant()` is deleted. One whitelist stanza
is needed after all, for the new `ScriptDocValues` subclass, because `.value` is not inherited: every subclass declares
its own `getValue()`. Declaring that as `Map` is what keeps the subscript free, since `java.util.Map` already exposes
`def get(def)`.

---

## 2. The key idea: resolve by field id, not by name **[done]**

Resolving a key used to compare the *name* each candidate field id stands for:

```java
int fieldId = VariantEncoding.readUnsigned(value, fieldIdsStart + mid * fieldIdSize, fieldIdSize);
int comparison = metadata.compareKey(fieldId, probe);   // needs every name in the segment
```

That single call is why a segment's whole name table had to be materialised — 27.4 MB at 761,007 names, per reader,
accounted by nothing, and paid in full by a fifty-document query. Issue I13.

The write path guarantees field id `i` is the document's `i`-th smallest key name, and the name column returns a
document's ordinals ascending — the same order. So a name becomes a field id without reading any name back:

```
once per segment    ord = names.lookupTerm(candidate)          one term-dictionary seek per candidate
once per document   fieldId = binarySearch(ordinals, 0, count, ord)
per container       objectGetByFieldId(fieldId)                unsigned integer compares
```

Consequences: no name table on this path, nothing to account against a breaker, integer compares instead of byte-string
compares, and **a key absent from a segment skips the segment entirely** rather than decoding every document in it.

Three things that are easy to get wrong, all of which cost real debugging:

- **A name in the document's ordinals does not mean it is a key of the container being probed.** The Variant dictionary
  is one per document, shared across every nesting level, so a key used only at depth 2 still appears in the ordinal
  list. The per-container probe is mandatory; only the two negative cases short-circuit.
- **The bounded `binarySearch(ordinals, 0, count, ord)` is required.** The ordinal buffer is deliberately oversized, so
  the 3-arg form would search stale entries from a previous document — and an unsorted tail makes the result undefined
  rather than merely wrong.
- **`Integer.compareUnsigned`, not `Integer.compare`.** `readUnsigned` returns a plain `int`, so a four-byte field id at
  or above 2^31 arrives negative and would sort below every other id.

### 2.1 The codec addition **[done]**

```java
/** Binary-searches this object's field ids, comparing ids rather than the names they resolve to. */
public Variant objectGetByFieldId(int fieldId);
```

**Precondition, and it is not a format guarantee.** Ascending field ids within an object hold only because the writer
relabels them into name order; the Variant spec orders members by key *string*. A value straight out of `VariantBuilder`
with an insertion-order dictionary will make this method **silently miss present keys**. It is in the javadoc, and no
test may build its fixture with the plain builder — `VariantRoundTripTests.relabelIntoNameOrder` exists for that.

### 2.2 Nested paths **[done]**

The candidate set is every dot-delimited span starting at the path start or just after a dot: for `nested.deep.value`
that is `{nested.deep.value, nested.deep, nested, deep.value, deep, value}` — **n(n+1)/2 spans, not n**. All of them are
seeked once per segment because the set is purely *syntactic*.

**The choice among them is per document, not per segment.** The longest-prefix rule probes live containers, so two
documents in one segment can resolve the same path along different splits. Any implementation that caches "the split"
per segment is wrong.

The segment-skip test uses **prefix spans only**. Resolution's first probe at the root is always a prefix, so if none
exists no document can match. A suffix span such as `value` may be a key elsewhere in the corpus and proves nothing.

### 2.3 Per-path duplication, and it is worse than first written **[open, accepted]**

The fielddata cache is keyed by `fieldType.name()`, so each path gets its own leaf with no shared state. Worse,
`ValuesSource.Numeric.FieldData` calls `load(context)` separately for `longValues`, `doubleValues` and `bytesValues`, so
the duplication is **within one path in one aggregation**, not only across paths: several leaves per segment, each with
its own blob cursor and ordinal buffer.

Single-path queries — the common and benchmarked case — are unaffected. Deliberately not solved; the fix if measurement
demands it is a per-segment cursor on the segment's core cache key, which is small transient state and does not
reintroduce I13.

---

## 3. Fielddata **[done]**

`FlatObjectFieldMapper.keyedFieldType(String)` (on the *mapper*, overriding `DynamicKeyFieldMapper` — not on the field
type) is the single point where a subfield type is built, so it is where the version and path state are injected.

```
FlatObjectBlobIndexFieldData   extends IndexNumericFieldData
  fieldName          the FULL keyed name, "attributes.status"
  blobFieldName / blobNamesFieldName   derived from the parent
  path               "status"
  getNumericType()   DOUBLE
  sortRequiresCustomComparator()  true
  load == loadDirect, no caching

FlatObjectBlobLeafFieldData    implements LeafNumericFieldData
  holds {reader, column names, path} and no cursor
  every getXValues() opens its own VariantBlobPathReader
  ramBytesUsed()     0
```

Four decisions that are not free:

- **`getFieldName()` returns the full keyed name.** It labels the `SortField`, `LongValuesComparatorSource` asserts on
  it, and it is the fielddata cache key. That it names no Lucene field is fine: with neither points nor a doc-values
  skipper, Lucene finds nothing to build competitive iteration from.
- **`sortRequiresCustomComparator()` must be `true`.** When false, `IndexNumericFieldData.sortField` short-circuits a
  MIN/MAX sort to a raw `SortedNumericSortField(getFieldName(), ...)` — a direct read of a Lucene column that does not
  exist. Lucene returns an empty iterator for an absent field rather than failing, so **every document would sort as
  missing, silently.** This is the easiest way to ship a wrong sort and no test catches it unless it asserts real
  ordering across segments.
- **Implement `LeafNumericFieldData` directly, never extend `LeafLongFieldData`/`LeafDoubleFieldData`.** Both declare
  `getScriptValues()` and `getBytesValues()` `final`, and derive bytes from the numeric view — so `terms` with
  `value_type: string` over a path holding `"info"` would bucket a stringified double.
- **Nothing is cached.** `load == loadDirect`. There is nothing to cache, and building fresh keeps every cursor confined
  to one iteration, which is what makes a shared leaf safe.

Both of §3's original claims verified correct: `CoreValuesSourceType.BYTES.getField` does fall through to
`ValuesSource.Bytes.FieldData` for non-ordinals fielddata, and `FieldSortBuilder` does gate `numeric_type` purely on
`instanceof IndexNumericFieldData`.

### 3.1 DocValueFormat — the blocker the first design never mentioned **[done]**

`ValuesSourceConfig` asks the field type for a `DocValueFormat` on **every** aggregation with a field, before reading a
document. The existing `FlatObjectDocValueFormat` implements only `format(BytesRef)` and `parseBytesRef`, so:

| what breaks | where |
|---|---|
| `sum` rendering `value_as_string` | `format(double)` → `UnsupportedOperationException` |
| `"missing": 0` | `parseDouble` → throws *before any document is read* |
| any multi-shard aggregation | `"flat_object"` is not a registered `NamedWriteable`; coordinator reduce fails |
| bucket keys | `format(BytesRef)` prefix-strips and returns a `DOC_VALUE_NO_MATCH` sentinel — I14's `java.lang.Object@...` |

It also cannot be made round-trippable as it stands: a non-static inner class with an empty `writeTo` that drops its
prefix.

**A blob-backed keyed path returns `DocValueFormat.RAW`.** The values reaching it are already bare, so there is nothing
to strip. This is a real change and it broke `FlatObjectFieldMapperTests.testFetchDocValues`, which asserted the
prefix-stripping behaviour; end-to-end `docvalue_fields` output is unchanged, because what used to be
`format("field.field.name=1234") → "1234"` is now `format("1234") → "1234"`.

---

## 4. Types **[done]**

### 4.1 What `value_type` conveys

`ValuesSourceConfig.internalResolve` honours the hint ahead of the field's own type, so `value_type` selects the
values-source **shape** — numeric, bytes, boolean, date, ip, geo_point — and decides whether `terms` buckets numbers or
strings.

It does **not** convey numeric width: `ValueType.LONG` and `ValueType.DOUBLE` both map to `CoreValuesSourceType.NUMERIC`.
(`numeric_type` on a sort also accepts `unsigned_long`, despite its error message listing four.)

**The sharp edge, found by the REST test rather than by reading.** The field reports itself as numeric, so `terms` on a
path holding *words* buckets through a numeric values source, coerces none of them, and returns **zero buckets with no
error**. It needs `value_type: string`:

```json
{"terms": {"field": "attributes.k8s.namespace", "value_type": "string"}}
```

A schemaless column cannot infer this, and one default cannot serve both. Numeric is the right default because it is
what makes `sum`/`avg`/`min`/`max` and a numeric sort work with no hint at all — the headline case — and because the
alternative default would make *those* fail. The empty result is the same thing a `terms` aggregation on a declared
numeric field does when handed words, so it is at least not a new behaviour. Both spellings are pinned in
`93_flat_object_columnar_aggregation.yml` so this is a contract rather than a surprise; a path with a declared type
(Phase 2) stops needing the hint.

### 4.2 Width

`DOUBLE`, with the limit documented: integers above 2^53 lose precision through an aggregation. `sum` and `avg` return
doubles anyway, so the exposure is `max`/`min` over large integer identifiers.

`NumericType.DOUBLE` makes `isFloatingPoint()` true, so `terms` buckets as `DoubleTerms` and every metric reads
`doubleValues()`. `getLongValues()` is exercised only by a sort asking `numeric_type: long`, and it therefore reads the
**stored value** rather than casting the double — otherwise the exactness that was asked for is lost on the way.

A sort escapes the limit; an aggregation does not. The fix is a declared type per path, which is Phase 2.

### 4.3 Coercion **[done]**

| stored | result |
|---|---|
| `200` | `200` |
| `"200"` | `200` — coerced, as a numeric field's `coerce: true` default does |
| `"200.7"` | `200` — parsed as a double, truncated toward zero |
| `[443, 80]` | **both**, ascending — see §9.2 |
| `[[80,443],8080]` | **three values**, flattened recursively |
| `"OK"` | skipped |
| `true`, object | skipped |
| path absent, or present and null | skipped, and not a failure |

`ValueCoercion` is unchanged: expansion happens in the caller. Changing `coerce()` to accept containers would break four
`ValueCoercionTests` assertions and change what the single-valued `get()` means for the `tags`/`numbers` paths.

**Lenient, and lenient by design**: an aggregation over a million documents must not fail because one document holds
`"OK"`. Strict mode is a later request-level flag, not a change of default.

---

## 5. `doc['attributes']` — a lazy Map view **[done]**

The first design said this returns what `getAll(docId)` produces. That is a materialised `Map`, which needs **every key
name of every document visited**, because `toJavaObject()` on an object calls `objectKeyAt` → `metadata.key`. Two ways
to get those names, and both were bad:

| approach | cost |
|---|---|
| eager per-segment table, built on first script use | reintroduces I13's 27.4 MB, just later, and rebuilt per script instance |
| per-document `lookupOrd` over that document's ordinals | the *scattered* access pattern: ~14,000 ns per name against ~230 ns sequential. At 100 keys that is ~1.4 ms per document — **~1,400 s per million, against `_source`'s 40 s** |

So the doc's own "no user gets slower" claim fails under either.

**The way out is to notice that scripts do not want the whole object.** `doc['attributes'].value['status']` wants one
value. So `.value` returns a **lazy `Map` view over the document's blob**, not a copy:

| operation | cost | needs names? |
|---|---|---|
| `get(key)` | cached `lookupTerm(key)` per segment, then §2's field-id search | **no** |
| `containsKey(key)` | same | **no** |
| `size()` | `objectSize()` | **no** |
| `entrySet()`, `keySet()`, `values()`, iteration | materialise this document's names via `lookupOrd` | yes, and only then |

The common script pays one field-id lookup and never touches a name. Enumeration still pays, but only when a script
actually enumerates — which is rare, and is the same machinery Phase 3's `_source` reconstruction needs anyway.

`getValue()` is **declared** as returning `Map<String, Object>`, so painless dispatches `['status']` through
`java.util.Map`'s already-whitelisted `def get(def)`. The concrete view class needs no whitelist entry, which removes an
item the first design had in §8.

`doc['attributes.status']` stays unsupported (§9.4).

---

## 6. Indices without the column **[done]**

The first design put this gate in `isAggregatable()`. **That gates nothing**: the aggregation framework never reads
`isAggregatable()` — its only consumers are field-caps and star-tree validation — and `MappedFieldType`'s base
implementation *derives* it from whether `fielddataBuilder` throws. A gate written as an override would have let a
pre-3.6 index aggregate and return a **silently wrong number**, which is the exact failure this section exists to
prevent. The quoted error message was also unreachable: it comes from `docValueFormat` and only for the bare parent
field.

So:

- **The gate is an `IllegalArgumentException` from `fielddataBuilder`**, with a new message naming the index and version.
  That is the first thing `ValuesSourceConfig` calls, so it is always what the user sees.
- **The `isAggregatable()` override stays**, returning `blobPath() != null && hasBlobColumns()`. It must not be deleted:
  the parent field's builder now succeeds because a script needs it, so the base derivation would report the parent as
  aggregatable — which would stop `AggregatorTestCase.testSupportedFieldTypes` skipping `flat_object` and start
  exercising it in **every** aggregation test in the repo against a document this mapper never wrote.
- The version is `Version.V_3_6_0` — `CURRENT`, unreleased. No new constant. (`Version` lives in `libs/core`.)
- It is carried on the **field type**, not only the mapper, because `FieldMapper.merge` replaces the field type from the
  incoming mapper while leaving mapper-owned fields at their cloned values. Constructor overloads default to
  `Version.CURRENT` so existing test call sites are untouched.

### 6.1 The write is gated too **[done, a deviation]**

The first design wrote the column unconditionally. That would newly reject, on **every existing** `flat_object` index,
documents they accept today: a top-level key named `_blob`/`_blobnames`/`_blobmeta`, and more than 65,535 distinct keys
in one document. Gating the write on the same version leaves pre-3.6 indices byte-identical and avoids writing a column
no reader will consult.

A duplicate JSON key is **not** in that set — plain `flat_object` already rejects it, verified against a running node.
It was however a 500 rather than a 400, because `endObject()` throws outside the `try`; that is fixed.

### 6.2 `variant_blob` is retired, not deleted **[done, a deviation]**

Deleting the parameter would permanently strand any index whose stored mapping names it: this type parser rejects a
mapping with any unrecognised key, so the shard fails allocation, the index goes red, and a mapping cannot be edited on
an index that will not open. That already happened on this branch (I11). It is accepted, ignored, and deprecation-logged.

---

## 7. What `doc['attributes.status']` does today

Asking for one path returns **every** attribute in the document, byte-identical to asking for the whole `_valueAndPath`
column. Recorded as I14 with a related defect: `terms` directly on a subfield renders buckets as `java.lang.Object@...`.
Both pre-existing.

---

## 8. Added, reused, deleted

**Added** — `Variant.objectGetByFieldId(int)`; `VariantMetadata.NameResolver` and the resolver-backed constructor;
`VariantBlobPathReader`; `FlatObjectBlobIndexFieldData` / `FlatObjectBlobLeafFieldData`; the lazy Map view (§5); the
`fielddataBuilder` version gate.

**Changed** — the column is written for every `flat_object` created at or after 3.6.0; keyed paths return
`DocValueFormat.RAW`; `isAggregatable()` is true for a blob-backed keyed path; **`ValueCoercion`'s caller expands
arrays** (the first design wrongly listed `ValueCoercion` as reused unchanged while §9.2 required aligning it).

**Reused unchanged** — the codec, `PathResolver`, `ValueCoercion` itself, the two-column write path.

**Deleted** — `variant()`, `ScriptVariantAccess`, `VariantFieldAccess`, the painless whitelist entry,
`SearchLookup.variantFieldAccess` and its thread-keyed map, `VariantMetadata`'s dead rank form, and three pieces of dead
code already in the prototype (`metaField`, `RESERVED_BLOB_NAMES_KEY` never being checked, the 3-arg mapper constructor).

**Kept, contrary to the first design** — `VariantBlobValueAccessor` and `FlatObjectValueAccessor.setNextReader`. Turning
the accessor into the `LeafFieldData` would break 15 call sites and remove the only reader that can serve `RAW` reads and
`getAll` for the equivalence comparison. The accessor stays as the oracle; fielddata is a third arm beside it. (The first
design's "per-thread caching" also misdescribed it: the accessor has no thread-keyed state — that was all in
`SearchLookup`.)

---

## 9. Decisions

### 9.1 Reporting skipped values — the warning header does not work **[open, needs your call]**

You chose "report the count" over silence. The mechanism the first design named cannot deliver it:

**`HeaderWarning` is non-deterministic here.** Concurrent segment search is on by default for aggregations. Slice tasks
run on the `index_searcher` pool, whose `ContextPreservingRunnable` restores the worker's own context on exit and never
merges the worker's `responseHeaders` back. Lucene hands some slices to the executor and runs the rest on the caller, so
the header survives only for whichever slice happens to land on the calling thread. A warning that appears sometimes is
worse than no warning.

There is also no fielddata-level emission point on the request thread: `load()` is reached from `getLeafCollector` on the
slice worker.

Three channels that do work, in order of cost:

1. **`value_count` against `hits.total`** — available today, exact, no code. It tells a user how many values contributed;
   the gap is the skips. Not automatic, but it is a real answer to "why is my sum lower than I expected".
2. **The aggregation profile** — `ConcurrentAggregationProfiler` already exists precisely to merge per-slice breakdowns,
   so it is the one channel that survives the concurrency that defeats the header. An exact per-aggregation count under
   `"profile": true`. The counter can live on the per-request `IndexFieldData` (built fresh per request, and retained by
   `ValuesSourceConfig` → `FieldContext` for the life of the request), with `ProfilingAggregator` reading it.
3. **A shard-level stat** — deterministic and operator-visible, but new stats API surface.

**Recommendation:** document (1) as the Phase 1 answer, implement the counter on the per-request `IndexFieldData` now
because it is nearly free, and land (2) as a small follow-up. Drop the header entirely. Note this reverses your Q2(b)
answer on the mechanism, not on the intent — hence flagging rather than deciding.

Also worth knowing regardless: `HeaderWarning.addWarning` runs two regex `assert`s per call, so with assertions enabled
it would dominate a benchmark if called per skipped value.

### 9.2 An array contributes every element **[done]**

Recursively, matching `DocumentParser` re-entering `parseArray`, and **ascending** — `MultiValueMode.MIN` takes the first
value and `MAX` walks to the last, so `[443, 80]` left in document order would report a minimum of 443. A container
element inside an array (`[80, {"a":1}]`) is one skipped value plus one kept, consistent with §4.3.

This is a genuine divergence from the single-valued accessor, not a bug: `get()` refuses a container, fielddata expands
it. The equivalence test asserts them separately rather than treating the difference as a disagreement.

### 9.3 `missing` cannot distinguish absent from unreadable **[done]**

Both take the substitution. Excluding the unreadable one needs our own `ValuesSourceType`, which would leave every
aggregation unregistered against it — including plugin aggregations we cannot register — and would be bypassed whenever
`value_type` is given. The two behaviours differ only when `missing` is specified.

Note this only works at all because §3.1 returns `RAW`: `CoreValuesSourceType.NUMERIC.replaceMissing` parses the
substitute through `docValueFormat.parseDouble` before reading any document.

### 9.4 `doc['attributes.status']` stays unsupported **[done]**

One subscript rule, not two. `getScriptValues()` on the keyed leaf is nonetheless implemented as `Doubles`, because the
interface requires it and refusing is not expressible — so the subscript now returns numbers rather than the whole
column. That is a bug fix, but it is not advertised.

---

## 10. Test plan

**Done** — `objectGetByFieldId` agrees with `objectGet(name)` for every key at every depth, over eight documents
including unsorted keys, prefix-overlapping keys, non-ASCII and empty; fielddata agrees with `_source` on every path and
type in `RICH_DOC`; arrays expand ascending and recursively, skipping an object element; a key absent from a segment
serves nothing.

**Done, at the REST layer** — `93_flat_object_columnar_aggregation.yml`, nine cases over **three shards** so the
coordinator reduce and `DocValueFormat` serialisation are exercised (a single-shard test passes while the feature is
broken for every real cluster): metrics with no script, `terms` with and without `value_type`, a nested path, the
mixed-type partial answer with `value_count` as the signal, array expansion including `min`/`max` to catch ordering, a
descending sort with `numeric_type: long`, an absent path, and `docvalue_fields`.

**Open**

- **Multi-segment sort asserting full ordering**, not just the top hit. This is the only thing that would catch
  `sortRequiresCustomComparator()` regressing, and the Lucene reasoning behind it rests on a decompiled third-party
  class rather than a test.
- **Merge** — results identical before and after `forceMerge(1)`, since ordinals are reassigned by the merge and the
  ordinal-to-field-id invariant has to survive it. Needs a non-merging index helper; the existing one force-merges to
  one segment.
- **That `nextOrd()` returns ascending, deduplicated ordinals** asserted directly. The whole design rests on it and
  nothing currently pins it.
- **`missing`** with an absent path and an unreadable value, pinned so a future framework change is deliberate.
- **The version gate**, both ways. The version is baked into the field type at mapper build time, so the test must vary
  the `MapperService`, not the `IndexSettings`.
- **Native/script parity** through the rewritten IT.

---

## 11. Deferred, with the reason

- Per-path and per-view duplication of per-document blob work (§2.3) — measure first.
- Exact numeric width for aggregations (§4.2) — Phase 2's declared types.
- Strict coercion mode (§4.3).
- An exact, surfaced count of skipped values (§9.1) — the profile channel.
- Excluding unreadable values from `missing` substitution (§9.3) — needs framework support.
- Whether the lazy Map view's enumeration path is fast enough for Phase 3's `_source` reconstruction — unmeasured.

---

## 12. What changed from the first version, and why

| § | first version said | reality |
|---|---|---|
| 3.1 | *DocValueFormat unmentioned* | Asked for on every aggregation; the existing format fails four ways. Return `RAW`. |
| 6 | gate via `isAggregatable()` | Never consulted by aggregations; would have returned silently wrong numbers. Gate in `fielddataBuilder`, keep the override for the parent. |
| 3 | `sortRequiresCustomComparator` unmentioned | Must be `true`, or MIN/MAX sorts every document as missing, silently. |
| 5 | Map is what `getAll` produces | Needs every key name; both ways of getting them are slower than `_source`. Replaced by a lazy Map view. |
| 2, 8 | "no name table" / delete it | True only for per-path reads. Enumeration still needs names, so the table became a lazy `NameResolver`. |
| 2.2 | three ords per segment | n(n+1)/2 spans; and the choice among them is per document. |
| 2.1 | `Integer.compare` | `Integer.compareUnsigned`; and the ascending-id precondition is not a format guarantee. |
| 8 | write unconditionally | Newly rejects documents existing indices accept. Write is version-gated. |
| 8 | delete `variant_blob` | Would strand indices permanently (I11). Accepted and ignored. |
| 8 | `ValueCoercion` reused unchanged | Contradicted §9.2; its caller expands arrays. |
| 8, 10 | accessor becomes the `LeafFieldData` | Contradicted §10's third arm and breaks 15 call sites. Accessor stays as the oracle. |
| 9.1 | header propagates from data nodes | Silently dropped under the default concurrency. Needs a different channel. |
| 9.1 | leaf counters leak across requests | Only for ordinals fielddata; and the config *is* retained for the request. Right conclusion, wrong reasons. |
| 3 | `keyedFieldType` on the field type | It is on the mapper. |
| 8 | 178 existing tests | 148 under `server/src/test`. |
