# Design: Storing the JSON value in `_source` (A) vs a Variant blob column (B)

## Status

Draft — design for [opensearch-project/sql#5704](https://github.com/opensearch-project/sql/issues/5704).

## Authors

Peng Huo

---

## 1. Goal

Decide where to keep the type-preserving copy of a `map<string, AnyValue>` value:

- **Solution A** — reuse `_source` (the JSON already stored per document).
- **Solution B** — store the value as a **Variant-encoded binary blob in a `BinaryDocValues`
  column**.

This design implements **both** behind an identical, script-based value-access path on a
`flat_object` field, so they can be measured head-to-head. The purpose is to **verify the A-vs-B
pros/cons below actually hold**; the test plan turns each row into a check.

## 2. Scope

- **Only the value store** (A vs B) is under design. Filtering is provided by `flat_object` and is
  **identical** in both arms — it is not the variable.
- No new field type; no changes to the inverted index; the value is read through a script in both
  arms.

## 3. Hypothesis — the pros/cons to verify

| Dimension | A: `_source` | B: Variant blob column | Verified by |
|---|---|---|---|
| Read granularity | whole document | only the `attributes` bytes | P (latency vs doc size / attributes fraction) |
| Physical form | stored fields (row, block-compressed) | `BinaryDocValues` (columnar, per-docID) | P (read & aggregation latency) |
| Get one path | decompress block + full JSON parse + scan | read blob + binary-search key + slice | P (point-read latency) |
| Type fidelity | JSON text (int/float/precision ambiguity) | explicit type tags + width | C (A≡B round-trip; record any divergence) |
| Extra storage | none (reuses `_source`) | extra copy (unless synthetic `_source`) | P (index size) |
| New code | ~none | Variant codec | qualitative |
| `_source` disabled | unavailable | still works | C (functional) |
| Write cost | none extra | encode + write column | P (index throughput) |

## 4. Common setup (identical in both arms)

- Field mapped as `flat_object` — same terms, same filtering, both arms.
- Input: the same documents (JSON / OTLP `KeyValueList`).
- **Value access via a script/derived accessor**: `get(path, type)` returns the typed value at a
  path. The aggregation/query layer consumes it identically, so **A and B must return the same
  results**. Only the accessor's backing store differs.

```
stats sum(get(attributes, "status", long))
where get(attributes, "level", string) = "info"     -- filtering also available via flat_object terms
fields get(attributes, "duration_ns", long)
```

## 5. Solution A — value in `_source`

- Nothing extra is stored; the value lives in `_source` as today.
- The accessor reads `_source` for the document, parses the JSON, navigates to `path`, and casts to
  `type` (this is the existing derived-field / scripted-field path).
- Cost model: per access, the stored-fields block is decompressed and the **whole document** JSON is
  parsed to reach one path.

## 6. Solution B — value in a Variant blob column

- At index time, the `attributes` value is encoded to **Variant binary** (`metadata` key
  dictionary + type-tagged `value` tree) and written to a dedicated `BinaryDocValues` column
  (`attributes.__blob`). AnyValue maps directly to Variant types, so `200` (int) and `"200"`
  (string) carry different type tags → type conflict is lossless.
- The accessor reads the doc's blob from the column, binary-searches `path` in the Variant metadata,
  jumps to its offset, and decodes to `type` (subtree access is a slice, not a full re-parse).
- Cost model: per access, only the **`attributes` bytes** are read; path lookup is O(log k) + slice.
- The `attributes` value uses only its own blob bytes — independent of `_source` (works with
  `_source` disabled).

## 7. Controlled variable

A and B share the same field, terms, inputs, accessor API, and query/aggregation layer. **The single
difference is the value store** — `_source` JSON (A) vs Variant `BinaryDocValues` (B). Any measured
difference in latency, size, throughput, or fidelity is therefore attributable to that choice, which
is what lets the comparison verify the table.

> Note: B bundles two changes vs A — columnar location **and** Variant format. The comparison
> verifies "B as defined" vs "A as defined". An optional third arm (raw JSON stored in a binary
> column) could isolate location from format if the results warrant it.

## 8. Non-goals

- No shredding, no typed columns, no discriminator, no star-tree.
- No new field type; `flat_object` is used as-is.
- No cross-engine read of the blob bytes.
- No change to filtering semantics.
