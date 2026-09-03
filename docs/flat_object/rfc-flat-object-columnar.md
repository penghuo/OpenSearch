# RFC: A Variant blob column for `flat_object`

**Status:** draft, for review
**Scope:** `flat_object` field mapper, `server`
**Prototype:** branch `flat-object-columnar-variant`, behind a mapping option that is off by default

Assumes familiarity with OpenSearch and with `flat_object`. This document explains what a Variant blob column is, how it
stores a document, how a value is read back out of it, and how to reach it from the query DSL.

---

## 1. Summary

A `flat_object` field can be filtered on but not *read*. The terms it writes are keyed by content, not addressable by path,
and they have already stringified the value — so anything that needs the actual value has to go back to `_source`.

This RFC proposes giving the field its own columnar store — a **Variant blob column** — so that reading a value does not
involve `_source` at all. The document's JSON is encoded following the [Apache Parquet
Variant](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) binary encoding and held in doc-values,
split across two columns:

```
attributes._blobnames    SORTED_SET   the segment's key-name dictionary,
                                      plus each document's names as a list of ordinals into it
attributes._blob         BINARY       the document's value tree, type-tagged,
                                      referring to names by position in that ordinal list
```

The column, not `_source`, is the field's store — the field need not appear in `_source` at all.

A read is a binary search to one path: no `_source`, no document parse, no bytes read outside that path. Types survive; an
integer comes back as an integer of the width it was written at.

**On Parquet.** This follows the Variant encoding but does not store a Parquet Variant value: the spec's self-contained
`(metadata, value)` pair is split, with the key dictionary in the name column rather than in every document. The `value`
half's byte layout is unchanged; `_blob` alone is not Parquet-decodable, and is not meant to be. See §3.

Behind a mapping option, off by default, adding no terms and changing no query plan.

---

## 2. The problem

`flat_object` writes three things for `{"attributes": {"status": 200, "k8s.namespace": "ns-01"}}`:

| | |
|---|---|
| `attributes` | path components, split on `.`, as terms and `SORTED_SET` doc-values |
| `attributes._value` | every leaf value, as a term |
| `attributes._valueAndPath` | every `path=value` pair, as a term |

Those make `{"term": {"attributes.status": "200"}}` work. None of them can answer "what is the value at
`attributes.status` in document 4,812?" — `_value` and `_valueAndPath` are term columns, keyed by content and addressed by
ordinal, and both hold strings.

So aggregating a subfield today means a derived field whose script reads `_source`:

```
def a = params._source.attributes;
if (a != null) { def x = a['status']; if (x != null && x instanceof Number) { emit(((Number)x).longValue()); } }
```

Per document that decompresses a stored-fields block, parses the entire document into a `Map`, and reads one entry from it.
Over a million documents of ~100 attributes each, a `sum` on one path takes **36.7 s**; a `terms` + `sum` group-by takes
**235 s** (Appendix A).

Cost tracks the document, not the request: reading one attribute costs parsing all hundred, and it worsens as filters narrow,
since a stored-fields block must be decompressed whole to reach any one document in it. The type is lost too — `_source`
returns whatever the JSON parser made of it. And with `_source` disabled there is nothing to read at all.

---

## 3. What a Variant blob column is

### 3.1 The Variant encoding

[Apache Parquet Variant](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) is a binary encoding for
schemaless, semi-structured data. A value is a pair of byte arrays:

```
metadata   <header> <dictionary_size> <offset>*(n+1) <key bytes>
             a dictionary of every field name in the value, optionally sorted

value      <value_metadata> <value_data>
             a type-tagged tree; objects refer to names by index into that dictionary
```

The properties that make it worth adopting here:

- **Type-tagged.** Each value carries its own type id, so `200` is stored as an `int16` and comes back as one. Strings under
  64 bytes fold their length into the header byte and cost one byte of overhead — which is most OTel attribute values.
- **Addressable without parsing.** An object stores its members ordered by key, with an offset table, so reaching a member
  is a binary search plus an offset computation. Nothing outside the path is touched.
- **Self-contained.** No schema, no mapping, no per-field declaration — the same property that makes `flat_object` useful.

This proposal adopts the **`value` half unchanged**: same header bit layout, same primitive type ids, same short-string
form, same object and array framing. A conforming Variant reader can decode it given the matching `metadata`.

### 3.2 A document, byte by byte

```json
{
  "resource":   {"service.name": "checkout", "service.version": "1.2.0", "host.name": "ip-10-0-0-7"},
  "attributes": {"http.method": "GET", "http.status_code": 200},
  "body":       {"message": "ok", "level": "INFO"}
}
```

Ten distinct key names across three nesting levels. Sorted by unsigned UTF-8 bytes — note that the ids say nothing about
nesting; `host.name` sits between two top-level keys:

| id | key |
|---|---|
| 0 | `attributes` |
| 1 | `body` |
| 2 | `host.name` |
| 3 | `http.method` |
| 4 | `http.status_code` |
| 5 | `level` |
| 6 | `message` |
| 7 | `resource` |
| 8 | `service.name` |
| 9 | `service.version` |

**`attributes._blobnames`** receives those ten names. Lucene deduplicates them against the segment's dictionary and gives the
document a bit-packed, ascending list of ordinals into it. The name text itself is stored once for the whole segment; the
document holds only the ordinals.

**`attributes._blob`** receives the value tree:

```
pos  0:  02                    object; is_large=0, field_id_size=1, field_offset_size=1
pos  1:  03                    num_elements = 3
pos  2:  00 01 07              field_ids     -> attributes, body, resource
pos  5:  00 0E 1D 41           field_offsets -> 0, 14, 29, and 65 = total size
                               values region therefore begins at pos 9

pos  9:    02 02  03 04  00 04 07              attributes, 2 members
pos 16:    0D 47 45 54                           "GET"     short string, length 3
pos 20:    10 C8 00                              200       int16 — int8 caps at 127

pos 23:    02 02  05 06  00 05 08              body, 2 members
pos 30:    11 49 4E 46 4F                        "INFO"
pos 35:    09 6F 6B                              "ok"

pos 38:    02 03  02 08 09  00 0C 15 1B        resource, 3 members
pos 47:    2D 69 70 2D 31 30 2D 30 2D 30 2D 37   "ip-10-0-0-7"
pos 59:    21 63 68 65 63 6B 6F 75 74            "checkout"
pos 68:    15 31 2E 32 2E 30                     "1.2.0"
```

## 4. Reading a value

Three tiers, at very different frequencies. Resolving `$.resource.service.version` from §3.2:

### Once per segment — read the name dictionary

`_blobnames` holds every distinct name in the segment, sorted, each with an ordinal. It is read once, in ordinal order, so
that afterwards any ordinal can be turned back into a name.

Ordinal order matters. Lucene keeps sorted terms in compressed blocks of sixteen, so reading them in sequence spends one
decompression per sixteen names — **~28 ns per name**, against **~14,000 ns** for the same names fetched in scattered order
That is why the dictionary is read in one pass up front rather than consulted a name at a time.

### Once per document — take its ordinal list and its value tree

`_blobnames` gives the document its own list of ordinals, ascending; `_blob` gives its value tree. **~100 ns per document.**

**That ordinal list is what joins the two columns.** Ordinals are assigned in name order, so a document's ordinals read
ascending *are* its names in sorted order — and the writer numbers field ids to match that same order. So a field id is a
position in the list:

```
field id  i   ->   entry i of the document's ordinal list   ->   the name at that ordinal
```

Nothing else has to be parsed to get there: no per-document dictionary header, no offset table over key bytes, and nothing
stored to connect a field id to a name beyond the ordinal list the name column had to record anyway.

### Per path — binary search the field ids

An object stores its members ordered by key, so finding one is a binary search over its `field_ids`. Each probe turns its
candidate field id into a name by the two steps above, and compares that name against the path segment being looked for.

```
root @ 0      field_ids [00 01 07]
              slot 1 -> id 01 -> "body"      < "resource"   -> go right
              slot 2 -> id 07 -> "resource"  hit
              field_offsets[2] = 29   ->  child at 9 + 29 = 38

resource @38  field_ids [02 08 09]
              slot 1 -> id 08 -> "service.name"     < target -> go right
              slot 2 -> id 09 -> "service.version"  hit
              field_offsets[2] = 21   ->  value at 47 + 21 = 68

value @68     0x15  ->  short string, length 5  ->  "1.2.0"
```

The `attributes` and `body` subtrees are never touched, `_source` is never decompressed, and no other column is opened.

---

## 5. Reaching it from the DSL

### 5.1 Enabling the column

One mapping option, off by default:

```json
PUT /logs
{
  "mappings": {
    "properties": {
      "@timestamp":  { "type": "date" },
      "attributes":  { "type": "flat_object", "variant_blob": true }
    }
  }
}
```

The column is now the field's store, so `_source` need not carry the field. Excluding it is the intended configuration —
leaving it in means storing the same values twice, in two formats:

```json
  "_source": { "excludes": ["attributes"] },
```

Excluding one field leaves `_source` enabled for the rest of the document, which is what the rest of the search API expects.
Disabling `_source` entirely is a different matter: the columns write and read perfectly well without it, but derived fields
are currently rejected outright on a `_source`-disabled index regardless of what their script reads — see §5.4.

**What exclusion costs.** An excluded field is gone from `_source` output: it will not appear in a `GET` by id or in search
hits, and reindex and update-by-query will not carry it. The values are all still there in the column, but nothing today
serves `_source` *from* the column — so a deployment that needs the original document back has to keep the field in `_source`
and accept storing it twice. Making the blob able to reconstruct the field's `_source` would remove that trade-off entirely
and is not part of this proposal.

### 5.2 Filtering is unchanged

The blob adds nothing to the inverted index, so every existing query works and costs exactly what it did:

```json
GET /logs/_search
{
  "query": { "bool": { "filter": [
      { "term":  { "attributes.http.status_code": "200" } },
      { "range": { "@timestamp": { "gte": "now-1h" } } } ] } }
}
```

### 5.3 Aggregating

The blob is exposed to Painless as `variant(<field>)`, with typed accessors by path. A derived field turns a path into an
aggregatable field:

```json
PUT /logs
{
  "mappings": {
    "_source": { "excludes": ["attributes"] },
    "properties": {
      "attributes": { "type": "flat_object", "variant_blob": true }
    },
    "derived": {
      "status": { "type": "long", "script": { "lang": "painless", "source":
        "def v = variant('attributes'); if (v != null) { def x = v.getLong('http.status_code'); if (x != null) { emit(x); } }" } },
      "namespace": { "type": "keyword", "script": { "lang": "painless", "source":
        "def v = variant('attributes'); if (v != null) { def x = v.getString('k8s.namespace'); if (x != null) { emit(x); } }" } }
    }
  }
}
```

```json
GET /logs/_search
{
  "size": 0,
  "query": { "range": { "@timestamp": { "gte": "now-1h" } } },
  "aggs": {
    "by_namespace": {
      "terms": { "field": "namespace", "size": 20 },
      "aggs": { "avg_status": { "avg": { "field": "status" } } }
    }
  }
}
```

`getLong` / `getDouble` / `getString` / `getBoolean` coerce through the same table as reading `_source`, so moving an
existing derived field from `params._source` to `variant(...)` does not change results. A path resolving to an object or
array, asked for as a scalar, yields no value either way.

### 5.4 The ergonomics gap this leaves

**Requiring a derived field to reach a columnar value is poor ergonomics.** The natural request is

```json
{ "aggs": { "total": { "sum": { "field": "attributes.http.status_code" } } } }
```

with nothing declared at all. That means a `ValuesSource` over the blob, keyed by path, plus a decision about how the type
is settled — declared per path, inferred per document, or coerced. It is the obvious next step, and this proposal is its
prerequisite, but it is a separate design.

Two related gaps, both stated so a reviewer is not surprised by them:

- **`fields` retrieval is broken on `flat_object` today, independently of this RFC.**
  `{"fields": ["attributes.http.method"]}` ignores the subpath and returns the whole object as a Java `Map.toString()` —
  `=` instead of `:`, unquoted, not parseable as JSON. The blob can serve it correctly, but "correctly" needs a decision
  first: typed values (`200`) as the blob knows them, or strings (`"200"`) as the rest of `flat_object` produces.
- **`_source`-disabled indices.** The blob is independent of `_source` on disk, but `DerivedFieldType` rejects *any* derived
  field on a `_source`-disabled index, even a script that only touches the blob. That guard needs relaxing before "works
  where `_source` cannot" is reachable.

---

## Appendix A. Benchmark results

### A.1 What was compared

Two index configurations over byte-identical documents, differing only in where the aggregation gets its value from.

| | how the value is read |
|---|---|
| **`_source`** | `flat_object`, `_source` enabled. Script: `def a = params._source.attributes; ... a['status'] ...` |
| **Variant blob** | `flat_object` with `variant_blob: true`, `_source` excludes `attributes`. Script: `def v = variant('attributes'); ... v.getLong('status') ...` |

Same derived-field names and types in both, same query bodies, same terms. The `_source` configuration is not a strawman:
it is what `SourceLookup` and derived fields do today, and there is no other way to aggregate a `flat_object` subfield.
Optimising it — a lazier `_source` reader — is separate work and was not attempted.

**Setup.** 16 vCPU, 61 GiB RAM, NVMe. OpenSearch 3.6.0-SNAPSHOT, Lucene 10.5.0, 16 GiB heap. One shard, no replicas,
force-merged to a single segment and verified. Queries: `request_cache=false`, **n=5** timed iterations after 2 warmups,
server-side `took`, p50. Fresh JVM per configuration for the read phase. Concurrent segment search left at its default, so
a query is partitioned within the single segment — both configurations equally.

### A.2 The corpus

10,000 services emitting OpenTelemetry-shaped log records. Each service always emits the same attribute keys, and **most of
those keys belong to that service alone** — which is what makes the field's vocabulary large.

```
per document    4 stable keys        status, duration_ns, level, k8s.namespace  (in every document)
              + ~20 shared keys      from a 1,000-name pool of real OTel convention names, by Zipf rank
              + 76 private keys      owned by that one service
              = ~100 attributes

values          40% string, 25% integer, 15% double, 8% boolean, 4% null, 4% array,
                4% nested object (2-3 fields each)
```

Measured from the finished index rather than assumed from the configuration:

| | measured |
|---|---|
| documents | 1,000,000 |
| distinct key names per document | **99.46** (min 89, max 107) |
| **distinct key names in the segment** | **761,007** |
| `attributes` JSON per document | 2,785 B |
| whole document | 2,846 B |

A **second, narrower corpus** is used in A.5 to show how the results move with document width: same generator, but ~16
attributes per document all drawn from the shared pool, so its vocabulary is **1,007 names**, at 10,000,000 documents. Its
`attributes` JSON is 441.5 B/doc.

### A.3 The query set

Nine queries, both configurations receiving identical bodies — only the script inside the derived field differs. Scoping is a
`range` inside the `query` clause, not a `post_filter`, which would leave the aggregation scanning everything while appearing
to scope it.

```json
Q1  sum on one always-present path                                        1,000,000 docs
{"size":0,"aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q2  Q1 scoped by time range to 10% of the corpus                            100,000 docs
{"size":0,"query":{"range":{"@timestamp":{"lt":1755820000000}}},
 "aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q3  Q1 scoped to 1%                                                          10,000 docs
{"size":0,"query":{"range":{"@timestamp":{"lt":1755730000000}}},
 "aggs":{"total":{"sum":{"field":"attr_status"}}}}

Q4  terms + sum group-by -- reads two paths per document                   1,000,000 docs
{"size":0,"aggs":{"by_ns":{"terms":{"field":"attr_namespace","size":32},
 "aggs":{"total":{"sum":{"field":"attr_status"}}}}}}

Q5  Q4 scoped to 10%                                                        100,000 docs
{"size":0,"query":{"range":{"@timestamp":{"lt":1755820000000}}},
 "aggs":{"by_ns":{"terms":{"field":"attr_namespace","size":32},
 "aggs":{"total":{"sum":{"field":"attr_status"}}}}}}

Q6  sum on a path present in 13.21% of documents                           1,000,000 docs
{"size":0,"aggs":{"total":{"sum":{"field":"attr_sparse"}}}}

Q7  sum on a path present in 1.30% of documents                            1,000,000 docs
{"size":0,"aggs":{"total":{"sum":{"field":"attr_rare"}}}}

Q8  five metrics on one path -- one read or five?                          1,000,000 docs
{"size":0,"aggs":{"s":{"sum":{"field":"attr_status"}},"a":{"avg":{"field":"attr_status"}},
 "mn":{"min":{"field":"attr_status"}},"mx":{"max":{"field":"attr_status"}},
 "c":{"value_count":{"field":"attr_status"}}}}

Q9  sum on two different paths                                            1,000,000 docs
{"size":0,"aggs":{"s1":{"sum":{"field":"attr_status"}},
 "s2":{"sum":{"field":"attr_sparse"}}}}
```

The derived fields resolve to these paths: `attr_status` → `status`, `attr_namespace` → `k8s.namespace`,
`attr_sparse` → `process.runtime.name`, `attr_rare` → `custom.tenant.attr_248`.

### A.4 Storage

Total index size per document, from the single force-merged segment.

| | `_source` | Variant blob | delta |
|---|---|---|---|
| bytes per document | 5,338.59 | **5,228.27** | **−110.32 (−2.07%)** |

### A.5 Query latency

Server-side `took`, p50, milliseconds.

| Q | `_source` | **Variant blob** | speedup |
|---|---|---|---|
| Q1 | 36,736 | **178** | **206×** |
| Q2 | 11,109 | **79** | 141× |
| Q3 | 1,095 | **25** | 44× |
| Q4 | 234,574 | **1,276** | **184×** |
| Q5 | 22,799 | **147** | 155× |
| Q6 | 36,821 | **177** | 208× |
| Q7 | 37,079 | **188** | 197× |
| Q8 | 37,328 | **363** | 103× |
| Q9 | 36,896 | **262** | 141× |

### A.6 Ingestion

| | `_source` | Variant blob |
|---|---|---|
| throughput, steady window | 3,914.06 docs/s | **3,973.84 docs/s** (+1.5%) |
| bulk p50 / p99 | 1,303.98 / 27,157.52 ms | 1,848.98 / 21,320.23 ms |
| force merge to one segment | 260.09 s | **420.11 s** (+61%) |
| young GC during indexing | 100 collections, 2,089 ms | 139 collections, 2,975 ms |

**Throughput is not distinguishable** — +1.5% is well inside the 11–19% run-to-run spread this harness shows, and bulk p50
and p99 disagree on which is ahead. Two costs that are real: force merge is 61% longer (two more doc-values columns, and a
761,007-entry dictionary to union and remap), and indexing allocates more (encoding the value tree and ordering its field
ids).

### A.7 Memory and GC

Allocation from `ThreadMXBean` counters rather than heap deltas, so a collection mid-run cannot hide anything. 100,000
documents, one path.

| | `_source` | **Variant blob** | ratio |
|---|---|---|---|
| allocated binding the segment | 3,808 B | **27,415,808 B** | 7,200× more |
| time binding the segment | 0.02 ms | **21.62 ms** | 1,000× more |
| **allocated per document read** | **262,801 B** | **491 B** | **535× less** |
| latency per document read | 105,079 ns | **503 ns** | **209× faster** |
| over a 1,000,000-document scan | **262 GB** | **0.49 GB** | 535× less |

Reading `_source` allocates **262 KB to extract one `long` from a 2,846-byte document** — 92× the document's own size.
Beyond "decompress a block, parse the document into a map", that figure is not further attributed.

GC over the query phase, from the node's collector counters:

| | young collections | young GC time |
|---|---|---|
| `_source` | **1,264** | **4,253 ms** |
| Variant blob | **11** | **194 ms** |

Note what this does *not* say: 4,253 ms against 454 s of query time is under 1%, so **GC pauses are not why reading
`_source` is slow**. The 262 GB of allocation is CPU work regardless.
