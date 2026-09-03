# flat_object value stores: adding the doc-values baseline, and the cost of the column

Everything below was measured on 2026-09-02 on one node, one session, in response to a single question: the
comparison so far had `_source` as the only existing way to read a `flat_object` attribute back — is that true?

It is not. There is a second route that works today, and it is faster than `_source`. This run measures it, and
along the way corrects three things I had asserted without measuring.

## 1. Setup

Node started on the existing benchmark data directory, so `svc10k-a` and `svc10k-variant-enc` are the *same
indices* the 2026-08-26 run measured — same corpus, same segments, force-merged to one segment each.

```
OpenSearch 3.6.0-SNAPSHOT, 16 GiB heap, 16 vCPU / 61 GiB / NVMe
1,000,000 documents, 10,000 services, 99.46 distinct attribute names per document,
761,007 distinct names in the field, 2,785 B of attributes JSON per document
request_cache=false, p50 of 3 timed iterations after 1 warmup, server-side `took`
concurrent segment search off (this node's default; see §5)
```

All three routes were checked to return the **identical aggregation value** on every query, so any latency
difference is the store and not the answer.

## 2. The three routes

| route | how the script reads one attribute |
|---|---|
| **`_source`** | `def a = params._source.attributes; a['status']` |
| **`doc[]` prefix-match** | `doc['attributes._valueAndPath']` holds every `path=value` pair; scan and prefix-match, then `Long.parseLong` |
| **Variant blob** | `variant('attributes').getLong('status')` |

The `doc[]` script breaks out of the scan on first match, which is the most favourable reading of that route:
the scan stops halfway on average instead of always running the document's full key set.

```painless
def vs = doc['attributes._valueAndPath'];
String p = 'attributes.attributes.status=';
for (v in vs) {
  String s = String.valueOf(v);
  if (s.startsWith(p)) { emit(Long.parseLong(s.substring(p.length()))); break; }
}
```

## 3. Query latency

Q1 — `{"size":0,"aggs":{"total":{"sum":{"field":"attr_status"}}}}`, one always-present path, 1M documents.
Sum agreed at `312739473` in all three.

| route | p50 | vs blob | 2026-08-26 run |
|---|---|---|---|
| `_source` | 39,991 ms | 121× | 36,736 |
| **`doc[]` prefix-match** | **27,360 ms** | **83×** | not measured |
| Variant blob | 331 ms | 1× | 178 |

Q7 — same shape on a path present in 1.30% of documents. Sum agreed at `-43176717` in all three.

| route | p50 | vs blob | 2026-08-26 run |
|---|---|---|---|
| `_source` | 39,708 ms | 110× | 37,079 |
| **`doc[]` prefix-match** | **24,345 ms** | **68×** | not measured |
| Variant blob | 360 ms | 1× | 188 |

**`doc[]` is 1.46–1.63× faster than `_source`.** It is the strongest existing baseline, and the earlier
comparison used the weaker one. Against it the blob column is **68–83×**, not 121×.

`doc[]` does not degrade on the rare path — it is slightly *faster* there (24.3 s against 27.4 s), because it
never finds a match and so never parses a number.

Reproduction against the 2026-08-26 run: `_source` lands within 9% on both queries. The blob is ~1.9× slower
today (331 against 178). Not isolated; page-cache state is the likely cause, since the earlier run queried an
index it had just written. The ratios are what this run supports, not the absolute figures.

## 4. What the column costs when `_source` keeps the field

Two fresh indices, the same 100,000 documents from the same generator, both with `_source` enabled, differing
only in whether the blob is written.

| | bytes/doc |
|---|---|
| plain `flat_object` | 5,587.1 |
| `+ variant_blob` | 7,054.6 |
| **delta** | **+1,467.5 (+26.3%)** |

The 2026-08-26 run reported index size going *down* 2.07% with the blob enabled. That was net of excluding the
field from `_source`. Enabling the column without also dropping the field from `_source` costs +26.3% on this
corpus — which is attribute-dominated (2,785 B of attributes in a 2,846 B document), so it is the high end of
the range.

## 5. Three things I had asserted without measuring, and what is actually true

**`doc['attributes.status']` does not read that path.** It ignores the key in the subscript and returns every
attribute in the document, byte-identical to `doc['attributes._valueAndPath']`:

```
doc['attributes']              -> [attributes.200, attributes.info, attributes.ns-1]        (values, no keys)
doc['attributes.status']       -> [attributes.attributes.k8s.namespace=ns-1,
                                   attributes.attributes.level=info,
                                   attributes.attributes.status=200]                        (everything)
```

So there is no per-path read through `doc[]` today, and the value arrives as text. Giving that subscript correct
per-path semantics would fix a defect rather than break an interface.

**Unused derived fields in a mapping cost nothing.** I suspected derived fields were evaluated eagerly. Adding
four unused derived fields to a mapping and re-running the identical query: 11,840 / 11,898 / 12,121 ms against a
12,001 ms clean baseline. No effect.

**Concurrent segment search does not explain the gap to the earlier run.** Setting
`index.search.concurrent_segment_search.mode=auto` on both indices changed nothing (blob 1,191 against 1,193;
`_source` 127,989 against 123,632). The 2026-08-26 report's claim that queries were partitioned within the
segment does not hold on this node, where the effective default is `none`.

The actual cause of that gap was **my own query**, not the configuration: I had added a `range` clause and
divided by `hits.total`, which the default `track_total_hits` caps at 10,000. The range matched all 1,000,000
documents. Corrected, `_source` is 39,991 ms without the clause and 123,632 ms with it — a 3.1× penalty from
stored-fields block locality when the collector walks a query-produced doc-id set instead of scanning. The blob
route is unaffected by the same clause.

## 6. Search-time versus mapping-defined derived fields

| blob route, Q1, 1M documents | p50 |
|---|---|
| derived field declared in the mapping | 331 ms |
| same script passed in the search body | 1,193 ms |

**3.6×**, for an identical script over an identical index. Consistent with the per-instance name-table bind
already recorded as a limitation: the accessor is cached per thread per field rather than per segment, so
anything that multiplies script instances multiplies the 21.62 ms / 27.4 MB bind. Not isolated further.

## 7. What this changes

- The claim that `_source` is the only way to read a `flat_object` value back is false, and any write-up
  repeating it needs correcting.
- The headline against the strongest existing baseline is 68–83×, not 121× or 206×.
- The conclusion does not change. `doc[]` cannot address a path — it returns the whole column and the caller
  string-matches — so it is a full per-document scan with text parsing. That is why it stays two orders of
  magnitude behind a column that binary-searches one key and decodes one typed value.
- Absolute figures from the 2026-08-26 run should be re-measured before being quoted anywhere durable. The
  ratios within a single session are the trustworthy part.
