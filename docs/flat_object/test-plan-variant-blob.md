# Test Plan: `_source` (A) vs Variant blob column (B)

## Status

Draft — validates [design-variant-blob.md](./design-variant-blob.md) for
[opensearch-project/sql#5704](https://github.com/opensearch-project/sql/issues/5704).

## Authors

Peng Huo

---

## Scope

Compare **Solution A** (value in `_source`) and **Solution B** (value in a Variant blob column),
both read through the same script accessor on a `flat_object` field. The plan's purpose is to
**verify each row of the pros/cons table** in the design.

- **Correctness**: A and B must produce **exactly the same results**. Any divergence is itself a
  finding (the type-fidelity row).
- **Performance**: A and B differ **only** in how the JSON value is stored (`_source` vs blob), so
  the performance tests target exactly that — value read, aggregation, index size, write throughput.

Numeric thresholds are **targets to ratify** once baselines exist.

---

## 1. Correctness — A ≡ B

Same documents indexed both ways; values read via the script accessor (from `_source` for A, from
the blob for B). Implemented with `OpenSearchIntegTestCase` / `yamlRestTest`; encode/decode with
JUnit + randomized testing.

| # | Input / query | Assert |
|---|---|---|
| C1.1 | `get(attributes, path, type)` for each value type: int, double, string, bool, null, array, nested object, dotted key | **A result == B result**, value and type. |
| C1.2 | Reconstruct the whole `attributes` value (A vs B) | deep-equals each other and the original. |
| C1.3 | `stats sum/avg/min/max/count(get(...))` over a dataset | **A aggregate == B aggregate**. |
| C1.4 | `stats ... by get(attributes, "k8s.namespace", string)` | group-by results identical A vs B. |
| C1.5 | Filtering via `flat_object` terms (sanity — identical in both) | same result set A vs B. |
| C1.6 | Mixed-type path (int + `"OK"`); numeric-string coercion on/off | identical values, identical excluded/coerced counts A vs B. |

### Type fidelity (where A may diverge — this validates the fidelity row)

| # | Input | Assert |
|---|---|---|
| C2.1 | `200` (int) vs `200.0` (double) vs `2e2` on the same path | B preserves the original type/width; **record whether A (JSON text) diverges** — divergence confirms A's fidelity con. |
| C2.2 | `int64` near max, big integer > `int64`, `-0.0`, leading-zero strings, `"200"` | B round-trips exactly (or documented fallback); A behavior recorded. |

### Functional

| # | Input | Assert |
|---|---|---|
| C3.1 | Index with `_source` **disabled** | **B still returns values; A cannot** — confirms the dependency row. |
| C3.2 | Property test: random AnyValue tree → Variant encode → decode | equals original; no crash (B codec correctness). |

---

## 2. Performance — the value-store difference only

### 2.1 Metrics

Point-read latency (`get` one path); aggregation latency (scan N matching docs); index size
(`_source` vs blob column, and total); index throughput (docs/s); heap during aggregation.

### 2.2 Datasets

Synthetic OTel generator with knobs: document size, **`attributes` fraction of the document**, keys
per document, value size, type mix, nesting depth; plus a real OTel log sample. Scales: 10M and
100M docs.

### 2.3 Experiments — each maps to a pros/cons row

| # | Experiment | Verifies (table row) | Target (to ratify) |
|---|---|---|---|
| P1 | Point-read latency `get(path)`, A vs B, sweep document size and `attributes` fraction | read granularity; get-one-path | B ≥ 2× faster when `attributes` is a small fraction of a large doc; ≥ parity when the doc ≈ `attributes`. |
| P2 | Aggregation latency `sum(get(...))` over N matching docs, A vs B | physical form (row vs columnar) | B faster; gap widens with N. |
| P3 | Index size: A (`_source` only) vs B (`_source` + blob; and B with synthetic `_source`) | extra storage | B-with-source ≤ ~2× A on the attributes bytes; B-with-synthetic-source ≤ ~1× A. |
| P4 | Index throughput, A vs B | write cost | B within ~20% of A. |
| P5 | Scaling: latency vs #docs, doc size, `attributes` fraction | read granularity (quantified) | B latency ∝ attributes bytes; A latency ∝ whole-doc bytes. |
| P6 | Heap during aggregation, A vs B | physical form | B ≤ A (no whole-doc materialization). |

No shredding / typed-column arm appears here — the comparison is strictly A vs B.

---

## 3. Verification matrix

| Pros/cons row | Test(s) | Confirms if |
|---|---|---|
| Read granularity | P1, P5 | B latency tracks `attributes` bytes; A tracks whole-doc bytes |
| Physical form (row vs columnar) | P2, P6 | B lower aggregation latency & heap |
| Get one path (parse vs slice) | P1 | B point-read faster, esp. large objects |
| Type fidelity | C2 | B preserves types A loses (divergence recorded) |
| Extra storage | P3 | B adds a copy unless synthetic `_source` |
| `_source` disabled | C3.1 | B works, A fails |
| Write cost | P4 | B throughput lower by the encode+column cost |
| Functional equivalence | C1 | A and B return identical results |

---

## 4. Test infrastructure

- **Unit / fuzz**: Variant encode/decode, path extraction, casting (JUnit + randomized testing;
  JMH for encode/decode and single-path decode microbench).
- **Integration**: `OpenSearchIntegTestCase` for A≡B result equivalence and aggregation; index with
  `_source` on and off (C3.1).
- **REST/API**: `yamlRestTest` for the accessor and query results.
- **Benchmarks**: OpenSearch-benchmark with the synthetic generator + real sample; sizes from
  `_cat/segments` / `_stats`.

---

## 5. Exit criteria

- **Correctness**: C1 shows A ≡ B for all values, aggregations, and filters; C2 documents every
  fidelity divergence (each divergence confirms the fidelity row); C3 passes.
- **Performance**: P1–P6 produce a **confirm/refute verdict for every row** of the verification
  matrix, with ratified thresholds.
- **Outcome**: a filled-in pros/cons table backed by measurements — the decision input for choosing
  A or B.
