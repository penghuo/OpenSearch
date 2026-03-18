# Parquet-backed DocValues Format for OpenSearch

## Goals

Replace Lucene's native doc_values data format with Apache Parquet encoding to achieve:

1. **Storage efficiency** — Parquet's columnar encodings (dictionary, RLE, delta-binary-packed) compress better than Lucene's native doc_values format.
2. **Query performance** — Leverage Parquet page-level min/max statistics for predicate pushdown in aggregations.
3. **Interoperability** — Enable external tools (Spark, Trino, DuckDB) to consume OpenSearch data via a Parquet export API.

## Scope

- Applies to **new indices only**. No migration path for existing indices.
- When parquet doc_values format is enabled, `_source` is **not stored**. It is reconstructed from parquet columns at fetch time.
- All five Lucene doc_values types supported from day one: `NUMERIC`, `SORTED_NUMERIC`, `BINARY`, `SORTED`, `SORTED_SET`.

## Activation

New index setting:

```
index.codec.doc_values.format: parquet   (default: "lucene")
```

Feature flag gated via `FeatureFlags` with `@LockFeatureFlag` for tests.

## Architecture

### Lucene Codec Integration

Lucene's `Codec` delegates columnar storage to `DocValuesFormat` via `getDocValuesFormatForField(field)`. OpenSearch's `PerFieldMappingPostingFormatCodec` overrides this method. Today it always returns `Lucene90DocValuesFormat`. With this change, when the index setting is enabled, it returns `ParquetDocValuesFormat` instead.

```
Lucene writes a field's doc_values
  → asks Codec: getDocValuesFormatForField("price")
    → index.codec.doc_values.format == "lucene":  returns Lucene90DocValuesFormat
    → index.codec.doc_values.format == "parquet": returns ParquetDocValuesFormat
```

No new Codec subclass is needed. The only change is in `PerFieldMappingPostingFormatCodec.getDocValuesFormatForField()`.

### Components

#### 1. `ParquetDocValuesFormat` (extends `DocValuesFormat`)

Lucene SPI entry point.

- `fieldsConsumer()` → returns `ParquetDocValuesWriter`
- `fieldsProducer()` → returns `ParquetDocValuesReader`
- File extensions: `.pdvd` (data), `.pdvm` (metadata)

#### 2. `ParquetDocValuesWriter` (extends `DocValuesConsumer`)

Receives all five doc_values types and encodes them using `parquet-column` / `parquet-encoding` primitives:

| Lucene DocValues Type | Parquet Type | Encoding |
|---|---|---|
| `NUMERIC` | `INT64` | Delta-binary-packed |
| `SORTED_NUMERIC` | Repeated `INT64` | Delta-binary-packed |
| `BINARY` | `BYTE_ARRAY` | Plain / delta-length |
| `SORTED` | `BYTE_ARRAY` | Dictionary + RLE |
| `SORTED_SET` | Repeated `BYTE_ARRAY` | Dictionary + RLE |

- Writes one Parquet column chunk per field per segment.
- Writes through Lucene's `IndexOutput` via `LuceneOutputFile` adapter.

#### 3. `ParquetDocValuesReader` (extends `DocValuesProducer`)

Reads Parquet column chunks and exposes them through Lucene's DocValues iterators:

- `NumericDocValues`, `SortedNumericDocValues`, `BinaryDocValues`, `SortedDocValues`, `SortedSetDocValues`
- Reads through Lucene's `IndexInput` via `LuceneInputFile` adapter.
- Leverages Parquet page-level min/max statistics for predicate pushdown.

#### 4. Lucene I/O Adapters

- `LuceneOutputFile` — adapts `IndexOutput` to Parquet's `PositionOutputStream`
- `LuceneInputFile` — adapts `IndexInput` to Parquet's `SeekableInputStream`

These keep all I/O within Lucene's `Directory` abstraction (local FS, remote store, etc.).

#### 5. `ParquetTypeMapping`

Centralized mapping between Lucene DocValues types and Parquet primitive types. Used by both writer and reader.

#### 6. `_source` Reconstruction

When parquet doc_values format is enabled:

- `SourceFieldMapper.preParse()` skips storing `_source` (same pattern as `derived_source`).
- At fetch time, `_source` is reconstructed by reading all parquet columns for the requested document and assembling JSON.
- Integrates with the existing `DerivedSourceDirectoryReader` pattern in `IndexShard`.
- `_update` API works via reconstructed `_source`.

#### 7. `_parquet_export` REST API

Read-only export endpoint that produces standalone Parquet files for external tools:

```
POST /my-index/_parquet_export?path=/shared/export/
```

- Reads parquet-encoded doc_values columns from segments via `ParquetDocValuesReader`.
- Writes proper standalone `.parquet` files with valid Parquet footers, row group metadata, and column statistics.
- External tools read directly:
  - Spark: `SELECT * FROM parquet.\`/shared/export/my-index/\``
  - DuckDB: `SELECT * FROM read_parquet('/shared/export/my-index/*.parquet')`
- For remote store indices, export can write directly to S3.

### Data Flow

```
Index time:
  Document → field mappers → doc_values fields
    → ParquetDocValuesWriter
      → parquet-column encoders (RLE, dict, delta)
        → IndexOutput → .pdvd/.pdvm files
  _source → NOT stored (skipped in SourceFieldMapper.preParse)

Search/Aggregation time:
  Aggregation framework → DocValues iterators
    → ParquetDocValuesReader
      → parquet-column decoders
        → IndexInput ← .pdvd/.pdvm files

Fetch time (_source reconstruction):
  FetchPhase → DerivedSourceDirectoryReader
    → ParquetDocValuesReader (read all columns for doc)
      → reconstruct JSON _source
```

### Merge Behavior

Segment merges work through Lucene's `DocValuesConsumer.merge()` default implementation — reads doc_values from old segments via `ParquetDocValuesReader`, writes to new segment via `ParquetDocValuesWriter`. New Parquet files are produced; old ones are deleted with the merged segments.

### Parquet Library Dependency

Use `parquet-column` and `parquet-encoding` modules from Apache `parquet-mr`. Avoid `parquet-hadoop` and its Hadoop transitive dependencies. Bridge I/O through Lucene's `Directory`/`IndexInput`/`IndexOutput` via the adapter classes.

### Package Structure

```
server/src/main/java/org/opensearch/index/codec/parquet/
  ├── ParquetDocValuesFormat.java
  ├── ParquetDocValuesWriter.java
  ├── ParquetDocValuesReader.java
  ├── ParquetTypeMapping.java
  ├── LuceneOutputFile.java
  └── LuceneInputFile.java
```

## Testing

### Unit Tests

- `ParquetDocValuesFormatTests` — extend Lucene's `BaseDocValuesFormatTestCase` conformance suite.
- `ParquetDocValuesWriterTests` / `ParquetDocValuesReaderTests` — round-trip tests for all five doc_values types. Edge cases: empty fields, single-value, multi-value, large cardinality dictionaries, null values.
- `ParquetTypeMappingTests` — verify type mapping correctness.
- `LuceneOutputFileTests` / `LuceneInputFileTests` — verify I/O adapter correctness.

### Internal Cluster Tests

- `ParquetDocValuesIT` — end-to-end:
  - Create index with `index.codec.doc_values.format: parquet`
  - Index documents, verify aggregations (terms, sum, avg, date_histogram) return correct results
  - Verify sorting works correctly
  - Verify `_source` is reconstructed properly on fetch
  - Verify `_update` API works via reconstructed `_source`
  - Verify segment merges produce valid parquet files
  - Verify no `_source` stored field exists in segments

### Performance Benchmarks

Use `opensearch-benchmark` with the `http_logs` workload.

**Baseline:** Default settings (`doc_values: true`, `_source: enabled`, Lucene90 doc_values format)

**Parquet:** `index.codec.doc_values.format: parquet` (parquet doc_values, `_source` disabled/reconstructed)

Compare:
- **Ingestion throughput** — docs/sec, indexing latency p50/p99
- **Query performance** — latency for standard `http_logs` queries (terms aggs, date_histogram, range queries, sorting)
- **Storage size** — total index size on disk, segment-level breakdown
