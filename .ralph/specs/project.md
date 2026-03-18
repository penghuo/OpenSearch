# Parquet DocValues Format Specification

## Goal

Replace Lucene's native doc_values data format with Apache Parquet encoding in OpenSearch. When parquet format is enabled, `_source` is not stored — it is reconstructed from parquet columns at fetch time.

## Architecture

A custom `ParquetDocValuesFormat` plugs into Lucene's codec SPI via `PerFieldMappingPostingFormatCodec.getDocValuesFormatForField()`. Uses `parquet-column`/`parquet-encoding` libraries (no Hadoop dependencies) with Lucene I/O adapters bridging `IndexInput`/`IndexOutput` to Parquet streams. Gated by `index.codec.doc_values.format: parquet` index setting and a feature flag.

## Tech Stack

Java 21, Lucene 10.4, parquet-column 1.15.1, parquet-encoding 1.15.1, OpenSearch server module

## Requirements

### Functional
- All five Lucene doc_values types supported: `NUMERIC`, `SORTED_NUMERIC`, `BINARY`, `SORTED`, `SORTED_SET`
- `_source` not stored when parquet enabled — reconstructed from parquet columns at fetch time
- Applies to new indices only (no migration path)
- Activation via index setting: `index.codec.doc_values.format: parquet`
- Feature flag gated: `opensearch.experimental.feature.parquet_doc_values.enabled`
- Must pass Lucene's `BaseDocValuesFormatTestCase` conformance suite
- Segment merges produce new parquet files via standard `DocValuesConsumer.merge()`
- `_parquet_export` REST API exports standalone Parquet files for Spark/Trino/DuckDB

### Type Mapping
| Lucene DocValues Type | Parquet Type | Encoding |
|---|---|---|
| `NUMERIC` | `INT64` | Delta-binary-packed |
| `SORTED_NUMERIC` | Repeated `INT64` | Delta-binary-packed |
| `BINARY` | `BYTE_ARRAY` | Plain / delta-length |
| `SORTED` | `BYTE_ARRAY` | Dictionary + RLE |
| `SORTED_SET` | Repeated `BYTE_ARRAY` | Dictionary + RLE |

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

### Key Integration Points
- `PerFieldMappingPostingFormatCodec.getDocValuesFormatForField()` — returns `ParquetDocValuesFormat` when setting enabled
- `SourceFieldMapper.preParse()` — skips `_source` storage when parquet enabled
- `IndexShard` — wraps directory reader with `DerivedSourceDirectoryReader` for `_source` reconstruction

### Completion Criteria (opensearch-benchmark http_logs workload)
1. Correctness: 100% pass — all queries return correct results
2. Ingestion throughput: 2x improvement over baseline (doc_values + _source enabled)
3. Query performance: within 10% of baseline
4. Storage size: 70% reduction from baseline
