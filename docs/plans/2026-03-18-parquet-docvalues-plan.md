# Parquet DocValues Format Implementation Plan

> **For Kiro:** Use `#executing-plans` for sequential tasks.

**Goal:** Replace Lucene's native doc_values format with Apache Parquet encoding, disabling `_source` storage and reconstructing it from parquet columns at fetch time.

**Architecture:** A custom `ParquetDocValuesFormat` plugs into Lucene's codec SPI via `PerFieldMappingPostingFormatCodec.getDocValuesFormatForField()`. Uses `parquet-column`/`parquet-encoding` libraries with Lucene I/O adapters. Gated by `index.codec.doc_values.format` index setting and a feature flag.

**Tech Stack:** Java 21, Lucene 10.4, parquet-column, parquet-encoding, OpenSearch server module

---

## Task 1: Feature Flag and Index Setting

**Files:**
- Modify: `server/src/main/java/org/opensearch/common/util/FeatureFlags.java`
- Modify: `server/src/main/java/org/opensearch/index/IndexSettings.java`
- Modify: `server/src/main/java/org/opensearch/common/settings/IndexScopedSettings.java`

**Step 1: Add feature flag**

In `FeatureFlags.java`, add after the `STREAM_TRANSPORT` block:

```java
public static final String PARQUET_DOC_VALUES = FEATURE_FLAG_PREFIX + "parquet_doc_values.enabled";
public static final Setting<Boolean> PARQUET_DOC_VALUES_SETTING = Setting.boolSetting(
    PARQUET_DOC_VALUES,
    false,
    Property.NodeScope
);
```

Register in `FeatureFlagsImpl` constructor map:

```java
put(PARQUET_DOC_VALUES_SETTING, PARQUET_DOC_VALUES_SETTING.getDefault(Settings.EMPTY));
```

**Step 2: Add index setting**

In `IndexSettings.java`, add after `INDEX_DERIVED_SOURCE_TRANSLOG_ENABLED_SETTING`:

```java
public static final String DOC_VALUES_FORMAT_PARQUET = "parquet";
public static final String DOC_VALUES_FORMAT_LUCENE = "lucene";

public static final Setting<String> INDEX_DOC_VALUES_FORMAT_SETTING = Setting.simpleString(
    "index.codec.doc_values.format",
    DOC_VALUES_FORMAT_LUCENE,
    value -> {
        if (DOC_VALUES_FORMAT_LUCENE.equals(value) == false && DOC_VALUES_FORMAT_PARQUET.equals(value) == false) {
            throw new IllegalArgumentException(
                "index.codec.doc_values.format must be [" + DOC_VALUES_FORMAT_LUCENE + "] or [" + DOC_VALUES_FORMAT_PARQUET + "]"
            );
        }
    },
    Property.IndexScope,
    Property.Final
);
```

Add field and accessor:

```java
private final boolean parquetDocValuesEnabled;

// In constructor, after derivedSourceEnabled initialization:
parquetDocValuesEnabled = DOC_VALUES_FORMAT_PARQUET.equals(scopedSettings.get(INDEX_DOC_VALUES_FORMAT_SETTING));

// Accessor:
public boolean isParquetDocValuesEnabled() {
    return parquetDocValuesEnabled;
}
```

**Step 3: Register in IndexScopedSettings**

In `IndexScopedSettings.java`, add after `INDEX_DERIVED_SOURCE_TRANSLOG_ENABLED_SETTING`:

```java
IndexSettings.INDEX_DOC_VALUES_FORMAT_SETTING,
```

**Step 4: Run precommit**

```bash
./gradlew :server:precommit
```

**Step 5: Commit**

```bash
git add -A && git commit -m "Add feature flag and index setting for parquet doc_values format"
```

---

## Task 2: Parquet Library Dependency

**Files:**
- Modify: `server/build.gradle`
- Create: `server/licenses/parquet-column-LICENSE.txt`
- Create: `server/licenses/parquet-column-NOTICE.txt`
- Create: `server/licenses/parquet-encoding-LICENSE.txt`
- Create: `server/licenses/parquet-encoding-NOTICE.txt`
- Create: `server/licenses/parquet-common-LICENSE.txt`
- Create: `server/licenses/parquet-common-NOTICE.txt`

**Step 1: Add dependency**

In `server/build.gradle` dependencies block, add:

```gradle
api 'org.apache.parquet:parquet-column:1.15.1'
api 'org.apache.parquet:parquet-encoding:1.15.1'
api 'org.apache.parquet:parquet-common:1.15.1'
```

**Step 2: Add license and notice files**

Copy Apache License 2.0 text to each `*-LICENSE.txt`. Copy Parquet NOTICE to each `*-NOTICE.txt`.

**Step 3: Generate SHAs**

```bash
./gradlew :server:updateSHAs
```

**Step 4: Verify**

```bash
./gradlew :server:check
```

**Step 5: Commit**

```bash
git add -A && git commit -m "Add parquet-column and parquet-encoding dependencies"
```

---

## Task 3: Lucene I/O Adapters

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/LuceneOutputFile.java`
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/LuceneInputFile.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/LuceneOutputFileTests.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/LuceneInputFileTests.java`

**Step 1: Write failing tests for LuceneOutputFile**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

public class LuceneOutputFileTests extends OpenSearchTestCase {

    public void testWriteAndPosition() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            IndexOutput out = dir.createOutput("test.pdvd", IOContext.DEFAULT);
            LuceneOutputFile adapter = new LuceneOutputFile(out);
            assertEquals(0, adapter.getPos());
            byte[] data = new byte[] { 1, 2, 3, 4 };
            adapter.write(data);
            assertEquals(4, adapter.getPos());
            adapter.close();
        }
    }
}
```

**Step 2: Run test to verify it fails**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.LuceneOutputFileTests" -Dtests.iters=1
```

Expected: FAIL — `LuceneOutputFile` class does not exist.

**Step 3: Implement LuceneOutputFile**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;

/**
 * Adapts Lucene's IndexOutput to Parquet's PositionOutputStream.
 */
public class LuceneOutputFile extends PositionOutputStream {
    private final IndexOutput output;

    public LuceneOutputFile(IndexOutput output) {
        this.output = output;
    }

    @Override
    public long getPos() throws IOException {
        return output.getFilePointer();
    }

    @Override
    public void write(int b) throws IOException {
        output.writeByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        output.writeBytes(b, off, len);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
```

**Step 4: Run test to verify it passes**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.LuceneOutputFileTests" -Dtests.iters=10
```

**Step 5: Write failing tests for LuceneInputFile**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

public class LuceneInputFileTests extends OpenSearchTestCase {

    public void testReadAndSeek() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[] { 10, 20, 30, 40, 50 };
            try (IndexOutput out = dir.createOutput("test.pdvd", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
            try (IndexInput in = dir.openInput("test.pdvd", IOContext.DEFAULT)) {
                LuceneInputFile adapter = new LuceneInputFile(in);
                assertEquals(5, adapter.getLength());
                byte[] buf = new byte[3];
                adapter.readFully(buf);
                assertEquals(10, buf[0]);
                assertEquals(20, buf[1]);
                assertEquals(30, buf[2]);
                adapter.seek(0);
                adapter.readFully(buf);
                assertEquals(10, buf[0]);
            }
        }
    }
}
```

**Step 6: Run test to verify it fails**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.LuceneInputFileTests" -Dtests.iters=1
```

**Step 7: Implement LuceneInputFile**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.IndexInput;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Adapts Lucene's IndexInput to Parquet's SeekableInputStream.
 */
public class LuceneInputFile {
    private final IndexInput input;

    public LuceneInputFile(IndexInput input) {
        this.input = input;
    }

    public long getLength() {
        return input.length();
    }

    public void seek(long pos) throws IOException {
        input.seek(pos);
    }

    public void readFully(byte[] buf) throws IOException {
        input.readBytes(buf, 0, buf.length);
    }

    public void readFully(byte[] buf, int off, int len) throws IOException {
        input.readBytes(buf, off, len);
    }

    public SeekableInputStream asSeekableInputStream() {
        return new DelegatingSeekableInputStream(new IndexInputStream(input)) {
            @Override
            public long getPos() throws IOException {
                return input.getFilePointer();
            }

            @Override
            public void seek(long newPos) throws IOException {
                input.seek(newPos);
            }
        };
    }

    private static class IndexInputStream extends InputStream {
        private final IndexInput input;

        IndexInputStream(IndexInput input) {
            this.input = input;
        }

        @Override
        public int read() throws IOException {
            return input.readByte() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            input.readBytes(b, off, len);
            return len;
        }
    }
}
```

**Step 8: Run test to verify it passes**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.LuceneInputFileTests" -Dtests.iters=10
```

**Step 9: Commit**

```bash
git add -A && git commit -m "Add Lucene I/O adapters for Parquet streams"
```

---

## Task 4: ParquetTypeMapping

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetTypeMapping.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetTypeMappingTests.java`

**Step 1: Write failing test**

```java
package org.opensearch.index.codec.parquet;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.opensearch.test.OpenSearchTestCase;

public class ParquetTypeMappingTests extends OpenSearchTestCase {

    public void testNumericMapping() {
        PrimitiveType type = ParquetTypeMapping.numericType("price");
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64, type.getPrimitiveTypeName());
        assertEquals("price", type.getName());
        assertEquals(Type.Repetition.REQUIRED, type.getRepetition());
    }

    public void testBinaryMapping() {
        PrimitiveType type = ParquetTypeMapping.binaryType("payload");
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.getPrimitiveTypeName());
    }

    public void testSortedMapping() {
        PrimitiveType type = ParquetTypeMapping.sortedType("status");
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.getPrimitiveTypeName());
    }
}
```

**Step 2: Run test to verify it fails**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetTypeMappingTests" -Dtests.iters=1
```

**Step 3: Implement ParquetTypeMapping**

```java
package org.opensearch.index.codec.parquet;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Maps Lucene DocValues types to Parquet primitive types.
 */
public final class ParquetTypeMapping {

    private ParquetTypeMapping() {}

    public static PrimitiveType numericType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
    }

    public static PrimitiveType binaryType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
    }

    public static PrimitiveType sortedType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
    }
}
```

**Step 4: Run test to verify it passes**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetTypeMappingTests" -Dtests.iters=10
```

**Step 5: Commit**

```bash
git add -A && git commit -m "Add Parquet type mapping for Lucene DocValues types"
```

---

## Task 5: ParquetDocValuesWriter

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesWriter.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetDocValuesWriterTests.java`

This is the core writer. It extends `DocValuesConsumer` and encodes all five doc_values types into Parquet column chunks using `parquet-column` encoding primitives. Each field is written as a separate column chunk to the `.pdvd` data file, with metadata written to `.pdvm`.

The writer must:
- Accept values via `addNumericField`, `addBinaryField`, `addSortedField`, `addSortedNumericField`, `addSortedSetField`
- Iterate the `DocValuesProducer` to read all values
- Encode using Parquet column writers
- Write column chunk bytes to `IndexOutput` via `LuceneOutputFile`
- Track field metadata (offsets, lengths, types, min/max stats) in the `.pdvm` file

**Step 1: Write a round-trip test for numeric fields**

Write a test that creates a `ParquetDocValuesWriter`, adds a numeric field, closes it, then reads back the raw bytes and verifies the metadata. Full round-trip testing will be done in Task 7 with `ParquetDocValuesFormat` + `BaseDocValuesFormatTestCase`.

**Step 2: Implement ParquetDocValuesWriter**

The implementation should follow the pattern of `Composite912DocValuesWriter` for file management (open data/meta outputs, write codec headers/footers, close properly).

**Step 3: Run tests**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetDocValuesWriterTests" -Dtests.iters=10
```

**Step 4: Commit**

```bash
git add -A && git commit -m "Add ParquetDocValuesWriter for all DocValues types"
```

---

## Task 6: ParquetDocValuesReader

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetDocValuesReaderTests.java`

This is the core reader. It extends `DocValuesProducer` and reads Parquet column chunks, exposing them through Lucene's DocValues iterator APIs.

The reader must:
- Read metadata from `.pdvm` to locate column chunks
- Decode Parquet column data from `.pdvd` via `LuceneInputFile`
- Return proper Lucene iterators: `NumericDocValues`, `SortedNumericDocValues`, `BinaryDocValues`, `SortedDocValues`, `SortedSetDocValues`
- For `SORTED`/`SORTED_SET`: reconstruct the ordinal-to-term mapping from Parquet dictionary pages

**Step 1: Write round-trip tests**

Test each doc_values type: write with `ParquetDocValuesWriter`, read with `ParquetDocValuesReader`, assert values match.

**Step 2: Implement ParquetDocValuesReader**

**Step 3: Run tests**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetDocValuesReaderTests" -Dtests.iters=10
```

**Step 4: Commit**

```bash
git add -A && git commit -m "Add ParquetDocValuesReader for all DocValues types"
```

---

## Task 7: ParquetDocValuesFormat and Conformance Tests

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesFormat.java`
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetDocValuesFormatTests.java`

**Step 1: Implement ParquetDocValuesFormat**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class ParquetDocValuesFormat extends DocValuesFormat {

    public static final String FORMAT_NAME = "ParquetDocValues";
    public static final String DATA_EXTENSION = "pdvd";
    public static final String META_EXTENSION = "pdvm";

    public ParquetDocValuesFormat() {
        super(FORMAT_NAME);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ParquetDocValuesWriter(state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ParquetDocValuesReader(state);
    }
}
```

**Step 2: Write conformance test extending BaseDocValuesFormatTestCase**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;

public class ParquetDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    @Override
    protected Codec getCodec() {
        return new Lucene104Codec() {
            @Override
            public org.apache.lucene.codecs.DocValuesFormat getDocValuesFormatForField(String field) {
                return new ParquetDocValuesFormat();
            }
        };
    }
}
```

This runs Lucene's full conformance suite (~50+ test methods) against our format.

**Step 3: Run conformance tests**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetDocValuesFormatTests" -Dtests.iters=3
```

Expected: ALL PASS. If any fail, fix the writer/reader until they pass.

**Step 4: Commit**

```bash
git add -A && git commit -m "Add ParquetDocValuesFormat with Lucene conformance tests"
```

---

## Task 8: Wire into PerFieldMappingPostingFormatCodec

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/codec/PerFieldMappingPostingFormatCodec.java`

**Step 1: Modify getDocValuesFormatForField**

```java
// Add field:
private final DocValuesFormat parquetDvFormat = new ParquetDocValuesFormat();

// Change method:
@Override
public DocValuesFormat getDocValuesFormatForField(String field) {
    if (mapperService.getIndexSettings().isParquetDocValuesEnabled()) {
        return parquetDvFormat;
    }
    return dvFormat;
}
```

**Step 2: Run existing codec tests to verify no regression**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.*" -Dtests.iters=3
```

**Step 3: Commit**

```bash
git add -A && git commit -m "Wire ParquetDocValuesFormat into PerFieldMappingPostingFormatCodec"
```

---

## Task 9: Disable _source Storage for Parquet Indices

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/mapper/SourceFieldMapper.java`
- Modify: `server/src/main/java/org/opensearch/index/IndexSettings.java`

**Step 1: Skip _source in preParse**

In `SourceFieldMapper.preParse()`, add after the `isDerivedSourceEnabled()` check:

```java
if (context.indexSettings().isParquetDocValuesEnabled()) {
    return;
}
```

**Step 2: Run existing SourceFieldMapper tests**

```bash
./gradlew :server:test --tests "*.SourceFieldMapperTests" -Dtests.iters=3
```

**Step 3: Commit**

```bash
git add -A && git commit -m "Skip _source storage when parquet doc_values format is enabled"
```

---

## Task 10: _source Reconstruction from Parquet

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

This task integrates with the existing `DerivedSourceDirectoryReader` pattern. When parquet doc_values is enabled, the `IndexShard` wraps the directory reader to reconstruct `_source` from parquet columns, similar to how `derived_source` works today.

**Step 1: Add parquet condition alongside derived_source**

In `IndexShard`, where `DerivedSourceDirectoryReader.wrap()` is called for `isDerivedSourceEnabled()`, add the same wrapping for `isParquetDocValuesEnabled()`.

**Step 2: Run internal cluster tests**

```bash
./gradlew :server:internalClusterTest --tests "*.DerivedSource*" -Dtests.iters=1
```

**Step 3: Commit**

```bash
git add -A && git commit -m "Enable _source reconstruction from parquet doc_values"
```

---

## Task 11: Internal Cluster Integration Test

**Files:**
- Create: `server/src/internalClusterTest/java/org/opensearch/index/codec/parquet/ParquetDocValuesIT.java`

**Step 1: Write integration test**

Test class extending `OpenSearchIntegTestCase` that:
- Creates an index with `index.codec.doc_values.format: parquet` (with feature flag enabled)
- Indexes documents with mixed field types (long, double, keyword, date, ip, text with keyword sub-field)
- Verifies: terms aggregation, sum aggregation, date_histogram, range query, sort
- Verifies: `_source` is returned correctly in search hits
- Verifies: `_update` API works
- Forces merge and re-verifies all of the above

**Step 2: Run integration test**

```bash
./gradlew :server:internalClusterTest --tests "org.opensearch.index.codec.parquet.ParquetDocValuesIT" -Dtests.iters=3
```

**Step 3: Commit**

```bash
git add -A && git commit -m "Add integration tests for parquet doc_values format"
```

---

## Task 12: Parquet Export REST API

**Files:**
- Create: `server/src/main/java/org/opensearch/rest/action/admin/indices/RestParquetExportAction.java`
- Create: `server/src/main/java/org/opensearch/action/admin/indices/parquet/ParquetExportAction.java`
- Create: `server/src/main/java/org/opensearch/action/admin/indices/parquet/ParquetExportRequest.java`
- Create: `server/src/main/java/org/opensearch/action/admin/indices/parquet/ParquetExportResponse.java`
- Create: `server/src/main/java/org/opensearch/action/admin/indices/parquet/TransportParquetExportAction.java`

This task implements the `POST /my-index/_parquet_export` REST endpoint that exports shard data as standalone Parquet files readable by Spark/Trino/DuckDB.

The transport action reads all doc_values columns from each shard's segments via `ParquetDocValuesReader` and writes proper standalone `.parquet` files with valid Parquet footers, row group metadata, and column statistics to the specified path.

**Step 1: Implement the action classes following existing REST action patterns**

**Step 2: Write integration test**

**Step 3: Run tests**

```bash
./gradlew :server:internalClusterTest --tests "*ParquetExport*" -Dtests.iters=3
```

**Step 4: Commit**

```bash
git add -A && git commit -m "Add _parquet_export REST API for external tool interoperability"
```

---

## Task 13: Benchmarking

**No code changes.** Run `opensearch-benchmark` with `http_logs` workload.

**Step 1: Build OpenSearch**

```bash
./gradlew localDistro
```

**Step 2: Run baseline benchmark**

Start OpenSearch with default settings. Run:

```bash
opensearch-benchmark execute-test --workload http_logs --target-hosts localhost:9200 --pipeline benchmark-only
```

**Step 3: Run parquet benchmark**

Start OpenSearch with feature flag enabled. Create index template with `index.codec.doc_values.format: parquet`. Run same benchmark.

**Step 4: Compare results**

Compare ingestion throughput (docs/sec, p50/p99 latency), query latency for standard http_logs queries, and total index storage size.

**Step 5: Document results in design doc**
