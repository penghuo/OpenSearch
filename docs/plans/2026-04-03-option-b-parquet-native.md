# Option B: Native Parquet .pdvd File — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `.pdvd` file a valid standalone Parquet file readable by both OpenSearch (Lucene codec) AND standard Parquet readers (Spark/Trino/DuckDB) without any client library changes.

**Architecture:** Replace Lucene's `CodecUtil` header/footer framing with standard Parquet `PAR1` magic + Thrift `FileMetaData` footer. Lucene-specific metadata (version, segment ID, field offsets) stored in Parquet's `key_value_metadata`. Packed values and doc IDs written as extra bytes between column chunks and footer — POC confirmed DuckDB ignores these (uses `ColumnMetaData` offsets for seeks). Eliminate the `.pdvm` metadata file entirely.

**Tech Stack:** parquet-format (Thrift structs), parquet-column (encoders), Lucene IndexInput/IndexOutput, Zstd compression

**POC Validation:** All 6 DuckDB tests passed — extra bytes before footer ignored, custom KV metadata ignored, sparse OPTIONAL fields efficient.

---

## File Structure

### Files to Modify

| File | Responsibility |
|------|---------------|
| `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesWriter.java` | Replace CodecUtil framing with PAR1 magic + Parquet footer. Accumulate column metadata during write. Write footer in close(). |
| `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java` | Read Parquet footer from EOF. Extract Lucene metadata from KV. Build FieldMeta from ColumnMetaData. Read pages via Parquet PageHeader. |
| `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesFormat.java` | Remove META_EXTENSION/META_CODEC. Single-file format. |

### Files to Create

| File | Responsibility |
|------|---------------|
| `server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterWriter.java` | Build FileMetaData with schema, RowGroup, KV metadata. Serialize footer. |
| `server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterReader.java` | Read footer from EOF. Parse FileMetaData. Extract Lucene metadata from KV. |
| `server/src/test/java/org/opensearch/index/codec/parquet/ParquetNativeFormatTests.java` | Validate .pdvd is readable by standard Parquet tools. Validate KV metadata round-trip. |

### Files Eliminated

| File | Reason |
|------|--------|
| `.pdvm` metadata file | Metadata moves into Parquet footer KV metadata. No longer created. |

---

## Key Constants and Formats

### Version

```java
static final int VERSION_PARQUET_NATIVE = 7;
static final int VERSION_CURRENT = VERSION_PARQUET_NATIVE;
```

### .pdvd File Layout (V7)

```
[PAR1]                                          ← 4 bytes, standard Parquet magic
[Field 1: dictionary page + data pages]         ← standard Parquet PageHeader + ZSTD data
[Field 1: doc IDs + packed values]              ← extra bytes (not in ColumnMetaData)
[Field 2: dictionary page + data pages]
[Field 2: doc IDs + packed values]
...
[Thrift FileMetaData]                           ← standard Parquet footer
  schema: [...field types...]
  row_groups: [ColumnChunk per field with offsets]
  key_value_metadata:
    lucene.codec.version = "7"
    lucene.segment.id = "<hex>"
    lucene.segment.suffix = ""
    field.<name>.dvTypeName = "SORTED_NUMERIC"
    field.<name>.docIdOffset = "<relative offset within column chunk>"
    field.<name>.packedValuesOffset = "<relative offset>"  (-1 if none)
[4-byte footer length, little-endian]
[PAR1]                                          ← 4 bytes, standard Parquet magic
```

### Page Format (Unchanged Internally)

Each data page uses standard Parquet `PageHeader` (Thrift-serialized) + ZSTD-compressed data. This replaces the current custom format of `[int:compressedSize][int:uncompressedSize][int:valueCount][int:rowCount][String:encoding...][bytes]`.

### KV Metadata Keys

| Key | Value | Purpose |
|-----|-------|---------|
| `lucene.codec.version` | `"7"` | Format version for backward compat |
| `lucene.segment.id` | hex string | Segment identity validation |
| `lucene.segment.suffix` | string | Segment suffix |
| `lucene.maxDoc` | `"247000000"` | Max doc count for dense field detection |
| `field.<name>.dvTypeName` | `"NUMERIC"` etc. | Lucene DocValuesType |
| `field.<name>.repetition` | `"REQUIRED"` etc. | Parquet repetition type |
| `field.<name>.pageCount` | `"5"` | Number of data pages |
| `field.<name>.hasDictionary` | `"true"` | Whether dict page exists |
| `field.<name>.docIdOffset` | `"12345"` | Byte offset within chunk to doc IDs |
| `field.<name>.packedValuesOffset` | `"67890"` or `"-1"` | Byte offset to packed values |

---

## Task Breakdown

### Task 1: ParquetFooterWriter — Build Valid Parquet Footer

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterWriter.java`
- Test: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetFooterWriterTests.java`

This helper builds the Thrift `FileMetaData` from accumulated column metadata. Modeled directly on `ParquetFileExporter.java:479-517`.

- [ ] **Step 1: Write the test**

```java
// ParquetFooterWriterTests.java
package org.opensearch.index.codec.parquet;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.Type;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ParquetFooterWriterTests extends OpenSearchTestCase {

    public void testBuildFooterSingleNumericField() throws Exception {
        ParquetFooterWriter.ColumnChunkInfo chunk = new ParquetFooterWriter.ColumnChunkInfo(
            "timestamp",
            "SORTED_NUMERIC",
            "REPEATED",
            Type.INT64,
            4,       // fileOffset (after PAR1 magic)
            1000,    // dictPageOffset (-1 = no dict)
            -1,
            5,       // pageCount
            false,   // hasDictionary
            800,     // docIdOffset (relative to chunk start)
            900,     // packedValuesOffset (relative to chunk start)
            1200,    // totalCompressedSize
            1200,    // totalUncompressedSize
            50000,   // numValues
            List.of(org.apache.parquet.format.Encoding.DELTA_BINARY_PACKED,
                     org.apache.parquet.format.Encoding.RLE)
        );

        ParquetFooterWriter builder = new ParquetFooterWriter(
            7,                    // version
            "abc123",             // segmentId
            "",                   // segmentSuffix
            50000,                // maxDoc
            50000,                // totalRows
            List.of(chunk)
        );

        FileMetaData footer = builder.buildFileMetaData();

        // Verify standard Parquet fields
        assertEquals(1, footer.getVersion());
        assertEquals(50000, footer.getNum_rows());
        assertEquals(1, footer.getRow_groups().size());
        assertEquals(2, footer.getSchema().size()); // root + 1 field

        // Verify KV metadata contains Lucene fields
        Map<String, String> kv = ParquetFooterWriter.kvToMap(footer.getKey_value_metadata());
        assertEquals("7", kv.get("lucene.codec.version"));
        assertEquals("abc123", kv.get("lucene.segment.id"));
        assertEquals("800", kv.get("field.timestamp.docIdOffset"));
        assertEquals("900", kv.get("field.timestamp.packedValuesOffset"));
        assertEquals("SORTED_NUMERIC", kv.get("field.timestamp.dvTypeName"));

        // Verify serialization round-trip
        byte[] bytes = builder.serializeFooter(footer);
        assertTrue(bytes.length > 0);
    }
}
```

- [ ] **Step 2: Run test — expect FAIL (class not found)**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetFooterWriterTests" 2>&1 | tail -5
```

- [ ] **Step 3: Implement ParquetFooterWriter**

```java
// ParquetFooterWriter.java
package org.opensearch.index.codec.parquet;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetFooterWriter {

    static final byte[] PARQUET_MAGIC = new byte[] { 'P', 'A', 'R', '1' };

    private final int codecVersion;
    private final String segmentId;
    private final String segmentSuffix;
    private final int maxDoc;
    private final long totalRows;
    private final List<ColumnChunkInfo> columns;

    public ParquetFooterWriter(int codecVersion, String segmentId, String segmentSuffix,
                               int maxDoc, long totalRows, List<ColumnChunkInfo> columns) {
        this.codecVersion = codecVersion;
        this.segmentId = segmentId;
        this.segmentSuffix = segmentSuffix;
        this.maxDoc = maxDoc;
        this.totalRows = totalRows;
        this.columns = columns;
    }

    public FileMetaData buildFileMetaData() {
        List<ColumnChunk> columnChunks = new ArrayList<>();
        List<SchemaElement> schema = new ArrayList<>();
        long totalByteSize = 0;

        // Root schema element
        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(columns.size());
        schema.add(root);

        for (ColumnChunkInfo col : columns) {
            // Schema element
            SchemaElement elem = new SchemaElement(col.fieldName);
            elem.setType(col.parquetType);
            elem.setRepetition_type(FieldRepetitionType.valueOf(col.repetition));
            schema.add(elem);

            // ColumnMetaData
            ColumnMetaData colMeta = new ColumnMetaData(
                col.parquetType,
                col.encodings,
                Collections.singletonList(col.fieldName),
                CompressionCodec.ZSTD,
                col.numValues,
                col.totalUncompressedSize,
                col.totalCompressedSize,
                col.dataPageOffset
            );
            if (col.dictPageOffset >= 0) {
                colMeta.setDictionary_page_offset(col.dictPageOffset);
            }

            ColumnChunk cc = new ColumnChunk(col.fileOffset);
            cc.setMeta_data(colMeta);
            columnChunks.add(cc);

            totalByteSize += col.totalCompressedSize;
        }

        RowGroup rowGroup = new RowGroup(columnChunks, totalByteSize, totalRows);
        FileMetaData fileMetaData = new FileMetaData(1, schema, totalRows, Collections.singletonList(rowGroup));
        fileMetaData.setCreated_by("OpenSearch Parquet DocValues");
        fileMetaData.setKey_value_metadata(buildKvMetadata());
        return fileMetaData;
    }

    private List<KeyValue> buildKvMetadata() {
        List<KeyValue> kv = new ArrayList<>();
        kv.add(new KeyValue("lucene.codec.version", String.valueOf(codecVersion)));
        kv.add(new KeyValue("lucene.segment.id", segmentId));
        kv.add(new KeyValue("lucene.segment.suffix", segmentSuffix));
        kv.add(new KeyValue("lucene.maxDoc", String.valueOf(maxDoc)));

        for (ColumnChunkInfo col : columns) {
            String prefix = "field." + col.fieldName + ".";
            kv.add(new KeyValue(prefix + "dvTypeName", col.dvTypeName));
            kv.add(new KeyValue(prefix + "repetition", col.repetition));
            kv.add(new KeyValue(prefix + "pageCount", String.valueOf(col.pageCount)));
            kv.add(new KeyValue(prefix + "hasDictionary", String.valueOf(col.hasDictionary)));
            kv.add(new KeyValue(prefix + "docIdOffset", String.valueOf(col.docIdOffset)));
            kv.add(new KeyValue(prefix + "packedValuesOffset", String.valueOf(col.packedValuesOffset)));
        }
        return kv;
    }

    public byte[] serializeFooter(FileMetaData fileMetaData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Util.writeFileMetaData(fileMetaData, baos);
        return baos.toByteArray();
    }

    public static Map<String, String> kvToMap(List<KeyValue> kvList) {
        Map<String, String> map = new HashMap<>();
        if (kvList != null) {
            for (KeyValue kv : kvList) {
                map.put(kv.getKey(), kv.getValue());
            }
        }
        return map;
    }

    public static class ColumnChunkInfo {
        final String fieldName;
        final String dvTypeName;
        final String repetition;
        final Type parquetType;
        final long fileOffset;
        final long dataPageOffset;
        final long dictPageOffset;
        final int pageCount;
        final boolean hasDictionary;
        final long docIdOffset;
        final long packedValuesOffset;
        final long totalCompressedSize;
        final long totalUncompressedSize;
        final long numValues;
        final List<org.apache.parquet.format.Encoding> encodings;

        public ColumnChunkInfo(String fieldName, String dvTypeName, String repetition,
                               Type parquetType, long fileOffset, long dataPageOffset,
                               long dictPageOffset, int pageCount, boolean hasDictionary,
                               long docIdOffset, long packedValuesOffset,
                               long totalCompressedSize, long totalUncompressedSize,
                               long numValues, List<org.apache.parquet.format.Encoding> encodings) {
            this.fieldName = fieldName;
            this.dvTypeName = dvTypeName;
            this.repetition = repetition;
            this.parquetType = parquetType;
            this.fileOffset = fileOffset;
            this.dataPageOffset = dataPageOffset;
            this.dictPageOffset = dictPageOffset;
            this.pageCount = pageCount;
            this.hasDictionary = hasDictionary;
            this.docIdOffset = docIdOffset;
            this.packedValuesOffset = packedValuesOffset;
            this.totalCompressedSize = totalCompressedSize;
            this.totalUncompressedSize = totalUncompressedSize;
            this.numValues = numValues;
            this.encodings = encodings;
        }
    }
}
```

- [ ] **Step 4: Run test — expect PASS**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.ParquetFooterWriterTests" 2>&1 | tail -5
```

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterWriter.java \
       server/src/test/java/org/opensearch/index/codec/parquet/ParquetFooterWriterTests.java
git commit -m "Add ParquetFooterWriter: builds standard Parquet FileMetaData with Lucene KV metadata"
```

---

### Task 2: ParquetFooterReader — Parse Footer and Extract Metadata

**Files:**
- Create: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterReader.java`
- Test: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetFooterReaderTests.java`

- [ ] **Step 1: Write the test**

```java
// ParquetFooterReaderTests.java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.format.FileMetaData;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

public class ParquetFooterReaderTests extends OpenSearchTestCase {

    public void testReadFooterRoundTrip() throws Exception {
        // Build a footer
        ParquetFooterWriter.ColumnChunkInfo chunk = new ParquetFooterWriter.ColumnChunkInfo(
            "status", "SORTED_NUMERIC", "REPEATED",
            org.apache.parquet.format.Type.INT64,
            4, 4, -1, 1, false, 100, -1,
            200, 200, 1000,
            List.of(org.apache.parquet.format.Encoding.DELTA_BINARY_PACKED)
        );
        ParquetFooterWriter writer = new ParquetFooterWriter(7, "seg123", "", 1000, 1000, List.of(chunk));
        FileMetaData meta = writer.buildFileMetaData();
        byte[] footerBytes = writer.serializeFooter(meta);

        // Write a fake .pdvd file: PAR1 + dummy data + footer + footer_len + PAR1
        Directory dir = newDirectory();
        try (IndexOutput out = dir.createOutput("test.pdvd", IOContext.DEFAULT)) {
            out.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);
            out.writeBytes(new byte[200], 0, 200); // dummy column data
            out.writeBytes(footerBytes, 0, footerBytes.length);
            byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array();
            out.writeBytes(lenBytes, 0, 4);
            out.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);
        }

        // Read it back
        try (IndexInput in = dir.openInput("test.pdvd", IOContext.DEFAULT)) {
            ParquetFooterReader reader = new ParquetFooterReader(in);
            FileMetaData readMeta = reader.readFooter();

            assertEquals(1000, readMeta.getNum_rows());
            assertEquals(1, readMeta.getRow_groups().size());

            Map<String, String> kv = ParquetFooterWriter.kvToMap(readMeta.getKey_value_metadata());
            assertEquals("7", kv.get("lucene.codec.version"));
            assertEquals("seg123", kv.get("lucene.segment.id"));
            assertEquals("SORTED_NUMERIC", kv.get("field.status.dvTypeName"));
            assertEquals("100", kv.get("field.status.docIdOffset"));
        }
        dir.close();
    }

    public void testInvalidMagicThrows() throws Exception {
        Directory dir = newDirectory();
        try (IndexOutput out = dir.createOutput("bad.pdvd", IOContext.DEFAULT)) {
            out.writeBytes(new byte[] { 'N', 'O', 'P', 'E' }, 0, 4);
            out.writeBytes(new byte[100], 0, 100);
        }
        try (IndexInput in = dir.openInput("bad.pdvd", IOContext.DEFAULT)) {
            expectThrows(org.apache.lucene.index.CorruptIndexException.class, () -> {
                new ParquetFooterReader(in).readFooter();
            });
        }
        dir.close();
    }
}
```

- [ ] **Step 2: Run test — expect FAIL (class not found)**

- [ ] **Step 3: Implement ParquetFooterReader**

```java
// ParquetFooterReader.java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ParquetFooterReader {

    private final IndexInput input;

    public ParquetFooterReader(IndexInput input) {
        this.input = input;
    }

    public FileMetaData readFooter() throws IOException {
        long fileLen = input.length();
        if (fileLen < 12) { // 4 (magic) + 4 (footer len) + 4 (magic)
            throw new CorruptIndexException("File too short for Parquet format", input);
        }

        // Verify trailing PAR1 magic
        input.seek(fileLen - 4);
        byte[] tailMagic = new byte[4];
        input.readBytes(tailMagic, 0, 4);
        if (!Arrays.equals(tailMagic, ParquetFooterWriter.PARQUET_MAGIC)) {
            throw new CorruptIndexException("Missing trailing PAR1 magic", input);
        }

        // Read footer length (4 bytes little-endian before trailing magic)
        input.seek(fileLen - 8);
        byte[] lenBytes = new byte[4];
        input.readBytes(lenBytes, 0, 4);
        int footerLen = ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

        if (footerLen <= 0 || footerLen > fileLen - 12) {
            throw new CorruptIndexException("Invalid footer length: " + footerLen, input);
        }

        // Read and deserialize footer
        long footerStart = fileLen - 8 - footerLen;
        input.seek(footerStart);
        byte[] footerBytes = new byte[footerLen];
        input.readBytes(footerBytes, 0, footerLen);

        return Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
    }

    public void validateLeadingMagic() throws IOException {
        input.seek(0);
        byte[] magic = new byte[4];
        input.readBytes(magic, 0, 4);
        if (!Arrays.equals(magic, ParquetFooterWriter.PARQUET_MAGIC)) {
            throw new CorruptIndexException("Missing leading PAR1 magic", input);
        }
    }

    public static Map<String, String> extractKvMetadata(FileMetaData meta) {
        return ParquetFooterWriter.kvToMap(meta.getKey_value_metadata());
    }
}
```

- [ ] **Step 4: Run test — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/org/opensearch/index/codec/parquet/ParquetFooterReader.java \
       server/src/test/java/org/opensearch/index/codec/parquet/ParquetFooterReaderTests.java
git commit -m "Add ParquetFooterReader: reads Parquet footer from EOF, extracts Lucene KV metadata"
```

---

### Task 3: Modify Writer — PAR1 Magic + PageHeader + Parquet Footer

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesWriter.java`
- Modify: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesFormat.java`

This is the largest task. The writer must:
1. Write `PAR1` instead of `CodecUtil.writeIndexHeader()`
2. Write Parquet `PageHeader` structs instead of custom page framing
3. Track `ColumnChunkInfo` per field during write
4. Write Parquet footer in `close()` instead of `CodecUtil.writeFooter()`
5. Eliminate `.pdvm` file

- [ ] **Step 1: Add VERSION_PARQUET_NATIVE and PARQUET_MAGIC constants**

In `ParquetDocValuesWriter.java`, after line 79:

```java
static final int VERSION_PARQUET_NATIVE = 7;
static final int VERSION_CURRENT = VERSION_PARQUET_NATIVE;
```

- [ ] **Step 2: Modify constructor — replace CodecUtil header with PAR1 magic, remove metaOut**

Replace the constructor to only open `dataOut`, write `PAR1` magic, and skip `.pdvm`:

```java
private final IndexOutput dataOut;
private final int maxDoc;
private final byte[] segmentId;
private final String segmentSuffix;
private final List<ParquetFooterWriter.ColumnChunkInfo> columnChunks = new ArrayList<>();

public ParquetDocValuesWriter(SegmentWriteState state, String dataExtension) throws IOException {
    String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    boolean success = false;
    try {
        dataOut = state.directory.createOutput(dataFileName, state.context);
        // Write leading PAR1 magic instead of CodecUtil header
        dataOut.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);
        maxDoc = state.segmentInfo.maxDoc();
        segmentId = Hex.encodeHexString(state.segmentInfo.getId());
        segmentSuffix = state.segmentSuffix;
        success = true;
    } finally {
        if (!success) {
            IOUtils.closeWhileHandlingException(this);
        }
    }
}
```

- [ ] **Step 3: Modify StreamingPageWriter to write Parquet PageHeader structs**

Replace the custom page format with standard Parquet `PageHeader` serialization (same as `ParquetFileExporter.java:439-445`):

```java
@Override
public void writePage(BytesInput bytes, int valueCount, int rowCount,
        Statistics<?> statistics, Encoding rlEncoding, Encoding dlEncoding,
        Encoding valuesEncoding) throws IOException {
    byte[] pageBytes = bytes.toByteArray();
    byte[] compressedBytes = Zstd.compress(pageBytes);

    // Standard Parquet PageHeader
    PageHeader ph = new PageHeader(PageType.DATA_PAGE, pageBytes.length, compressedBytes.length);
    ph.setData_page_header(new DataPageHeader(
        valueCount,
        convertEncoding(valuesEncoding),
        convertEncoding(dlEncoding),
        convertEncoding(rlEncoding)
    ));

    byte[] headerBytes = serializePageHeader(ph);
    out.writeBytes(headerBytes, headerBytes.length);
    out.writeBytes(compressedBytes, compressedBytes.length);

    totalCompressedSize += headerBytes.length + compressedBytes.length;
    totalUncompressedSize += headerBytes.length + pageBytes.length;
    trackEncoding(valuesEncoding);
    trackEncoding(rlEncoding);
    trackEncoding(dlEncoding);
    pageCount++;
}
```

Similarly update `writeDictionaryPage()` to use `PageHeader` with `DictionaryPageHeader`.

- [ ] **Step 4: Modify each addXxxField() method to build ColumnChunkInfo**

After writing pages + docIds + packedValues for each field, build and accumulate a `ColumnChunkInfo`:

```java
// At end of addNumericField(), replace writeFieldMeta() call:
columnChunks.add(new ParquetFooterWriter.ColumnChunkInfo(
    field.name, field.getDocValuesType().name(),
    parquetType.getRepetition().name(), Type.INT64,
    dataStartOffset, firstDataPageOffset, dictPageOffset,
    pageWriter.getPageCount(), pageWriter.hasDictionary(),
    docIdOffset, packedValuesOffset,
    pageWriter.getTotalCompressedSize(), pageWriter.getTotalUncompressedSize(),
    docCount, pageWriter.getEncodings()
));
```

- [ ] **Step 5: Modify close() to write Parquet footer**

```java
@Override
public void close() throws IOException {
    if (closed) return;
    closed = true;
    boolean success = false;
    try {
        // Build and write Parquet footer
        ParquetFooterWriter footerWriter = new ParquetFooterWriter(
            VERSION_CURRENT, segmentId, segmentSuffix,
            maxDoc, maxDoc, columnChunks
        );
        FileMetaData footer = footerWriter.buildFileMetaData();
        byte[] footerBytes = footerWriter.serializeFooter(footer);

        dataOut.writeBytes(footerBytes, 0, footerBytes.length);

        // Footer length (4 bytes little-endian)
        byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
            .putInt(footerBytes.length).array();
        dataOut.writeBytes(lenBytes, 0, 4);

        // Trailing PAR1 magic
        dataOut.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);

        success = true;
    } finally {
        if (success) {
            IOUtils.close(dataOut);
        } else {
            IOUtils.closeWhileHandlingException(dataOut);
        }
    }
}
```

- [ ] **Step 6: Update ParquetDocValuesFormat.java — single file, no .pdvm**

```java
public class ParquetDocValuesFormat extends DocValuesFormat {
    public static final String FORMAT_NAME = "Parquet";
    public static final String DATA_EXTENSION = "pdvd";

    public ParquetDocValuesFormat() { super(FORMAT_NAME); }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ParquetDocValuesWriter(state, DATA_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ParquetDocValuesReader(state, DATA_EXTENSION);
    }
}
```

- [ ] **Step 7: Compile and fix imports**

```bash
./gradlew :server:compileJava 2>&1 | tail -10
```

- [ ] **Step 8: Commit**

```bash
git add server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesWriter.java \
       server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesFormat.java
git commit -m "ParquetDocValuesWriter V7: write PAR1 magic, PageHeader structs, Parquet footer"
```

---

### Task 4: Modify Reader — Read Parquet Footer, Build FieldMeta

**Files:**
- Modify: `server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java`

The reader must:
1. Read Parquet footer from EOF instead of opening `.pdvm`
2. Extract FieldMeta from `ColumnMetaData` + KV metadata
3. Read pages using Parquet `PageHeader` structs instead of custom format
4. Maintain backward compat with V2-V6 (detect by trying PAR1 magic first, fall back to CodecUtil)

- [ ] **Step 1: Modify constructor — read Parquet footer**

```java
public ParquetDocValuesReader(SegmentReadState state, String dataExtension) throws IOException {
    String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    IndexInput dataInput = state.directory.openInput(dataFileName, state.context);
    boolean success = false;
    try {
        // Detect format: V7+ starts with PAR1, older versions start with CodecUtil header
        dataInput.seek(0);
        byte[] magic = new byte[4];
        dataInput.readBytes(magic, 0, 4);

        if (Arrays.equals(magic, ParquetFooterWriter.PARQUET_MAGIC)) {
            // V7: read Parquet footer
            ParquetFooterReader footerReader = new ParquetFooterReader(dataInput);
            FileMetaData fileMetaData = footerReader.readFooter();
            Map<String, String> kv = ParquetFooterReader.extractKvMetadata(fileMetaData);

            this.version = Integer.parseInt(kv.get("lucene.codec.version"));
            String expectedId = Hex.encodeHexString(state.segmentInfo.getId());
            String actualId = kv.get("lucene.segment.id");
            if (!expectedId.equals(actualId)) {
                throw new CorruptIndexException("Segment ID mismatch", dataInput);
            }

            buildFieldsFromFooter(fileMetaData, kv);
        } else {
            // V2-V6: legacy CodecUtil format — fall back to old reader path
            dataInput.seek(0);
            // ... existing CodecUtil.checkIndexHeader() + readFields() logic ...
        }

        this.dataIn = dataInput;
        warmUp();
        success = true;
    } finally {
        if (!success) {
            IOUtils.closeWhileHandlingException(dataInput);
        }
    }
}
```

- [ ] **Step 2: Add buildFieldsFromFooter() method**

```java
private void buildFieldsFromFooter(FileMetaData fileMetaData, Map<String, String> kv) {
    RowGroup rowGroup = fileMetaData.getRow_groups().get(0);
    for (ColumnChunk cc : rowGroup.getColumns()) {
        ColumnMetaData colMeta = cc.getMeta_data();
        String fieldName = colMeta.getPath_in_schema().get(0);

        String prefix = "field." + fieldName + ".";
        String dvTypeName = kv.get(prefix + "dvTypeName");
        String repetition = kv.get(prefix + "repetition");
        int pageCount = Integer.parseInt(kv.get(prefix + "pageCount"));
        boolean hasDictionary = Boolean.parseBoolean(kv.get(prefix + "hasDictionary"));
        long docIdOffset = Long.parseLong(kv.get(prefix + "docIdOffset"));

        fields.put(fieldName, new FieldMeta(
            fieldName, dvTypeName, repetition,
            colMeta.getType().name(),
            cc.getFile_offset(),                    // dataStartOffset
            colMeta.getTotal_compressed_size(),      // dataLength
            pageCount, hasDictionary, docIdOffset
        ));
    }
}
```

- [ ] **Step 3: Modify readFieldData() to read PageHeader structs for V7**

For V7 files, data pages use Thrift-serialized `PageHeader` instead of the custom `[int:compressedSize][int:uncompressedSize]...` format. Add a version-branched reader:

```java
if (version >= ParquetDocValuesWriter.VERSION_PARQUET_NATIVE) {
    // V7: read standard Parquet PageHeader
    for (int i = 0; i < meta.pageCount; i++) {
        PageHeader ph = readPageHeader(slice);
        int compressedSize = ph.getCompressed_page_size();
        int uncompressedSize = ph.getUncompressed_page_size();
        DataPageHeader dph = ph.getData_page_header();
        byte[] compressedBytes = new byte[compressedSize];
        slice.readBytes(compressedBytes, 0, compressedSize);
        byte[] pageBytes = Zstd.decompress(compressedBytes, uncompressedSize);
        dataPages.add(new DataPageV1(...));
    }
} else {
    // V2-V6: existing custom page format
    // ... existing code unchanged ...
}
```

- [ ] **Step 4: Add readPageHeader() helper using Thrift deserialization**

```java
private static PageHeader readPageHeader(IndexInput input) throws IOException {
    // Read enough bytes for Thrift deserialization (PageHeader is small, typically < 100 bytes)
    // Use a buffered approach: read up to 256 bytes, try to deserialize
    long startPos = input.getFilePointer();
    byte[] buf = new byte[256];
    int bytesRead = Math.min(256, (int)(input.length() - startPos));
    input.readBytes(buf, 0, bytesRead);
    ByteArrayInputStream bais = new ByteArrayInputStream(buf, 0, bytesRead);
    PageHeader ph = Util.readPageHeader(bais);
    // Advance input past the actual bytes consumed
    input.seek(startPos + (bytesRead - bais.available()));
    return ph;
}
```

- [ ] **Step 5: Run all parquet tests**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.*" 2>&1 | grep "tests completed"
```

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/org/opensearch/index/codec/parquet/ParquetDocValuesReader.java
git commit -m "ParquetDocValuesReader V7: read Parquet footer, PageHeader structs, backward compat V2-V6"
```

---

### Task 5: Native Format Validation Tests

**Files:**
- Create: `server/src/test/java/org/opensearch/index/codec/parquet/ParquetNativeFormatTests.java`

Validate that .pdvd files are valid Parquet files readable by standard tools.

- [ ] **Step 1: Write validation tests**

```java
package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public class ParquetNativeFormatTests extends OpenSearchTestCase {

    public void testPdvdFileStartsWithPAR1Magic() throws Exception {
        // Write a segment with ParquetDocValuesWriter
        Directory dir = newDirectory();
        // ... setup SegmentWriteState, write a NUMERIC field ...

        // Read the raw .pdvd file
        IndexInput in = dir.openInput(/* pdvd filename */, IOContext.DEFAULT);
        byte[] magic = new byte[4];
        in.readBytes(magic, 0, 4);
        assertArrayEquals(ParquetFooterWriter.PARQUET_MAGIC, magic);
        in.close();
        dir.close();
    }

    public void testPdvdFileHasValidParquetFooter() throws Exception {
        // Write segment, then parse footer using standard Parquet Util
        Directory dir = newDirectory();
        // ... write fields ...

        IndexInput in = dir.openInput(/* pdvd filename */, IOContext.DEFAULT);
        long fileLen = in.length();

        // Read trailing magic
        in.seek(fileLen - 4);
        byte[] tailMagic = new byte[4];
        in.readBytes(tailMagic, 0, 4);
        assertArrayEquals(ParquetFooterWriter.PARQUET_MAGIC, tailMagic);

        // Read footer
        in.seek(fileLen - 8);
        byte[] lenBytes = new byte[4];
        in.readBytes(lenBytes, 0, 4);
        int footerLen = ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

        in.seek(fileLen - 8 - footerLen);
        byte[] footerBytes = new byte[footerLen];
        in.readBytes(footerBytes, 0, footerLen);

        // Parse with standard Parquet Thrift deserializer
        FileMetaData meta = Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
        assertNotNull(meta);
        assertTrue(meta.getNum_rows() > 0);
        assertNotNull(meta.getSchema());
        assertNotNull(meta.getRow_groups());

        // Verify Lucene metadata in KV
        Map<String, String> kv = ParquetFooterWriter.kvToMap(meta.getKey_value_metadata());
        assertEquals("7", kv.get("lucene.codec.version"));

        in.close();
        dir.close();
    }

    public void testKvMetadataContainsAllFieldInfo() throws Exception {
        // Write segment with NUMERIC + SORTED_SET fields
        // Verify KV metadata has entries for both fields
    }

    public void testRoundTripReadWrite() throws Exception {
        // Write with V7 writer, read with V7 reader
        // Verify all values match
    }

    public void testBackwardCompatV5() throws Exception {
        // Write with old V5 format (CodecUtil header)
        // Read with V7 reader — should detect non-PAR1 magic and fall back
    }
}
```

- [ ] **Step 2: Implement tests and verify all pass**

- [ ] **Step 3: Commit**

```bash
git add server/src/test/java/org/opensearch/index/codec/parquet/ParquetNativeFormatTests.java
git commit -m "Add ParquetNativeFormatTests: validate .pdvd is valid Parquet with standard tools"
```

---

### Task 6: Run Full Test Suite

**Files:** None (testing only)

- [ ] **Step 1: Run parquet unit tests**

```bash
./gradlew :server:test --tests "org.opensearch.index.codec.parquet.*" 2>&1 | grep "tests completed"
```

Expected: All tests pass except pre-existing `testThreads` failure.

- [ ] **Step 2: Build localDistro**

```bash
./benchmark.sh build 2>&1 | tail -3
```

- [ ] **Step 3: Run yamlRestTestParquet**

```bash
./benchmark.sh test 2>&1 | tail -3
```

Expected: PASS (100% DSL compatibility).

- [ ] **Step 4: Commit any fixes needed**

- [ ] **Step 5: Push all changes**

```bash
git push origin features/parquet
```

---

## Risk Mitigations

| Risk | Mitigation |
|------|-----------|
| Thrift PageHeader read is slower than custom int reads | PageHeader is <100 bytes; deserialize once per page. Benchmark will validate. |
| Old V2-V6 indices unreadable | Backward compat in reader: detect PAR1 vs CodecUtil magic at byte 0. |
| Extra bytes confuse some Parquet readers | POC validated with DuckDB. Test with Spark before GA. |
| Footer at EOF adds seek on reader open | One seek + ~1KB read. Negligible vs page decompression. |
| No .pdvm breaks Lucene's file listing | Lucene lists files from `SegmentInfo`. Remove `.pdvm` from the file list. |

## Success Criteria

1. `.pdvd` files parseable by `Util.readFileMetaData()` (standard Parquet)
2. DuckDB `SELECT * FROM 'segment.pdvd'` returns correct data
3. yamlRestTestParquet 100% pass
4. Benchmark: no regression vs current V6 performance
5. V2-V6 indices still readable (backward compat)
