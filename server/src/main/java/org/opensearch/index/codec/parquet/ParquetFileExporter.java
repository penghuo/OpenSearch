/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Exports doc_values from an OpenSearch index shard as standalone Parquet files
 * readable by Spark, Trino, and DuckDB.
 *
 * The export reads Lucene doc_values iterators and writes proper Parquet files
 * with valid footers using parquet-format Thrift structures.
 */
public class ParquetFileExporter {

    private static final byte[] MAGIC = "PAR1".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    /**
     * Result of an export operation.
     */
    public static class ExportResult {
        public final long totalBytes;
        public final long numRows;

        public ExportResult(long totalBytes, long numRows) {
            this.totalBytes = totalBytes;
            this.numRows = numRows;
        }
    }

    /**
     * Exports all doc_values from the given searcher to a standalone Parquet file.
     *
     * @param searcher the index searcher to read doc values from
     * @param outputFile the path to write the Parquet file to
     * @return export result with total bytes and row count
     */
    public static ExportResult export(IndexSearcher searcher, Path outputFile) throws IOException {
        // Collect field info across all segments
        Map<String, DocValuesType> fields = new LinkedHashMap<>();
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                if (fi.getDocValuesType() != DocValuesType.NONE && fi.name.startsWith("_") == false) {
                    fields.putIfAbsent(fi.name, fi.getDocValuesType());
                }
            }
        }

        if (fields.isEmpty()) {
            return new ExportResult(0, 0);
        }

        // Count total rows
        long totalRows = 0;
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            totalRows += ctx.reader().maxDoc();
        }

        // For each field, encode all values into Parquet pages
        List<ColumnData> columns = new ArrayList<>();
        for (Map.Entry<String, DocValuesType> entry : fields.entrySet()) {
            columns.add(encodeColumn(entry.getKey(), entry.getValue(), searcher, totalRows));
        }

        // Write the Parquet file
        long bytesWritten = writeParquetFile(outputFile, fields, columns, totalRows);
        return new ExportResult(bytesWritten, totalRows);
    }

    /**
     * Holds captured Parquet pages for a single column.
     */
    static class ColumnData {
        final List<CapturedPage> dataPages;
        final CapturedDictPage dictPage;
        final long numValues;

        ColumnData(List<CapturedPage> dataPages, CapturedDictPage dictPage, long numValues) {
            this.dataPages = dataPages;
            this.dictPage = dictPage;
            this.numValues = numValues;
        }
    }

    static class CapturedPage {
        final byte[] bytes;
        final int valueCount;
        final Encoding valuesEncoding;
        final Encoding rlEncoding;
        final Encoding dlEncoding;

        CapturedPage(byte[] bytes, int valueCount, Encoding valuesEncoding, Encoding rlEncoding, Encoding dlEncoding) {
            this.bytes = bytes;
            this.valueCount = valueCount;
            this.valuesEncoding = valuesEncoding;
            this.rlEncoding = rlEncoding;
            this.dlEncoding = dlEncoding;
        }
    }

    static class CapturedDictPage {
        final byte[] bytes;
        final int dictSize;
        final Encoding encoding;

        CapturedDictPage(byte[] bytes, int dictSize, Encoding encoding) {
            this.bytes = bytes;
            this.dictSize = dictSize;
            this.encoding = encoding;
        }
    }

    /**
     * PageWriter that captures pages in memory instead of writing to disk.
     */
    static class CapturingPageWriter implements PageWriter {
        final List<CapturedPage> pages = new ArrayList<>();
        CapturedDictPage dictPage;
        long memSize;

        @Override
        public void writePage(
            BytesInput bytes,
            int valueCount,
            Statistics<?> statistics,
            Encoding rlEncoding,
            Encoding dlEncoding,
            Encoding valuesEncoding
        ) throws IOException {
            byte[] data = bytes.toByteArray();
            pages.add(new CapturedPage(data, valueCount, valuesEncoding, rlEncoding, dlEncoding));
            memSize += data.length;
        }

        @Override
        public void writePage(
            BytesInput bytes,
            int valueCount,
            int rowCount,
            Statistics<?> statistics,
            Encoding rlEncoding,
            Encoding dlEncoding,
            Encoding valuesEncoding
        ) throws IOException {
            writePage(bytes, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
        }

        @Override
        public void writePageV2(
            int rowCount,
            int nullCount,
            int valueCount,
            BytesInput repetitionLevels,
            BytesInput definitionLevels,
            Encoding dataEncoding,
            BytesInput data,
            Statistics<?> statistics
        ) throws IOException {
            BytesInput combined = BytesInput.concat(repetitionLevels, definitionLevels, data);
            writePage(combined, valueCount, null, Encoding.RLE, Encoding.RLE, dataEncoding);
        }

        @Override
        public long getMemSize() {
            return memSize;
        }

        @Override
        public long allocatedSize() {
            return memSize;
        }

        @Override
        public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
            byte[] data = dictionaryPage.getBytes().toByteArray();
            dictPage = new CapturedDictPage(data, dictionaryPage.getDictionarySize(), dictionaryPage.getEncoding());
        }

        @Override
        public String memUsageString(String prefix) {
            return prefix + " CapturingPageWriter: " + memSize;
        }
    }

    /**
     * PageWriteStore that returns a single CapturingPageWriter for any column.
     */
    static class CapturingPageWriteStore implements PageWriteStore {
        final CapturingPageWriter writer = new CapturingPageWriter();

        @Override
        public PageWriter getPageWriter(ColumnDescriptor path) {
            return writer;
        }

        @Override
        public void close() {}
    }

    private static PrimitiveType exportType(String name, DocValuesType dvType) {
        switch (dvType) {
            case NUMERIC:
                return new PrimitiveType(PrimitiveType.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, name);
            case SORTED_NUMERIC:
                return new PrimitiveType(PrimitiveType.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, name);
            case BINARY:
                return new PrimitiveType(PrimitiveType.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, name);
            case SORTED:
                return new PrimitiveType(PrimitiveType.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, name);
            case SORTED_SET:
                return new PrimitiveType(PrimitiveType.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, name);
            default:
                throw new IllegalArgumentException("Unsupported doc values type: " + dvType);
        }
    }

    private static ColumnData encodeColumn(String fieldName, DocValuesType dvType, IndexSearcher searcher, long totalRows)
        throws IOException {
        PrimitiveType parquetType = exportType(fieldName, dvType);
        MessageType schema = new MessageType("schema", parquetType);
        ColumnDescriptor colDesc = schema.getColumns().get(0);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore();
        ParquetProperties props = ParquetProperties.builder().build();
        ColumnWriteStore writeStore = props.newColumnWriteStore(schema, pageStore);
        ColumnWriter writer = writeStore.getColumnWriter(colDesc);

        long numValues = 0;
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            numValues += writeFieldValues(writer, ctx.reader(), fieldName, dvType);
        }

        writeStore.flush();
        writeStore.close();

        return new ColumnData(new ArrayList<>(pageStore.writer.pages), pageStore.writer.dictPage, numValues);
    }

    private static long writeFieldValues(ColumnWriter writer, LeafReader reader, String fieldName, DocValuesType dvType)
        throws IOException {
        int maxDoc = reader.maxDoc();
        long valuesWritten = 0;

        switch (dvType) {
            case NUMERIC: {
                NumericDocValues dv = reader.getNumericDocValues(fieldName);
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (dv != null && dv.advanceExact(doc)) {
                        writer.write(dv.longValue(), 0, 1); // rep=0, def=1
                        valuesWritten++;
                    } else {
                        writer.writeNull(0, 0); // rep=0, def=0
                        valuesWritten++;
                    }
                }
                break;
            }
            case SORTED_NUMERIC: {
                SortedNumericDocValues dv = reader.getSortedNumericDocValues(fieldName);
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (dv != null && dv.advanceExact(doc)) {
                        int count = dv.docValueCount();
                        for (int i = 0; i < count; i++) {
                            writer.write(dv.nextValue(), i == 0 ? 0 : 1, 1);
                            valuesWritten++;
                        }
                    } else {
                        writer.writeNull(0, 0);
                        valuesWritten++;
                    }
                }
                break;
            }
            case BINARY: {
                BinaryDocValues dv = reader.getBinaryDocValues(fieldName);
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (dv != null && dv.advanceExact(doc)) {
                        byte[] bytes = new byte[dv.binaryValue().length];
                        System.arraycopy(dv.binaryValue().bytes, dv.binaryValue().offset, bytes, 0, dv.binaryValue().length);
                        writer.write(Binary.fromConstantByteArray(bytes), 0, 1);
                        valuesWritten++;
                    } else {
                        writer.writeNull(0, 0);
                        valuesWritten++;
                    }
                }
                break;
            }
            case SORTED: {
                SortedDocValues dv = reader.getSortedDocValues(fieldName);
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (dv != null && dv.advanceExact(doc)) {
                        org.apache.lucene.util.BytesRef ref = dv.lookupOrd(dv.ordValue());
                        byte[] bytes = new byte[ref.length];
                        System.arraycopy(ref.bytes, ref.offset, bytes, 0, ref.length);
                        writer.write(Binary.fromConstantByteArray(bytes), 0, 1);
                        valuesWritten++;
                    } else {
                        writer.writeNull(0, 0);
                        valuesWritten++;
                    }
                }
                break;
            }
            case SORTED_SET: {
                SortedSetDocValues dv = reader.getSortedSetDocValues(fieldName);
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (dv != null && dv.advanceExact(doc)) {
                        int count = dv.docValueCount();
                        for (int i = 0; i < count; i++) {
                            long ord = dv.nextOrd();
                            org.apache.lucene.util.BytesRef ref = dv.lookupOrd(ord);
                            byte[] bytes = new byte[ref.length];
                            System.arraycopy(ref.bytes, ref.offset, bytes, 0, ref.length);
                            writer.write(Binary.fromConstantByteArray(bytes), i == 0 ? 0 : 1, 1);
                            valuesWritten++;
                        }
                    } else {
                        writer.writeNull(0, 0);
                        valuesWritten++;
                    }
                }
                break;
            }
            default:
                break;
        }
        return valuesWritten;
    }

    private static long writeParquetFile(Path outputFile, Map<String, DocValuesType> fields, List<ColumnData> columns, long totalRows)
        throws IOException {
        Files.createDirectories(outputFile.getParent());

        try (OutputStream out = Files.newOutputStream(outputFile)) {
            // Write magic
            out.write(MAGIC);
            long offset = MAGIC.length;

            List<ColumnChunk> columnChunks = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>(fields.keySet());

            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DocValuesType dvType = fields.get(fieldName);
                ColumnData colData = columns.get(i);

                long chunkStart = offset;
                long dictPageOffset = -1;
                long dataPageOffset = offset;
                List<org.apache.parquet.format.Encoding> encodings = new ArrayList<>();

                // Write dictionary page
                if (colData.dictPage != null) {
                    dictPageOffset = offset;
                    org.apache.parquet.format.Encoding dictEnc = convertEncoding(colData.dictPage.encoding);
                    encodings.add(dictEnc);

                    PageHeader ph = new PageHeader(PageType.DICTIONARY_PAGE, colData.dictPage.bytes.length, colData.dictPage.bytes.length);
                    ph.setDictionary_page_header(new DictionaryPageHeader(colData.dictPage.dictSize, dictEnc));

                    byte[] headerBytes = serializePageHeader(ph);
                    out.write(headerBytes);
                    out.write(colData.dictPage.bytes);
                    offset += headerBytes.length + colData.dictPage.bytes.length;
                    dataPageOffset = offset;
                }

                // Write data pages
                for (CapturedPage page : colData.dataPages) {
                    if (dataPageOffset == offset && dictPageOffset >= 0) {
                        dataPageOffset = offset;
                    } else if (dictPageOffset < 0 && dataPageOffset == chunkStart) {
                        dataPageOffset = offset;
                    }

                    org.apache.parquet.format.Encoding valEnc = convertEncoding(page.valuesEncoding);
                    org.apache.parquet.format.Encoding rlEnc = convertEncoding(page.rlEncoding);
                    org.apache.parquet.format.Encoding dlEnc = convertEncoding(page.dlEncoding);

                    if (encodings.contains(valEnc) == false) encodings.add(valEnc);
                    if (encodings.contains(rlEnc) == false) encodings.add(rlEnc);
                    if (encodings.contains(dlEnc) == false) encodings.add(dlEnc);

                    PageHeader ph = new PageHeader(PageType.DATA_PAGE, page.bytes.length, page.bytes.length);
                    ph.setData_page_header(new DataPageHeader(page.valueCount, valEnc, dlEnc, rlEnc));

                    byte[] headerBytes = serializePageHeader(ph);
                    out.write(headerBytes);
                    out.write(page.bytes);
                    offset += headerBytes.length + page.bytes.length;
                }

                long totalSize = offset - chunkStart;

                // Build ColumnMetaData
                Type parquetType = (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC)
                    ? Type.INT64
                    : Type.BYTE_ARRAY;

                ColumnMetaData colMeta = new ColumnMetaData(
                    parquetType,
                    encodings,
                    Collections.singletonList(fieldName),
                    CompressionCodec.UNCOMPRESSED,
                    colData.numValues,
                    totalSize,
                    totalSize,
                    dataPageOffset
                );
                if (dictPageOffset >= 0) {
                    colMeta.setDictionary_page_offset(dictPageOffset);
                }

                ColumnChunk cc = new ColumnChunk(chunkStart);
                cc.setMeta_data(colMeta);
                columnChunks.add(cc);
            }

            long totalByteSize = offset - MAGIC.length;

            // Build RowGroup
            RowGroup rowGroup = new RowGroup(columnChunks, totalByteSize, totalRows);

            // Build schema
            List<SchemaElement> schema = new ArrayList<>();
            SchemaElement root = new SchemaElement("schema");
            root.setNum_children(fields.size());
            schema.add(root);

            for (Map.Entry<String, DocValuesType> entry : fields.entrySet()) {
                SchemaElement elem = new SchemaElement(entry.getKey());
                DocValuesType dvType = entry.getValue();

                if (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC) {
                    elem.setType(Type.INT64);
                } else {
                    elem.setType(Type.BYTE_ARRAY);
                }

                if (dvType == DocValuesType.SORTED_NUMERIC || dvType == DocValuesType.SORTED_SET) {
                    elem.setRepetition_type(FieldRepetitionType.REPEATED);
                } else {
                    elem.setRepetition_type(FieldRepetitionType.OPTIONAL);
                }

                schema.add(elem);
            }

            // Build FileMetaData
            FileMetaData fileMetaData = new FileMetaData(1, schema, totalRows, Collections.singletonList(rowGroup));
            fileMetaData.setCreated_by("OpenSearch Parquet Export");

            // Serialize footer
            byte[] footerBytes = serializeFileMetaData(fileMetaData);
            out.write(footerBytes);

            // Write footer length (4 bytes, little-endian)
            byte[] footerLen = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array();
            out.write(footerLen);

            // Write magic
            out.write(MAGIC);

            return offset + footerBytes.length + 4 + MAGIC.length;
        }
    }

    private static org.apache.parquet.format.Encoding convertEncoding(Encoding encoding) {
        return org.apache.parquet.format.Encoding.valueOf(encoding.name());
    }

    private static byte[] serializePageHeader(PageHeader header) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Util.writePageHeader(header, baos);
        return baos.toByteArray();
    }

    private static byte[] serializeFileMetaData(FileMetaData metadata) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Util.writeFileMetaData(metadata, baos);
        return baos.toByteArray();
    }
}
