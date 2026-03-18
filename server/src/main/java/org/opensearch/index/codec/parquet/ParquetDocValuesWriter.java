/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes Lucene doc values using Parquet column encoding.
 *
 * <p>Each field is encoded as a Parquet column chunk using {@code ColumnWriteStoreV1}
 * with a custom in-memory {@link PageWriteStore}. Encoded pages are serialized to
 * a {@code .pdvd} data file, with field metadata (offsets, page counts, types)
 * written to a {@code .pdvm} metadata file.</p>
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesWriter extends DocValuesConsumer {

    /** Codec name written into file headers for validation on read. */
    static final String DATA_CODEC = "ParquetDocValuesData";
    /** Codec name for the metadata file. */
    static final String META_CODEC = "ParquetDocValuesMeta";
    /** Current format version. */
    static final int VERSION_CURRENT = 0;

    /** Marker written to metadata to signal end of field entries. */
    static final String END_MARKER = "__END__";

    private final IndexOutput dataOut;
    private final IndexOutput metaOut;

    public ParquetDocValuesWriter(SegmentWriteState state, String dataExtension, String metaExtension, String dataCodec, String metaCodec)
        throws IOException {
        String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        boolean success = false;
        try {
            dataOut = state.directory.createOutput(dataFileName, state.context);
            CodecUtil.writeIndexHeader(dataOut, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            metaOut = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(metaOut, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    public ParquetDocValuesWriter(SegmentWriteState state, String dataExtension, String metaExtension) throws IOException {
        this(state, dataExtension, metaExtension, DATA_CODEC, META_CODEC);
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.numericType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore(descriptor);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        NumericDocValues values = valuesProducer.getNumeric(field);
        while (values.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
            cw.write(values.longValue(), 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        writeFieldData(field.name, field.getDocValuesType().name(), parquetType, pageStore.capturingWriter());
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore(descriptor);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != BinaryDocValues.NO_MORE_DOCS) {
            BytesRef br = values.binaryValue();
            cw.write(Binary.fromConstantByteArray(br.bytes, br.offset, br.length), 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        writeFieldData(field.name, field.getDocValuesType().name(), parquetType, pageStore.capturingWriter());
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore(descriptor);
        ColumnWriteStore writeStore = ParquetProperties.builder()
            .withDictionaryEncoding(true)
            .build()
            .newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        SortedDocValues values = valuesProducer.getSorted(field);
        while (values.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
            BytesRef br = values.lookupOrd(values.ordValue());
            cw.write(Binary.fromConstantByteArray(br.bytes, br.offset, br.length), 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        writeFieldData(field.name, field.getDocValuesType().name(), parquetType, pageStore.capturingWriter());
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedNumericType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore(descriptor);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        while (values.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
            int count = values.docValueCount();
            if (count == 0) {
                cw.writeNull(0, 0);
            } else {
                cw.write(values.nextValue(), 0, 1);
                for (int i = 1; i < count; i++) {
                    cw.write(values.nextValue(), 1, 1);
                }
            }
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        writeFieldData(field.name, field.getDocValuesType().name(), parquetType, pageStore.capturingWriter());
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedSetType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        CapturingPageWriteStore pageStore = new CapturingPageWriteStore(descriptor);
        ColumnWriteStore writeStore = ParquetProperties.builder()
            .withDictionaryEncoding(true)
            .build()
            .newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        SortedSetDocValues values = valuesProducer.getSortedSet(field);
        while (values.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
            int count = values.docValueCount();
            if (count == 0) {
                cw.writeNull(0, 0);
            } else {
                for (int i = 0; i < count; i++) {
                    long ord = values.nextOrd();
                    BytesRef br = values.lookupOrd(ord);
                    int rep = (i == 0) ? 0 : 1;
                    cw.write(Binary.fromConstantByteArray(br.bytes, br.offset, br.length), rep, 1);
                }
            }
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        writeFieldData(field.name, field.getDocValuesType().name(), parquetType, pageStore.capturingWriter());
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (metaOut != null) {
                metaOut.writeString(END_MARKER);
                CodecUtil.writeFooter(metaOut);
            }
            if (dataOut != null) {
                CodecUtil.writeFooter(dataOut);
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(metaOut, dataOut);
            } else {
                IOUtils.closeWhileHandlingException(metaOut, dataOut);
            }
        }
    }

    /**
     * Writes captured Parquet pages for one field to the data file and records
     * metadata (field name, type, offset, page count) in the meta file.
     */
    private void writeFieldData(String fieldName, String dvTypeName, PrimitiveType parquetType, CapturingPageWriter pageWriter)
        throws IOException {
        long dataStartOffset = dataOut.getFilePointer();

        DictionaryPage dictPage = pageWriter.getDictionaryPage();
        boolean hasDictionary = dictPage != null;
        if (hasDictionary) {
            byte[] dictBytes = dictPage.getBytes().toByteArray();
            dataOut.writeInt(dictBytes.length);
            dataOut.writeInt(dictPage.getDictionarySize());
            dataOut.writeString(dictPage.getEncoding().name());
            dataOut.writeBytes(dictBytes, dictBytes.length);
        }

        List<CapturedPage> pages = pageWriter.getPages();
        for (CapturedPage page : pages) {
            byte[] pageBytes = page.bytes.toByteArray();
            dataOut.writeInt(pageBytes.length);
            dataOut.writeInt(page.valueCount);
            dataOut.writeInt(page.rowCount);
            dataOut.writeString(page.valuesEncoding.name());
            dataOut.writeString(page.rlEncoding.name());
            dataOut.writeString(page.dlEncoding.name());
            dataOut.writeBytes(pageBytes, pageBytes.length);
        }

        long dataEndOffset = dataOut.getFilePointer();

        metaOut.writeString(fieldName);
        metaOut.writeString(dvTypeName);
        metaOut.writeString(parquetType.getRepetition().name());
        metaOut.writeString(parquetType.getPrimitiveTypeName().name());
        metaOut.writeLong(dataStartOffset);
        metaOut.writeLong(dataEndOffset - dataStartOffset);
        metaOut.writeInt(pages.size());
        metaOut.writeByte(hasDictionary ? (byte) 1 : (byte) 0);
    }

    /** Holds the raw bytes and metadata of a single captured data page. */
    static class CapturedPage {
        final BytesInput bytes;
        final int valueCount;
        final int rowCount;
        final Encoding valuesEncoding;
        final Encoding rlEncoding;
        final Encoding dlEncoding;

        CapturedPage(BytesInput bytes, int valueCount, int rowCount, Encoding valuesEncoding, Encoding rlEncoding, Encoding dlEncoding) {
            try {
                this.bytes = BytesInput.from(bytes.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException("Failed to copy page bytes", e);
            }
            this.valueCount = valueCount;
            this.rowCount = rowCount;
            this.valuesEncoding = valuesEncoding;
            this.rlEncoding = rlEncoding;
            this.dlEncoding = dlEncoding;
        }
    }

    /**
     * A {@link PageWriter} that captures pages in memory rather than writing to disk.
     * Used as the sink for {@code ColumnWriteStoreV1} during encoding.
     */
    static class CapturingPageWriter implements PageWriter {
        private final List<CapturedPage> pages = new ArrayList<>();
        private DictionaryPage dictionaryPage;
        private long memSize;

        @Override
        public void writePage(
            BytesInput bytes,
            int valueCount,
            Statistics<?> statistics,
            Encoding rlEncoding,
            Encoding dlEncoding,
            Encoding valuesEncoding
        ) throws IOException {
            writePage(bytes, valueCount, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
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
            pages.add(new CapturedPage(bytes, valueCount, rowCount, valuesEncoding, rlEncoding, dlEncoding));
            memSize += bytes.size();
        }

        @Override
        public void writePage(
            BytesInput bytes,
            int valueCount,
            int rowCount,
            Statistics<?> statistics,
            SizeStatistics sizeStatistics,
            Encoding rlEncoding,
            Encoding dlEncoding,
            Encoding valuesEncoding
        ) throws IOException {
            writePage(bytes, valueCount, rowCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
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
            pages.add(new CapturedPage(combined, valueCount, rowCount, dataEncoding, Encoding.RLE, Encoding.RLE));
            memSize += combined.size();
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
            byte[] dictBytes = dictionaryPage.getBytes().toByteArray();
            this.dictionaryPage = new DictionaryPage(
                BytesInput.from(dictBytes),
                dictionaryPage.getDictionarySize(),
                dictionaryPage.getEncoding()
            );
            memSize += dictBytes.length;
        }

        @Override
        public String memUsageString(String prefix) {
            return prefix + " CapturingPageWriter: " + memSize + " bytes";
        }

        @Override
        public void close() {}

        List<CapturedPage> getPages() {
            return pages;
        }

        DictionaryPage getDictionaryPage() {
            return dictionaryPage;
        }
    }

    /**
     * A {@link PageWriteStore} backed by a single {@link CapturingPageWriter}.
     * Each field uses one column, so one page writer suffices.
     */
    static class CapturingPageWriteStore implements PageWriteStore {
        private final CapturingPageWriter writer;

        CapturingPageWriteStore(ColumnDescriptor descriptor) {
            this.writer = new CapturingPageWriter();
        }

        @Override
        public PageWriter getPageWriter(ColumnDescriptor path) {
            return writer;
        }

        @Override
        public void close() {}

        CapturingPageWriter capturingWriter() {
            return writer;
        }
    }
}
