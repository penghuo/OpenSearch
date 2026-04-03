/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import com.github.luben.zstd.Zstd;

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
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.DirectWriter;
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
import java.util.Arrays;

/**
 * Writes Lucene doc values using Parquet column encoding with streaming page writes.
 *
 * <p>Pages are written directly to the {@code .pdvd} data file as they are produced
 * by the Parquet column writer, avoiding buffering all encoded data in memory.
 * Doc IDs are accumulated in a compact {@code int[]} and written after the pages.
 * Field metadata is written to the {@code .pdvm} metadata file.</p>
 *
 * <p>File format per field in .pdvd (version 2):
 * <pre>
 *   [ZSTD-compressed data pages...]
 *   [ZSTD-compressed dictionary page (if any)]
 *   [byte: denseFlag][int: docCount][int[]: docIds (if sparse)]
 * </pre>
 * Each data page: [int: compressedSize][int: uncompressedSize][int: valueCount]
 *                  [int: rowCount][String: valuesEncoding][String: rlEncoding]
 *                  [String: dlEncoding][byte[]: ZSTD-compressed page data]
 * Dictionary page: [int: compressedSize][int: uncompressedSize][int: dictSize]
 *                  [String: encoding][byte[]: ZSTD-compressed dict data]
 * Metadata per field in .pdvm includes a {@code docIdOffset} so the reader can
 * locate the doc ID section without scanning pages.
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesWriter extends DocValuesConsumer {

    static final String DATA_CODEC = "ParquetDocValuesData";
    static final String META_CODEC = "ParquetDocValuesMeta";
    static final int VERSION_START = 2;
    static final int VERSION_PACKED_VALUES = 3;
    static final int VERSION_BLOCK_PACKED = 4;
    static final int VERSION_SKIP_INDEX = 5;
    static final int VERSION_CURRENT = VERSION_SKIP_INDEX;

    /** Block shift for block-based packed values encoding (2^14 = 16384 docs per block, matching Lucene90). */
    static final int BLOCK_SHIFT = 14;
    static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
    static final String END_MARKER = "__END__";

    /** Flag byte: field has a value for every doc in the segment (no doc IDs stored). */
    static final byte DENSE_FLAG = 1;
    /** Flag byte: field is sparse — doc IDs stored explicitly. */
    static final byte SPARSE_FLAG = 0;

    private final IndexOutput dataOut;
    private final IndexOutput metaOut;
    private final int maxDoc;

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
            maxDoc = state.segmentInfo.maxDoc();
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

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut);
        StreamingPageWriteStore pageStore = new StreamingPageWriteStore(pageWriter);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        IntArrayBuilder docIds = new IntArrayBuilder();
        LongArrayBuilder collectedValues = new LongArrayBuilder();
        NumericDocValues values = valuesProducer.getNumeric(field);
        while (values.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
            docIds.add(values.docID());
            long v = values.longValue();
            collectedValues.add(v);
            cw.write(v, 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        pageWriter.writeDictionaryPageToOutput();
        long docIdOffset = dataOut.getFilePointer() - dataStartOffset;
        writeDocIds(docIds);

        // Write packed values section for mmap'd DirectReader access (version 3+)
        long packedValuesOffset = writePackedValues(collectedValues);

        long dataLength = dataOut.getFilePointer() - dataStartOffset;

        writeFieldMeta(
            field.name,
            field.getDocValuesType().name(),
            parquetType,
            dataStartOffset,
            dataLength,
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset
        );
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut);
        StreamingPageWriteStore pageStore = new StreamingPageWriteStore(pageWriter);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        IntArrayBuilder docIds = new IntArrayBuilder();
        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != BinaryDocValues.NO_MORE_DOCS) {
            docIds.add(values.docID());
            BytesRef br = values.binaryValue();
            cw.write(Binary.fromReusedByteArray(br.bytes, br.offset, br.length), 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        pageWriter.writeDictionaryPageToOutput();
        long docIdOffset = dataOut.getFilePointer() - dataStartOffset;
        writeDocIds(docIds);
        long dataLength = dataOut.getFilePointer() - dataStartOffset;

        writeFieldMeta(
            field.name,
            field.getDocValuesType().name(),
            parquetType,
            dataStartOffset,
            dataLength,
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset
        );
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut);
        StreamingPageWriteStore pageStore = new StreamingPageWriteStore(pageWriter);
        ColumnWriteStore writeStore = ParquetProperties.builder()
            .withDictionaryEncoding(true)
            .build()
            .newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        IntArrayBuilder docIds = new IntArrayBuilder();
        SortedDocValues values = valuesProducer.getSorted(field);
        while (values.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
            docIds.add(values.docID());
            BytesRef br = values.lookupOrd(values.ordValue());
            cw.write(Binary.fromReusedByteArray(br.bytes, br.offset, br.length), 0, 0);
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        pageWriter.writeDictionaryPageToOutput();
        long docIdOffset = dataOut.getFilePointer() - dataStartOffset;
        writeDocIds(docIds);
        long dataLength = dataOut.getFilePointer() - dataStartOffset;

        writeFieldMeta(
            field.name,
            field.getDocValuesType().name(),
            parquetType,
            dataStartOffset,
            dataLength,
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset
        );
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedNumericType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut);
        StreamingPageWriteStore pageStore = new StreamingPageWriteStore(pageWriter);
        ColumnWriteStore writeStore = ParquetProperties.builder().build().newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        IntArrayBuilder docIds = new IntArrayBuilder();
        // Collect singleton values for packed encoding; set to null if multi-valued detected
        LongArrayBuilder singletonValues = new LongArrayBuilder();
        boolean isSingleton = true;
        SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        while (values.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
            docIds.add(values.docID());
            int count = values.docValueCount();
            if (count == 0) {
                cw.writeNull(0, 0);
                isSingleton = false;
            } else {
                long firstVal = values.nextValue();
                cw.write(firstVal, 0, 1);
                if (count == 1 && isSingleton) {
                    singletonValues.add(firstVal);
                } else {
                    isSingleton = false;
                }
                for (int i = 1; i < count; i++) {
                    cw.write(values.nextValue(), 1, 1);
                }
            }
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        pageWriter.writeDictionaryPageToOutput();
        long docIdOffset = dataOut.getFilePointer() - dataStartOffset;
        writeDocIds(docIds);

        // Write packed values only for singleton sorted numeric fields
        long packedValuesOffset = (isSingleton && singletonValues.size() > 0) ? writePackedValues(singletonValues) : -1L;

        long dataLength = dataOut.getFilePointer() - dataStartOffset;

        writeFieldMeta(
            field.name,
            field.getDocValuesType().name(),
            parquetType,
            dataStartOffset,
            dataLength,
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset
        );
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedSetType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut);
        StreamingPageWriteStore pageStore = new StreamingPageWriteStore(pageWriter);
        ColumnWriteStore writeStore = ParquetProperties.builder()
            .withDictionaryEncoding(true)
            .build()
            .newColumnWriteStore(schema, pageStore);
        ColumnWriter cw = writeStore.getColumnWriter(descriptor);

        IntArrayBuilder docIds = new IntArrayBuilder();
        SortedSetDocValues values = valuesProducer.getSortedSet(field);
        while (values.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
            docIds.add(values.docID());
            int count = values.docValueCount();
            if (count == 0) {
                cw.writeNull(0, 0);
            } else {
                for (int i = 0; i < count; i++) {
                    long ord = values.nextOrd();
                    BytesRef br = values.lookupOrd(ord);
                    int rep = (i == 0) ? 0 : 1;
                    cw.write(Binary.fromReusedByteArray(br.bytes, br.offset, br.length), rep, 1);
                }
            }
            writeStore.endRecord();
        }
        writeStore.flush();
        writeStore.close();

        pageWriter.writeDictionaryPageToOutput();
        long docIdOffset = dataOut.getFilePointer() - dataStartOffset;
        writeDocIds(docIds);
        long dataLength = dataOut.getFilePointer() - dataStartOffset;

        writeFieldMeta(
            field.name,
            field.getDocValuesType().name(),
            parquetType,
            dataStartOffset,
            dataLength,
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset
        );
    }

    /**
     * Writes block-based bit-packed values using Lucene's DirectWriter for mmap'd O(1) random access.
     * Each block of BLOCK_SIZE docs has its own minValue, maxValue, gcd, and bitsPerValue for better compression.
     * Format:
     *   [int: numBlocks]
     *   [block index: numBlocks × (long: blockOffset, long: blockMinValue, long: blockMaxValue, long: blockGcd, byte: blockBitsPerValue)]
     *   [block data: for each block, DirectWriter packed data]
     * Block index is 33 bytes per block. Block offsets are relative to the start of block data section.
     * The per-block minValue/maxValue enable DocValuesSkipper in the reader for skipping non-competitive blocks.
     * Returns the absolute file offset where packed section begins.
     */
    private long writePackedValues(LongArrayBuilder values) throws IOException {
        if (values.size() == 0) {
            return -1L;
        }
        long packedValuesOffset = dataOut.getFilePointer();
        int numValues = values.size();
        int numBlocks = (numValues + BLOCK_SIZE - 1) >>> BLOCK_SHIFT;

        // Compute per-block stats
        long[] blockMinValues = new long[numBlocks];
        long[] blockMaxValues = new long[numBlocks];
        long[] blockGcds = new long[numBlocks];
        int[] blockBitsPerValue = new int[numBlocks];

        for (int b = 0; b < numBlocks; b++) {
            int start = b << BLOCK_SHIFT;
            int end = Math.min(start + BLOCK_SIZE, numValues);

            long minValue = values.get(start);
            long maxValue = values.get(start);
            for (int i = start + 1; i < end; i++) {
                long v = values.get(i);
                minValue = Math.min(minValue, v);
                maxValue = Math.max(maxValue, v);
            }

            long gcd = 0;
            if (minValue != maxValue) {
                gcd = maxValue - minValue;
                for (int i = start; i < end; i++) {
                    long v = values.get(i);
                    if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
                        gcd = 1;
                        break;
                    }
                    gcd = MathUtil.gcd(gcd, v - minValue);
                }
            }

            long maxDelta = gcd != 0 ? (maxValue - minValue) / gcd : 0;
            int bpv = (minValue != maxValue) ? DirectWriter.unsignedBitsRequired(maxDelta) : 0;

            blockMinValues[b] = minValue;
            blockMaxValues[b] = maxValue;
            blockGcds[b] = gcd;
            blockBitsPerValue[b] = bpv;
        }

        // Pre-compute block offsets using DirectWriter.bytesRequired
        long[] blockOffsets = new long[numBlocks];
        long runningOffset = 0;
        for (int b = 0; b < numBlocks; b++) {
            blockOffsets[b] = runningOffset;
            int start = b << BLOCK_SHIFT;
            int end = Math.min(start + BLOCK_SIZE, numValues);
            int blockCount = end - start;
            if (blockBitsPerValue[b] > 0) {
                runningOffset += DirectWriter.bytesRequired(blockCount, blockBitsPerValue[b]);
            }
        }

        // Write header: number of blocks
        dataOut.writeInt(numBlocks);

        // Write block index with pre-computed offsets (33 bytes per block)
        for (int b = 0; b < numBlocks; b++) {
            dataOut.writeLong(blockOffsets[b]);
            dataOut.writeLong(blockMinValues[b]);
            dataOut.writeLong(blockMaxValues[b]);
            dataOut.writeLong(blockGcds[b]);
            dataOut.writeByte((byte) blockBitsPerValue[b]);
        }

        // Write block data
        for (int b = 0; b < numBlocks; b++) {
            int start = b << BLOCK_SHIFT;
            int end = Math.min(start + BLOCK_SIZE, numValues);
            int blockCount = end - start;
            int bpv = blockBitsPerValue[b];

            if (bpv > 0) {
                DirectWriter writer = DirectWriter.getInstance(dataOut, blockCount, bpv);
                long minVal = blockMinValues[b];
                long gcd = blockGcds[b];
                for (int i = start; i < end; i++) {
                    writer.add(gcd != 0 ? (values.get(i) - minVal) / gcd : 0);
                }
                writer.finish();
            }
        }

        return packedValuesOffset;
    }

    private void writeDocIds(IntArrayBuilder docIds) throws IOException {
        if (docIds.size() == maxDoc) {
            // Dense field: every doc has a value, no need to store doc IDs
            dataOut.writeByte(DENSE_FLAG);
            dataOut.writeInt(docIds.size());
        } else {
            // Sparse field: store doc IDs explicitly
            dataOut.writeByte(SPARSE_FLAG);
            dataOut.writeInt(docIds.size());
            for (int i = 0; i < docIds.size(); i++) {
                dataOut.writeInt(docIds.get(i));
            }
        }
    }

    private void writeFieldMeta(
        String fieldName,
        String dvTypeName,
        PrimitiveType parquetType,
        long dataStartOffset,
        long dataLength,
        int pageCount,
        boolean hasDictionary,
        long docIdOffset
    ) throws IOException {
        metaOut.writeString(fieldName);
        metaOut.writeString(dvTypeName);
        metaOut.writeString(parquetType.getRepetition().name());
        metaOut.writeString(parquetType.getPrimitiveTypeName().name());
        metaOut.writeLong(dataStartOffset);
        metaOut.writeLong(dataLength);
        metaOut.writeInt(pageCount);
        metaOut.writeByte(hasDictionary ? (byte) 1 : (byte) 0);
        metaOut.writeLong(docIdOffset);
    }

    private boolean closed;

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
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
     * A {@link PageWriter} that streams data pages directly to an {@link IndexOutput},
     * avoiding in-memory buffering. Dictionary pages are deferred until after all data
     * pages are written (they arrive during flush, after data pages).
     */
    static class StreamingPageWriter implements PageWriter {
        private final IndexOutput out;
        private int pageCount;
        private DictionaryPage dictionaryPage;

        StreamingPageWriter(IndexOutput out) {
            this.out = out;
        }

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
            byte[] pageBytes = bytes.toByteArray();
            byte[] compressedBytes = Zstd.compress(pageBytes);
            out.writeInt(compressedBytes.length);
            out.writeInt(pageBytes.length);
            out.writeInt(valueCount);
            out.writeInt(rowCount);
            out.writeString(valuesEncoding.name());
            out.writeString(rlEncoding.name());
            out.writeString(dlEncoding.name());
            out.writeBytes(compressedBytes, compressedBytes.length);
            pageCount++;
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
            writePage(combined, valueCount, rowCount, statistics, Encoding.RLE, Encoding.RLE, dataEncoding);
        }

        @Override
        public long getMemSize() {
            return 0;
        }

        @Override
        public long allocatedSize() {
            return 0;
        }

        @Override
        public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
            // Buffer the dictionary page — it arrives during flush and must be written
            // after all data pages but before doc IDs
            byte[] dictBytes = dictionaryPage.getBytes().toByteArray();
            this.dictionaryPage = new DictionaryPage(
                BytesInput.from(dictBytes),
                dictionaryPage.getDictionarySize(),
                dictionaryPage.getEncoding()
            );
        }

        /** Writes the buffered dictionary page to the output, if any. */
        void writeDictionaryPageToOutput() throws IOException {
            if (dictionaryPage != null) {
                byte[] dictBytes = dictionaryPage.getBytes().toByteArray();
                byte[] compressedBytes = Zstd.compress(dictBytes);
                out.writeInt(compressedBytes.length);
                out.writeInt(dictBytes.length);
                out.writeInt(dictionaryPage.getDictionarySize());
                out.writeString(dictionaryPage.getEncoding().name());
                out.writeBytes(compressedBytes, compressedBytes.length);
            }
        }

        @Override
        public String memUsageString(String prefix) {
            return prefix + " StreamingPageWriter: 0 bytes (streaming)";
        }

        @Override
        public void close() {}

        int getPageCount() {
            return pageCount;
        }

        boolean hasDictionary() {
            return dictionaryPage != null;
        }
    }

    /**
     * A {@link PageWriteStore} backed by a single {@link StreamingPageWriter}.
     */
    static class StreamingPageWriteStore implements PageWriteStore {
        private final StreamingPageWriter writer;

        StreamingPageWriteStore(StreamingPageWriter writer) {
            this.writer = writer;
        }

        @Override
        public PageWriter getPageWriter(ColumnDescriptor path) {
            return writer;
        }

        @Override
        public void close() {}
    }

    /**
     * Growable primitive int array to avoid boxing overhead of {@code ArrayList<Integer>}.
     * For 181M docs, this uses ~724MB vs ~2.9GB with boxed Integers.
     */
    static class IntArrayBuilder {
        private int[] data;
        private int size;

        IntArrayBuilder() {
            this.data = new int[1024];
        }

        void add(int value) {
            if (size == data.length) {
                data = Arrays.copyOf(data, data.length + (data.length >> 1));
            }
            data[size++] = value;
        }

        int get(int index) {
            return data[index];
        }

        int size() {
            return size;
        }
    }

    /**
     * Growable primitive long array for collecting numeric values during indexing.
     * Used to write packed values section for mmap'd DirectReader access.
     */
    static class LongArrayBuilder {
        private long[] data;
        private int size;

        LongArrayBuilder() {
            this.data = new long[1024];
        }

        void add(long value) {
            if (size == data.length) {
                data = Arrays.copyOf(data, data.length + (data.length >> 1));
            }
            data[size++] = value;
        }

        long get(int index) {
            return data[index];
        }

        int size() {
            return size;
        }
    }
}
