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
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.opensearch.common.util.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Writes Lucene doc values using Parquet column encoding with streaming page writes.
 *
 * <p>Pages are written directly to the {@code .pdvd} data file as they are produced
 * by the Parquet column writer, avoiding buffering all encoded data in memory.
 * Doc IDs are accumulated in a compact {@code int[]} and written after the pages.
 * Field metadata is stored in the Parquet footer at the end of the file.</p>
 *
 * <p>V7 format: the {@code .pdvd} file is a valid Parquet file:
 * <pre>
 *   PAR1 (4 bytes)
 *   [per-field column data: Parquet PageHeaders + ZSTD-compressed pages...]
 *   [per-field: doc IDs + optional packed values (Lucene-specific appendix)]
 *   Parquet footer (Thrift FileMetaData)
 *   footer length (4 bytes LE)
 *   PAR1 (4 bytes)
 * </pre>
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
    static final int VERSION_FLAT_PACKED = 6;
    static final int VERSION_PARQUET_NATIVE = 7;
    static final int VERSION_CURRENT = VERSION_PARQUET_NATIVE;

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
    private final String segmentIdHex;
    private final String segmentSuffix;
    private final List<ParquetFooterWriter.ColumnChunkInfo> columnChunks;
    private final boolean parquetNativeFormat;

    /**
     * V7 constructor: writes CodecUtil header + PAR1 magic, Parquet PageHeaders, and Parquet footer.
     * No metadata file is created. The CodecUtil header is required by Lucene's compound file format.
     * External Parquet readers skip the CodecUtil header bytes to find the PAR1 magic.
     */
    public ParquetDocValuesWriter(SegmentWriteState state, String dataExtension) throws IOException {
        String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        boolean success = false;
        try {
            dataOut = state.directory.createOutput(dataFileName, state.context);
            // Write Lucene CodecUtil header first (required by compound file format)
            CodecUtil.writeIndexHeader(dataOut, DATA_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            // Then write PAR1 magic for Parquet readers (at a known offset after CodecUtil header)
            dataOut.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, ParquetFooterWriter.PARQUET_MAGIC.length);
            metaOut = null;
            maxDoc = state.segmentInfo.maxDoc();
            segmentIdHex = bytesToHex(state.segmentInfo.getId());
            segmentSuffix = state.segmentSuffix;
            columnChunks = new ArrayList<>();
            parquetNativeFormat = true;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    /**
     * Legacy constructor for backward compatibility. Delegates to old CodecUtil-based format.
     */
    public ParquetDocValuesWriter(SegmentWriteState state, String dataExtension, String metaExtension, String dataCodec, String metaCodec)
        throws IOException {
        String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        boolean success = false;
        try {
            dataOut = state.directory.createOutput(dataFileName, state.context);
            CodecUtil.writeIndexHeader(dataOut, dataCodec, VERSION_FLAT_PACKED, state.segmentInfo.getId(), state.segmentSuffix);
            metaOut = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(metaOut, metaCodec, VERSION_FLAT_PACKED, state.segmentInfo.getId(), state.segmentSuffix);
            maxDoc = state.segmentInfo.maxDoc();
            segmentIdHex = bytesToHex(state.segmentInfo.getId());
            segmentSuffix = state.segmentSuffix;
            columnChunks = new ArrayList<>();
            parquetNativeFormat = false;
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
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut, parquetNativeFormat);
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

        if (parquetNativeFormat) {
            addColumnChunkInfo(
                field.name,
                field.getDocValuesType().name(),
                parquetType,
                Type.INT64,
                dataStartOffset,
                pageWriter,
                docIdOffset,
                packedValuesOffset,
                docIds.size()
            );
        } else {
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
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut, parquetNativeFormat);
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

        if (parquetNativeFormat) {
            addColumnChunkInfo(
                field.name,
                field.getDocValuesType().name(),
                parquetType,
                Type.BYTE_ARRAY,
                dataStartOffset,
                pageWriter,
                docIdOffset,
                -1L,
                docIds.size()
            );
        } else {
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
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut, parquetNativeFormat);
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

        if (parquetNativeFormat) {
            addColumnChunkInfo(
                field.name,
                field.getDocValuesType().name(),
                parquetType,
                Type.BYTE_ARRAY,
                dataStartOffset,
                pageWriter,
                docIdOffset,
                -1L,
                docIds.size()
            );
        } else {
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
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedNumericType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut, parquetNativeFormat);
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

        if (parquetNativeFormat) {
            addColumnChunkInfo(
                field.name,
                field.getDocValuesType().name(),
                parquetType,
                Type.INT64,
                dataStartOffset,
                pageWriter,
                docIdOffset,
                packedValuesOffset,
                docIds.size()
            );
        } else {
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
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedSetType(field.name);
        MessageType schema = ParquetTypeMapping.messageType(field.name, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        long dataStartOffset = dataOut.getFilePointer();
        StreamingPageWriter pageWriter = new StreamingPageWriter(dataOut, parquetNativeFormat);
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

        if (parquetNativeFormat) {
            addColumnChunkInfo(
                field.name,
                field.getDocValuesType().name(),
                parquetType,
                Type.BYTE_ARRAY,
                dataStartOffset,
                pageWriter,
                docIdOffset,
                -1L,
                docIds.size()
            );
        } else {
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
    }

    /**
     * Builds a ColumnChunkInfo from the page writer state and adds it to the list
     * for the Parquet footer.
     */
    private void addColumnChunkInfo(
        String fieldName,
        String dvTypeName,
        PrimitiveType schemaType,
        Type parquetFormatType,
        long dataStartOffset,
        StreamingPageWriter pageWriter,
        long docIdOffset,
        long packedValuesOffset,
        int numValues
    ) {
        FieldRepetitionType repetition = convertRepetition(schemaType.getRepetition());
        columnChunks.add(new ParquetFooterWriter.ColumnChunkInfo(
            fieldName,
            dvTypeName,
            repetition,
            parquetFormatType,
            dataStartOffset,
            pageWriter.getFirstDataPageOffset(),
            pageWriter.getDictPageOffset(),
            pageWriter.getPageCount(),
            pageWriter.hasDictionary(),
            docIdOffset,
            packedValuesOffset,
            pageWriter.getTotalCompressedSize(),
            pageWriter.getTotalUncompressedSize(),
            numValues,
            pageWriter.getEncodings()
        ));
    }

    /**
     * Converts Parquet schema repetition to Thrift FieldRepetitionType.
     */
    private static FieldRepetitionType convertRepetition(org.apache.parquet.schema.Type.Repetition repetition) {
        switch (repetition) {
            case REQUIRED:
                return FieldRepetitionType.REQUIRED;
            case OPTIONAL:
                return FieldRepetitionType.OPTIONAL;
            case REPEATED:
                return FieldRepetitionType.REPEATED;
            default:
                throw new IllegalArgumentException("Unknown repetition: " + repetition);
        }
    }

    /**
     * Writes block-based bit-packed values using Lucene's DirectWriter for mmap'd O(1) random access.
     * Each block of BLOCK_SIZE docs has its own minValue, maxValue, gcd, and bitsPerValue for better compression.
     * Format:
     * V6 (VERSION_FLAT_PACKED) format:
     *   [int: numBlocks]
     *   [block skip index: numBlocks × (long: blockMinValue, long: blockMaxValue) = 16 bytes/block]
     *   [long: globalMinValue]
     *   [long: globalGcd]
     *   [byte: globalBitsPerValue]
     *   [flat DirectWriter packed data for ALL values]
     *
     * The block skip index provides per-block min/max for DocValuesSkipper competitive pruning.
     * The flat packed section uses a single DirectReader for O(1) access identical to Lucene90:
     *   value = globalMin + flatReader.get(idx) * globalGcd
     *
     * Returns the absolute file offset where packed section begins.
     */
    private long writePackedValues(LongArrayBuilder values) throws IOException {
        if (values.size() == 0) {
            return -1L;
        }
        long packedValuesOffset = dataOut.getFilePointer();
        int numValues = values.size();
        int numBlocks = (numValues + BLOCK_SIZE - 1) >>> BLOCK_SHIFT;

        // Compute global stats
        long globalMin = values.get(0);
        long globalMax = values.get(0);
        for (int i = 1; i < numValues; i++) {
            long v = values.get(i);
            globalMin = Math.min(globalMin, v);
            globalMax = Math.max(globalMax, v);
        }

        long globalGcd = 0;
        if (globalMin != globalMax) {
            globalGcd = globalMax - globalMin;
            for (int i = 0; i < numValues; i++) {
                long v = values.get(i);
                if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
                    globalGcd = 1;
                    break;
                }
                globalGcd = MathUtil.gcd(globalGcd, v - globalMin);
            }
        }

        long maxDelta = globalGcd != 0 ? (globalMax - globalMin) / globalGcd : 0;
        int globalBpv = (globalMin != globalMax) ? DirectWriter.unsignedBitsRequired(maxDelta) : 0;

        // Compute per-block min/max for skip index
        long[] blockMinValues = new long[numBlocks];
        long[] blockMaxValues = new long[numBlocks];
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
            blockMinValues[b] = minValue;
            blockMaxValues[b] = maxValue;
        }

        // Write block skip index header
        dataOut.writeInt(numBlocks);

        // Write block skip index (16 bytes per block: min + max only)
        for (int b = 0; b < numBlocks; b++) {
            dataOut.writeLong(blockMinValues[b]);
            dataOut.writeLong(blockMaxValues[b]);
        }

        // Write flat packed section header
        dataOut.writeLong(globalMin);
        dataOut.writeLong(globalGcd);
        dataOut.writeByte((byte) globalBpv);

        // Write flat packed data — single contiguous DirectWriter for all values
        if (globalBpv > 0) {
            DirectWriter writer = DirectWriter.getInstance(dataOut, numValues, globalBpv);
            for (int i = 0; i < numValues; i++) {
                writer.add(globalGcd != 0 ? (values.get(i) - globalMin) / globalGcd : 0);
            }
            writer.finish();
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

    /**
     * Writes field metadata to the legacy .pdvm metadata file.
     * Only used in non-Parquet-native (V6 and earlier) format.
     */
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
            if (parquetNativeFormat) {
                // Write Parquet footer
                if (dataOut != null) {
                    ParquetFooterWriter footerWriter = new ParquetFooterWriter(
                        String.valueOf(VERSION_CURRENT),
                        segmentIdHex,
                        segmentSuffix,
                        maxDoc,
                        maxDoc,
                        columnChunks
                    );
                    FileMetaData footer = footerWriter.buildFileMetaData();
                    byte[] footerBytes = ParquetFooterWriter.serializeFooter(footer);
                    dataOut.writeBytes(footerBytes, 0, footerBytes.length);
                    // 4-byte footer length little-endian
                    byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array();
                    dataOut.writeBytes(lenBytes, 0, 4);
                    // Trailing PAR1 magic
                    dataOut.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, ParquetFooterWriter.PARQUET_MAGIC.length);
                    // CodecUtil footer (required by Lucene compound file format)
                    CodecUtil.writeFooter(dataOut);
                }
            } else {
                // Legacy format: write CodecUtil footers
                if (metaOut != null) {
                    metaOut.writeString(END_MARKER);
                    CodecUtil.writeFooter(metaOut);
                }
                if (dataOut != null) {
                    CodecUtil.writeFooter(dataOut);
                }
            }
            success = true;
        } finally {
            if (success) {
                if (parquetNativeFormat) {
                    IOUtils.close(dataOut);
                } else {
                    IOUtils.close(metaOut, dataOut);
                }
            } else {
                if (parquetNativeFormat) {
                    IOUtils.closeWhileHandlingException(dataOut);
                } else {
                    IOUtils.closeWhileHandlingException(metaOut, dataOut);
                }
            }
        }
    }

    /**
     * Converts a byte array to a hex string.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    /**
     * A {@link PageWriter} that streams data pages directly to an {@link IndexOutput},
     * avoiding in-memory buffering. Dictionary pages are deferred until after all data
     * pages are written (they arrive during flush, after data pages).
     *
     * <p>When {@code parquetNative} is true, pages are written with standard Parquet
     * {@link PageHeader} Thrift structs. Otherwise, the legacy custom framing is used.</p>
     */
    static class StreamingPageWriter implements PageWriter {
        private final IndexOutput out;
        private final boolean parquetNative;
        private int pageCount;
        private DictionaryPage dictionaryPage;

        // Parquet-native tracking fields
        private long totalCompressedSize;
        private long totalUncompressedSize;
        private final List<org.apache.parquet.format.Encoding> encodings;
        private long firstDataPageOffset = -1;
        private long dictPageOffset = -1;

        StreamingPageWriter(IndexOutput out, boolean parquetNative) {
            this.out = out;
            this.parquetNative = parquetNative;
            this.encodings = new ArrayList<>();
        }

        StreamingPageWriter(IndexOutput out) {
            this(out, false);
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

            if (parquetNative) {
                if (firstDataPageOffset < 0) {
                    firstDataPageOffset = out.getFilePointer();
                }

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
                addEncoding(convertEncoding(valuesEncoding));
                addEncoding(convertEncoding(rlEncoding));
                addEncoding(convertEncoding(dlEncoding));
            } else {
                out.writeInt(compressedBytes.length);
                out.writeInt(pageBytes.length);
                out.writeInt(valueCount);
                out.writeInt(rowCount);
                out.writeString(valuesEncoding.name());
                out.writeString(rlEncoding.name());
                out.writeString(dlEncoding.name());
                out.writeBytes(compressedBytes, compressedBytes.length);
            }
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

                if (parquetNative) {
                    dictPageOffset = out.getFilePointer();

                    PageHeader ph = new PageHeader(PageType.DICTIONARY_PAGE, dictBytes.length, compressedBytes.length);
                    ph.setDictionary_page_header(new DictionaryPageHeader(
                        dictionaryPage.getDictionarySize(),
                        convertEncoding(dictionaryPage.getEncoding())
                    ));

                    byte[] headerBytes = serializePageHeader(ph);
                    out.writeBytes(headerBytes, headerBytes.length);
                    out.writeBytes(compressedBytes, compressedBytes.length);

                    totalCompressedSize += headerBytes.length + compressedBytes.length;
                    totalUncompressedSize += headerBytes.length + dictBytes.length;
                    addEncoding(convertEncoding(dictionaryPage.getEncoding()));
                } else {
                    out.writeInt(compressedBytes.length);
                    out.writeInt(dictBytes.length);
                    out.writeInt(dictionaryPage.getDictionarySize());
                    out.writeString(dictionaryPage.getEncoding().name());
                    out.writeBytes(compressedBytes, compressedBytes.length);
                }
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

        long getTotalCompressedSize() {
            return totalCompressedSize;
        }

        long getTotalUncompressedSize() {
            return totalUncompressedSize;
        }

        List<org.apache.parquet.format.Encoding> getEncodings() {
            return encodings;
        }

        long getFirstDataPageOffset() {
            return firstDataPageOffset;
        }

        long getDictPageOffset() {
            return dictPageOffset;
        }

        private void addEncoding(org.apache.parquet.format.Encoding encoding) {
            if (!encodings.contains(encoding)) {
                encodings.add(encoding);
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
