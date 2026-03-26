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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads Lucene doc values from Parquet-encoded {@code .pdvd}/{@code .pdvm} files
 * written by {@link ParquetDocValuesWriter}.
 *
 * <p>On construction, reads all field metadata from the {@code .pdvm} file. When a
 * field's doc values are requested, seeks into the {@code .pdvd} file, reconstructs
 * Parquet pages, and uses {@link ColumnReader} to decode values into Lucene
 * {@link NumericDocValues}, {@link BinaryDocValues}, {@link SortedDocValues},
 * {@link SortedNumericDocValues}, or {@link SortedSetDocValues} iterators.</p>
 *
 * <p>Decoded field data is cached at the segment level so that repeated calls
 * (e.g. during _source reconstruction or aggregations) do not re-read from disk.</p>
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesReader extends DocValuesProducer {

    private final IndexInput dataIn;
    private final int version;
    private final Map<String, FieldMeta> fields = new HashMap<>();

    // Segment-level caches for decoded field data, keyed by field name.
    // Each cache stores the fully decoded arrays so repeated calls avoid disk I/O.
    private final ConcurrentHashMap<String, CachedNumeric> numericCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedBinary> binaryCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSorted> sortedCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSortedNumeric> sortedNumericCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSortedSet> sortedSetCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockPackedData> skipperCache = new ConcurrentHashMap<>();

    /** Cached decoded data for NUMERIC fields. */
    private static class CachedNumeric {
        final int[] docIds;
        final long[] values;
        final boolean isDense;

        CachedNumeric(int[] docIds, long[] values, boolean isDense) {
            this.docIds = docIds;
            this.values = values;
            this.isDense = isDense;
        }
    }

    /** Cached decoded data for BINARY fields. */
    private static class CachedBinary {
        final int[] docIds;
        final BytesRef[] values;
        final boolean isDense;

        CachedBinary(int[] docIds, BytesRef[] values, boolean isDense) {
            this.docIds = docIds;
            this.values = values;
            this.isDense = isDense;
        }
    }

    /** Cached decoded data for SORTED fields. */
    private static class CachedSorted {
        final int[] docIds;
        final int[] ords;
        final BytesRef[] dict;
        final boolean isDense;

        CachedSorted(int[] docIds, int[] ords, BytesRef[] dict, boolean isDense) {
            this.docIds = docIds;
            this.ords = ords;
            this.dict = dict;
            this.isDense = isDense;
        }
    }

    /** Cached decoded data for SORTED_NUMERIC fields. */
    private static class CachedSortedNumeric {
        final int[] docIds;
        final long[][] allValues;
        final boolean isDense;
        /** Non-null when every document has exactly one value (singleton optimization). */
        final long[] singletonValues;

        CachedSortedNumeric(int[] docIds, long[][] allValues, boolean isDense) {
            this.docIds = docIds;
            this.allValues = allValues;
            this.isDense = isDense;
            // Detect singleton: all docs have exactly 1 value
            boolean singleton = true;
            for (long[] vals : allValues) {
                if (vals.length != 1) {
                    singleton = false;
                    break;
                }
            }
            if (singleton && allValues.length > 0) {
                long[] flat = new long[allValues.length];
                for (int i = 0; i < allValues.length; i++) {
                    flat[i] = allValues[i][0];
                }
                this.singletonValues = flat;
            } else {
                this.singletonValues = null;
            }
        }
    }

    /** Cached decoded data for SORTED_SET fields. */
    private static class CachedSortedSet {
        final int[] docIds;
        final long[][] docOrds;
        final BytesRef[] dict;
        final boolean isDense;

        CachedSortedSet(int[] docIds, long[][] docOrds, BytesRef[] dict, boolean isDense) {
            this.docIds = docIds;
            this.docOrds = docOrds;
            this.dict = dict;
            this.isDense = isDense;
        }
    }

    /**
     * Metadata for a single field, read from the {@code .pdvm} file.
     */
    static class FieldMeta {
        final String fieldName;
        final String dvTypeName;
        final String repetition;
        final String primitiveTypeName;
        final long dataStartOffset;
        final long dataLength;
        final int pageCount;
        final boolean hasDictionary;
        final long docIdOffset;

        FieldMeta(
            String fieldName,
            String dvTypeName,
            String repetition,
            String primitiveTypeName,
            long dataStartOffset,
            long dataLength,
            int pageCount,
            boolean hasDictionary,
            long docIdOffset
        ) {
            this.fieldName = fieldName;
            this.dvTypeName = dvTypeName;
            this.repetition = repetition;
            this.primitiveTypeName = primitiveTypeName;
            this.dataStartOffset = dataStartOffset;
            this.dataLength = dataLength;
            this.pageCount = pageCount;
            this.hasDictionary = hasDictionary;
            this.docIdOffset = docIdOffset;
        }
    }

    public ParquetDocValuesReader(SegmentReadState state, String dataExtension, String metaExtension, String dataCodec, String metaCodec)
        throws IOException {
        String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        boolean success = false;
        IndexInput dataInput = null;
        ChecksumIndexInput metaInput = null;
        try {
            dataInput = state.directory.openInput(dataFileName, state.context);
            int dataVersion = CodecUtil.checkIndexHeader(
                dataInput,
                dataCodec,
                ParquetDocValuesWriter.VERSION_START,
                ParquetDocValuesWriter.VERSION_SKIP_INDEX,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            metaInput = state.directory.openChecksumInput(metaFileName);
            CodecUtil.checkIndexHeader(
                metaInput,
                metaCodec,
                ParquetDocValuesWriter.VERSION_START,
                ParquetDocValuesWriter.VERSION_SKIP_INDEX,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            readFields(metaInput, dataVersion);
            CodecUtil.checkFooter(metaInput);

            this.dataIn = dataInput;
            this.version = dataVersion;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(dataInput, metaInput);
            } else {
                IOUtils.close(metaInput);
            }
        }
    }

    public ParquetDocValuesReader(SegmentReadState state, String dataExtension, String metaExtension) throws IOException {
        this(state, dataExtension, metaExtension, ParquetDocValuesWriter.DATA_CODEC, ParquetDocValuesWriter.META_CODEC);
    }

    private void readFields(IndexInput metaIn, int version) throws IOException {
        String fieldName = metaIn.readString();
        while (fieldName.equals(ParquetDocValuesWriter.END_MARKER) == false) {
            String dvTypeName = metaIn.readString();
            String repetition = metaIn.readString();
            String primitiveTypeName = metaIn.readString();
            long dataStartOffset = metaIn.readLong();
            long dataLength = metaIn.readLong();
            int pageCount = metaIn.readInt();
            boolean hasDictionary = metaIn.readByte() == 1;
            long docIdOffset = metaIn.readLong();
            fields.put(
                fieldName,
                new FieldMeta(
                    fieldName,
                    dvTypeName,
                    repetition,
                    primitiveTypeName,
                    dataStartOffset,
                    dataLength,
                    pageCount,
                    hasDictionary,
                    docIdOffset
                )
            );
            fieldName = metaIn.readString();
        }
    }

    /**
     * Reads doc IDs and all values for a field from the data file using Parquet's ColumnReader.
     * Version 1 format: [data pages...][dictionary page][docCount][docIds...]
     * Doc IDs are located at meta.docIdOffset within the field's data slice.
     */
    private FieldData readFieldData(FieldMeta meta, PrimitiveType parquetType) throws IOException {
        MessageType schema = ParquetTypeMapping.messageType(meta.fieldName, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        IndexInput slice = dataIn.slice("field:" + meta.fieldName, meta.dataStartOffset, meta.dataLength);

        // Read doc IDs from the docIdOffset position
        IndexInput docIdSlice = dataIn.slice(
            "field-docids:" + meta.fieldName,
            meta.dataStartOffset + meta.docIdOffset,
            meta.dataLength - meta.docIdOffset
        );
        byte denseFlag = docIdSlice.readByte();
        int docCount = docIdSlice.readInt();
        boolean isDense = (denseFlag == ParquetDocValuesWriter.DENSE_FLAG);
        int[] docIds;
        if (isDense) {
            // Dense field: sequential doc IDs [0, 1, 2, ..., docCount-1]
            docIds = new int[docCount];
            for (int i = 0; i < docCount; i++) {
                docIds[i] = i;
            }
        } else {
            // Sparse field: doc IDs stored explicitly
            docIds = new int[docCount];
            for (int i = 0; i < docCount; i++) {
                docIds[i] = docIdSlice.readInt();
            }
        }

        if (docCount == 0) {
            return new FieldData(docIds, null, isDense);
        }

        // Read data pages from the beginning of the slice
        List<DataPage> dataPages = new ArrayList<>();
        long totalValueCount = 0;
        long totalRowCount = 0;
        for (int i = 0; i < meta.pageCount; i++) {
            int compressedSize = slice.readInt();
            int uncompressedSize = slice.readInt();
            int valueCount = slice.readInt();
            int rowCount = slice.readInt();
            String valuesEncodingName = slice.readString();
            String rlEncodingName = slice.readString();
            String dlEncodingName = slice.readString();
            byte[] compressedBytes = new byte[compressedSize];
            slice.readBytes(compressedBytes, 0, compressedSize);
            byte[] pageBytes = Zstd.decompress(compressedBytes, uncompressedSize);
            dataPages.add(
                new DataPageV1(
                    BytesInput.from(pageBytes),
                    valueCount,
                    uncompressedSize,
                    Statistics.getBuilderForReading(parquetType).build(),
                    Encoding.valueOf(rlEncodingName),
                    Encoding.valueOf(dlEncodingName),
                    Encoding.valueOf(valuesEncodingName)
                )
            );
            totalValueCount += valueCount;
            totalRowCount += rowCount;
        }

        // Read dictionary page (after data pages, before doc IDs)
        DictionaryPage dictPage = null;
        if (meta.hasDictionary) {
            int compressedSize = slice.readInt();
            int uncompressedSize = slice.readInt();
            int dictSize = slice.readInt();
            String dictEncodingName = slice.readString();
            byte[] compressedBytes = new byte[compressedSize];
            slice.readBytes(compressedBytes, 0, compressedSize);
            byte[] dictBytes = Zstd.decompress(compressedBytes, uncompressedSize);
            dictPage = new DictionaryPage(BytesInput.from(dictBytes), dictSize, Encoding.valueOf(dictEncodingName));
        }

        final DictionaryPage finalDictPage = dictPage;
        final long finalTotalValueCount = totalValueCount;
        final long finalTotalRowCount = totalRowCount;

        PageReadStore pageReadStore = new PageReadStore() {
            @Override
            public PageReader getPageReader(ColumnDescriptor path) {
                return new PageReader() {
                    private int pageIdx = 0;

                    @Override
                    public DictionaryPage readDictionaryPage() {
                        return finalDictPage;
                    }

                    @Override
                    public long getTotalValueCount() {
                        return finalTotalValueCount;
                    }

                    @Override
                    public DataPage readPage() {
                        if (pageIdx >= dataPages.size()) {
                            return null;
                        }
                        return dataPages.get(pageIdx++);
                    }
                };
            }

            @Override
            public long getRowCount() {
                return finalTotalRowCount;
            }
        };

        ColumnReadStoreImpl readStore = new ColumnReadStoreImpl(pageReadStore, new NoOpGroupConverter(), schema, "parquet-docvalues");
        return new FieldData(docIds, readStore.getColumnReader(descriptor), isDense);
    }

    /** Holds doc IDs, the column reader, and whether the field is dense. */
    private static class FieldData {
        final int[] docIds;
        final ColumnReader reader;
        final boolean isDense;

        FieldData(int[] docIds, ColumnReader reader, boolean isDense) {
            this.docIds = docIds;
            this.reader = reader;
            this.isDense = isDense;
        }
    }

    /** Cached data for NUMERIC fields using mmap'd packed values (version 3+). */
    private static class CachedNumericPacked extends CachedNumeric {
        final LongValues packedValues;
        /** Doc count for bounds checking; used instead of docIds.length when docIds is null (dense). */
        final int docCount;

        CachedNumericPacked(int[] docIds, LongValues packedValues, boolean isDense, int docCount) {
            super(docIds, null, isDense);
            this.packedValues = packedValues;
            this.docCount = docCount;
        }
    }

    /** Cached data for SORTED_NUMERIC singleton fields using mmap'd packed values (version 3+). */
    private static class CachedSortedNumericPacked extends CachedSortedNumeric {
        final LongValues packedValues;
        /** Doc count for bounds checking; used instead of docIds.length when docIds is null (dense). */
        final int docCount;

        CachedSortedNumericPacked(int[] docIds, LongValues packedValues, boolean isDense, int docCount) {
            super(docIds, new long[0][], isDense);
            this.packedValues = packedValues;
            this.docCount = docCount;
        }
    }

    /** Block-level metadata from the packed values section, used to build DocValuesSkipper. */
    static class BlockPackedData {
        final LongValues values;
        final int numBlocks;
        final long[] blockMinValues;
        final long[] blockMaxValues;
        final int docCount;
        final boolean isDense;
        final int[] docIds;

        BlockPackedData(LongValues values, int numBlocks, long[] blockMinValues, long[] blockMaxValues, int docCount, boolean isDense, int[] docIds) {
            this.values = values;
            this.numBlocks = numBlocks;
            this.blockMinValues = blockMinValues;
            this.blockMaxValues = blockMaxValues;
            this.docCount = docCount;
            this.isDense = isDense;
            this.docIds = docIds;
        }
    }

    /** Doc IDs, dense flag, doc count, and end offset read from the .pdvd file without parquet page decompression. */
    private static class DocIdData {
        /** Doc IDs array. Null for dense fields to avoid allocating a sequential int[docCount]. */
        final int[] docIds;
        final boolean isDense;
        final int docCount;
        /** Absolute file offset where the doc ID section ends. */
        final long endOffset;

        DocIdData(int[] docIds, boolean isDense, int docCount, long endOffset) {
            this.docIds = docIds;
            this.isDense = isDense;
            this.docCount = docCount;
            this.endOffset = endOffset;
        }
    }

    /**
     * Reads ONLY the doc ID section from .pdvd, skipping parquet page decompression.
     * Also computes the absolute file offset where the doc ID section ends.
     * For dense fields, docIds is null to avoid wasteful int[docCount] allocation.
     */
    private DocIdData readDocIdsOnly(FieldMeta meta) throws IOException {
        long absDocIdStart = meta.dataStartOffset + meta.docIdOffset;
        // Use a fresh clone to avoid position interference
        IndexInput in = dataIn.clone();
        in.seek(absDocIdStart);
        byte denseFlag = in.readByte();
        int docCount = in.readInt();
        boolean isDense = (denseFlag == ParquetDocValuesWriter.DENSE_FLAG);
        int[] docIds;
        if (isDense) {
            // Dense: doc IDs are [0..docCount-1], no need to allocate an array
            docIds = null;
        } else {
            // Sparse field: doc IDs stored explicitly
            docIds = new int[docCount];
            for (int i = 0; i < docCount; i++) {
                docIds[i] = in.readInt();
            }
        }
        // Compute absolute end offset: start + 1 (flag) + 4 (count) + sparse doc IDs
        long docIdSectionSize = 1 + 4 + (isDense ? 0 : 4L * docCount);
        long endOffset = absDocIdStart + docIdSectionSize;
        return new DocIdData(docIds, isDense, docCount, endOffset);
    }

    /**
     * Loads packed values via mmap'd DirectReader for O(1) random access.
     * Uses a RandomAccessInput slice from the main data input for thread-safe reads.
     * Format at packedValuesOffset: [long: minValue][long: gcd][byte: bitsPerValue][packed data...]
     * Used for VERSION_PACKED_VALUES (3) backward compatibility.
     */
    private LongValues loadPackedValues(long packedValuesOffset, int docCount) throws IOException {
        // Use a fresh clone to avoid position interference
        IndexInput in = dataIn.clone();
        in.seek(packedValuesOffset);
        long minValue = in.readLong();
        long gcd = in.readLong();
        int bitsPerValue = in.readByte();

        if (bitsPerValue == 0) {
            // All values are identical — return constant
            final long constValue = minValue;
            return new LongValues() {
                @Override
                public long get(long index) {
                    return constValue;
                }
            };
        }

        // Create a thread-safe RandomAccessInput over the packed data for DirectReader.
        long packedDataStart = packedValuesOffset + 17; // 8 (minValue) + 8 (gcd) + 1 (bitsPerValue)
        long packedDataLength = DirectWriter.bytesRequired(docCount, bitsPerValue);
        RandomAccessInput rai = threadSafeRandomAccess(packedDataStart, packedDataLength);
        LongValues packed = DirectReader.getInstance(rai, bitsPerValue);

        final long fMinValue = minValue;
        final long fGcd = gcd;
        return new LongValues() {
            @Override
            public long get(long index) {
                return fMinValue + packed.get(index) * fGcd;
            }
        };
    }

    /**
     * Loads block-based packed values via DirectReader for O(1) random access.
     * Each block of BLOCK_SIZE docs has its own minValue, gcd, bitsPerValue, and DirectReader instance.
     * V4 format: 25 bytes/block (offset, min, gcd, bpv)
     * V5 format: 33 bytes/block (offset, min, max, gcd, bpv) — adds maxValue for DocValuesSkipper
     */
    private BlockPackedData loadBlockPackedData(long packedValuesOffset, int docCount, boolean isDense, int[] docIds) throws IOException {
        // Use a fresh clone to avoid position interference
        IndexInput in = dataIn.clone();
        in.seek(packedValuesOffset);
        int numBlocks = in.readInt();

        boolean hasMaxValues = version >= ParquetDocValuesWriter.VERSION_SKIP_INDEX;
        int bytesPerBlock = hasMaxValues ? 33 : 25;

        long[] blockOffsets = new long[numBlocks];
        long[] blockMinValues = new long[numBlocks];
        long[] blockMaxValues = new long[numBlocks];
        long[] blockGcds = new long[numBlocks];
        int[] blockBitsPerValue = new int[numBlocks];

        for (int b = 0; b < numBlocks; b++) {
            blockOffsets[b] = in.readLong();
            blockMinValues[b] = in.readLong();
            if (hasMaxValues) {
                blockMaxValues[b] = in.readLong();
            }
            blockGcds[b] = in.readLong();
            blockBitsPerValue[b] = in.readByte();
        }

        // For V4 (no maxValues stored), set maxValues = Long.MAX_VALUE as safe upper bound.
        // The skipper will still work but won't skip blocks as aggressively.
        if (hasMaxValues == false) {
            for (int b = 0; b < numBlocks; b++) {
                blockMaxValues[b] = (blockBitsPerValue[b] == 0) ? blockMinValues[b] : Long.MAX_VALUE;
            }
        }

        long blockDataStart = packedValuesOffset + 4 + (long) bytesPerBlock * numBlocks;

        // Pre-create DirectReader instances per block (cached for O(1) access)
        final LongValues[] blockReaders = new LongValues[numBlocks];
        for (int b = 0; b < numBlocks; b++) {
            int bpv = blockBitsPerValue[b];
            if (bpv == 0) {
                // All values in this block are identical
                final long constValue = blockMinValues[b];
                blockReaders[b] = new LongValues() {
                    @Override
                    public long get(long index) {
                        return constValue;
                    }
                };
            } else {
                int blockStart = b << ParquetDocValuesWriter.BLOCK_SHIFT;
                int blockEnd = Math.min(blockStart + ParquetDocValuesWriter.BLOCK_SIZE, docCount);
                int blockCount = blockEnd - blockStart;
                long dataOffset = blockDataStart + blockOffsets[b];
                long dataLength = DirectWriter.bytesRequired(blockCount, bpv);
                RandomAccessInput rai = threadSafeRandomAccess(dataOffset, dataLength);
                LongValues packed = DirectReader.getInstance(rai, bpv);
                final long minVal = blockMinValues[b];
                final long gcd = blockGcds[b];
                blockReaders[b] = new LongValues() {
                    @Override
                    public long get(long index) {
                        return minVal + packed.get(index) * gcd;
                    }
                };
            }
        }

        final int blockShift = ParquetDocValuesWriter.BLOCK_SHIFT;
        final int blockMask = ParquetDocValuesWriter.BLOCK_SIZE - 1;
        LongValues longValues = new LongValues() {
            @Override
            public long get(long index) {
                int blockIdx = (int) (index >> blockShift);
                int inBlockIdx = (int) (index & blockMask);
                return blockReaders[blockIdx].get(inBlockIdx);
            }
        };
        return new BlockPackedData(longValues, numBlocks, blockMinValues, blockMaxValues, docCount, isDense, docIds);
    }

    /**
     * Returns a thread-safe RandomAccessInput for the given region of the data file.
     * Uses IndexInput.randomAccessSlice() which provides zero-copy mmap'd access
     * for MMapDirectory. All reads are positional (absolute offset) with no mutable
     * cursor state, making them inherently thread-safe on shared memory.
     */
    private RandomAccessInput threadSafeRandomAccess(long offset, long length) throws IOException {
        return dataIn.randomAccessSlice(offset, length);
    }

    /**
     * Thread-safe RandomAccessInput backed by a byte array.
     * All reads are position-based with no shared mutable state.
     * Uses little-endian byte order to match DirectWriter/DirectReader expectations.
     */
    private static class ByteArrayRandomAccessInput implements RandomAccessInput {
        private final byte[] data;

        ByteArrayRandomAccessInput(byte[] data) {
            this.data = data;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public byte readByte(long pos) {
            return data[(int) pos];
        }

        @Override
        public short readShort(long pos) {
            int p = (int) pos;
            return (short) ((data[p] & 0xFF) | (data[p + 1] & 0xFF) << 8);
        }

        @Override
        public int readInt(long pos) {
            int p = (int) pos;
            return (data[p] & 0xFF)
                | (data[p + 1] & 0xFF) << 8
                | (data[p + 2] & 0xFF) << 16
                | (data[p + 3] & 0xFF) << 24;
        }

        @Override
        public long readLong(long pos) {
            int p = (int) pos;
            return ((long) (data[p] & 0xFF))
                | ((long) (data[p + 1] & 0xFF) << 8)
                | ((long) (data[p + 2] & 0xFF) << 16)
                | ((long) (data[p + 3] & 0xFF) << 24)
                | ((long) (data[p + 4] & 0xFF) << 32)
                | ((long) (data[p + 5] & 0xFF) << 40)
                | ((long) (data[p + 6] & 0xFF) << 48)
                | ((long) (data[p + 7] & 0xFF) << 56);
        }
    }

    private CachedNumeric loadNumeric(FieldMeta meta) throws IOException {
        // Fast path: use mmap'd packed values when available (version 3+)
        if (version >= ParquetDocValuesWriter.VERSION_PACKED_VALUES) {
            DocIdData docIdData = readDocIdsOnly(meta);
            if (docIdData.docCount == 0) {
                return new CachedNumeric(docIdData.docIds, new long[0], docIdData.isDense);
            }
            // Packed values section starts right after doc IDs in the .pdvd file
            long fieldEndOffset = meta.dataStartOffset + meta.dataLength;
            if (docIdData.endOffset < fieldEndOffset) {
                LongValues packedValues;
                if (version >= ParquetDocValuesWriter.VERSION_BLOCK_PACKED) {
                    BlockPackedData bpd = loadBlockPackedData(docIdData.endOffset, docIdData.docCount, docIdData.isDense, docIdData.docIds);
                    skipperCache.putIfAbsent(meta.fieldName, bpd);
                    packedValues = bpd.values;
                } else {
                    packedValues = loadPackedValues(docIdData.endOffset, docIdData.docCount);
                }
                return new CachedNumericPacked(docIdData.docIds, packedValues, docIdData.isDense, docIdData.docCount);
            }
        }
        // Fallback: decode from parquet pages (version 2 or no packed section)
        PrimitiveType parquetType = ParquetTypeMapping.numericType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedNumeric(docIds, new long[0], fieldData.isDense);
        }
        ColumnReader reader = fieldData.reader;
        long[] values = new long[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            values[i] = reader.getLong();
            reader.consume();
        }
        return new CachedNumeric(docIds, values, fieldData.isDense);
    }

    /**
     * Linear scan forward from start looking for target in docIds.
     * Returns the index if found within LINEAR_SCAN_THRESHOLD steps, or -1 if not found within that window.
     */
    private static int linearScanForward(int[] docIds, int start, int target) {
        int end = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
        for (int i = start; i < end; i++) {
            if (docIds[i] >= target) {
                return i;
            }
        }
        return -1;
    }

    /** Threshold for switching from linear scan to binary search in advance(). */
    private static final int LINEAR_SCAN_THRESHOLD = 64;

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedNumeric cached = computeNumericCache(field.name, meta);
        int[] docIds = cached.docIds;

        // For dense packed fields, docIds is null; use docCount from CachedNumericPacked
        final int docCount = (cached instanceof CachedNumericPacked)
            ? ((CachedNumericPacked) cached).docCount
            : (docIds != null ? docIds.length : 0);

        if (docCount == 0) {
            return DocValues.emptyNumeric();
        }

        // Use mmap'd packed values for O(1) random access when available (version 3+),
        // otherwise fall back to heap long[] from parquet page decoding
        final boolean usePacked = (cached instanceof CachedNumericPacked);
        final LongValues packedValues = usePacked ? ((CachedNumericPacked) cached).packedValues : null;
        final long[] values = usePacked ? null : cached.values;

        if (cached.isDense) {
            // Dense: docIds may be null (packed path), use docCount for bounds
            final int maxDoc = docCount;
            return new NumericDocValues() {
                private int doc = -1;

                @Override
                public long longValue() {
                    return usePacked ? packedValues.get(doc) : values[doc];
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    return target < maxDoc;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        return new NumericDocValues() {
            private int idx = -1;
            private int doc = -1;

            @Override
            public long longValue() {
                return usePacked ? packedValues.get(idx) : values[idx];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (idx >= 0 && idx < docIds.length) {
                    if (docIds[idx] == target) {
                        return true;
                    } else if (docIds[idx] < target) {
                        // Forward: try linear scan first
                        int i = linearScanForward(docIds, idx, target);
                        if (i >= 0) {
                            if (docIds[i] == target) {
                                idx = i;
                                return true;
                            }
                            idx = i - 1;
                            return false;
                        }
                        int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                        int found = java.util.Arrays.binarySearch(docIds, lo, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    } else {
                        // Backward: narrow search to [0, idx)
                        int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }
                }
                // Cold start: search entire array
                int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    return true;
                }
                idx = -found - 2;
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                idx++;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                int start = Math.max(idx + 1, 0);
                // Linear probe for sequential access pattern
                if (start < docIds.length && docIds[start] <= target) {
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        idx = i;
                        doc = docIds[idx];
                        return doc;
                    }
                    start = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    doc = docIds[idx];
                } else {
                    idx = -found - 1;
                    if (idx >= docIds.length) {
                        doc = NO_MORE_DOCS;
                    } else {
                        doc = docIds[idx];
                    }
                }
                return doc;
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };
    }

    private CachedNumeric computeNumericCache(String fieldName, FieldMeta meta) {
        return numericCache.computeIfAbsent(fieldName, k -> {
            try {
                return loadNumeric(meta);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private CachedBinary loadBinary(FieldMeta meta) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedBinary(docIds, new BytesRef[0], fieldData.isDense);
        }
        ColumnReader reader = fieldData.reader;
        BytesRef[] values = new BytesRef[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            Binary bin = reader.getBinary();
            values[i] = new BytesRef(bin.getBytes().clone());
            reader.consume();
        }
        return new CachedBinary(docIds, values, fieldData.isDense);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedBinary cached = binaryCache.computeIfAbsent(field.name, k -> {
            try {
                return loadBinary(meta);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        int[] docIds = cached.docIds;
        BytesRef[] values = cached.values;

        if (docIds.length == 0) {
            return DocValues.emptyBinary();
        }

        if (cached.isDense) {
            final int maxDoc = docIds.length;
            return new BinaryDocValues() {
                private int doc = -1;

                @Override
                public BytesRef binaryValue() {
                    return values[doc];
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    return target < maxDoc;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        return new BinaryDocValues() {
            private int idx = -1;
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return values[idx];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (idx >= 0 && idx < docIds.length) {
                    if (docIds[idx] == target) {
                        return true;
                    } else if (docIds[idx] < target) {
                        // Forward: try linear scan first
                        int i = linearScanForward(docIds, idx, target);
                        if (i >= 0) {
                            if (docIds[i] == target) {
                                idx = i;
                                return true;
                            }
                            idx = i - 1;
                            return false;
                        }
                        int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                        int found = java.util.Arrays.binarySearch(docIds, lo, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    } else {
                        // Backward: narrow search to [0, idx)
                        int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }
                }
                // Cold start: search entire array
                int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    return true;
                }
                idx = -found - 2;
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                idx++;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                int start = Math.max(idx + 1, 0);
                if (start < docIds.length && docIds[start] <= target) {
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        idx = i;
                        doc = docIds[idx];
                        return doc;
                    }
                    start = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    doc = docIds[idx];
                } else {
                    idx = -found - 1;
                    if (idx >= docIds.length) {
                        doc = NO_MORE_DOCS;
                    } else {
                        doc = docIds[idx];
                    }
                }
                return doc;
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };
    }

    private CachedSorted loadSorted(FieldMeta meta) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedSorted(docIds, new int[0], new BytesRef[0], fieldData.isDense);
        }
        ColumnReader reader = fieldData.reader;
        BytesRef[] rawValues = new BytesRef[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            rawValues[i] = new BytesRef(reader.getBinary().getBytes().clone());
            reader.consume();
        }
        Map<BytesRef, Integer> valueToOrd = new HashMap<>();
        List<BytesRef> sortedDict = new ArrayList<>();
        for (BytesRef val : rawValues) {
            if (valueToOrd.containsKey(val) == false) {
                valueToOrd.put(val, sortedDict.size());
                sortedDict.add(val);
            }
        }
        sortedDict.sort(BytesRef::compareTo);
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }
        int[] ords = new int[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            ords[i] = valueToOrd.get(rawValues[i]);
        }
        BytesRef[] dict = sortedDict.toArray(new BytesRef[0]);
        return new CachedSorted(docIds, ords, dict, fieldData.isDense);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSorted cached = sortedCache.computeIfAbsent(field.name, k -> {
            try {
                return loadSorted(meta);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        int[] docIds = cached.docIds;
        int[] ords = cached.ords;
        BytesRef[] dict = cached.dict;

        if (docIds.length == 0) {
            return DocValues.emptySorted();
        }

        if (cached.isDense) {
            final int maxDoc = docIds.length;
            return new SortedDocValues() {
                private int doc = -1;

                @Override
                public int ordValue() {
                    return ords[doc];
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    return dict[ord];
                }

                @Override
                public int getValueCount() {
                    return dict.length;
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    return target < maxDoc;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        return new SortedDocValues() {
            private int idx = -1;
            private int doc = -1;

            @Override
            public int ordValue() {
                return ords[idx];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return dict[ord];
            }

            @Override
            public int getValueCount() {
                return dict.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (idx >= 0 && idx < docIds.length) {
                    if (docIds[idx] == target) {
                        return true;
                    } else if (docIds[idx] < target) {
                        // Forward: try linear scan first
                        int i = linearScanForward(docIds, idx, target);
                        if (i >= 0) {
                            if (docIds[i] == target) {
                                idx = i;
                                return true;
                            }
                            idx = i - 1;
                            return false;
                        }
                        int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                        int found = java.util.Arrays.binarySearch(docIds, lo, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    } else {
                        // Backward: narrow search to [0, idx)
                        int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }
                }
                // Cold start: search entire array
                int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    return true;
                }
                idx = -found - 2;
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                idx++;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                int start = Math.max(idx + 1, 0);
                if (start < docIds.length && docIds[start] <= target) {
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        idx = i;
                        doc = docIds[idx];
                        return doc;
                    }
                    start = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    doc = docIds[idx];
                } else {
                    idx = -found - 1;
                    if (idx >= docIds.length) {
                        doc = NO_MORE_DOCS;
                    } else {
                        doc = docIds[idx];
                    }
                }
                return doc;
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };
    }

    private CachedSortedNumeric loadSortedNumeric(FieldMeta meta) throws IOException {
        // Fast path: use mmap'd packed values for singleton sorted numeric fields (version 3+)
        if (version >= ParquetDocValuesWriter.VERSION_PACKED_VALUES) {
            DocIdData docIdData = readDocIdsOnly(meta);
            if (docIdData.docCount == 0) {
                return new CachedSortedNumeric(docIdData.docIds, new long[0][], docIdData.isDense);
            }
            long fieldEndOffset = meta.dataStartOffset + meta.dataLength;
            if (docIdData.endOffset < fieldEndOffset) {
                LongValues packedValues;
                if (version >= ParquetDocValuesWriter.VERSION_BLOCK_PACKED) {
                    BlockPackedData bpd = loadBlockPackedData(docIdData.endOffset, docIdData.docCount, docIdData.isDense, docIdData.docIds);
                    skipperCache.putIfAbsent(meta.fieldName, bpd);
                    packedValues = bpd.values;
                } else {
                    packedValues = loadPackedValues(docIdData.endOffset, docIdData.docCount);
                }
                return new CachedSortedNumericPacked(docIdData.docIds, packedValues, docIdData.isDense, docIdData.docCount);
            }
        }
        // Fallback: decode from parquet pages (version 2 or multi-valued)
        PrimitiveType parquetType = ParquetTypeMapping.sortedNumericType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedSortedNumeric(docIds, new long[0][], fieldData.isDense);
        }
        ColumnReader reader = fieldData.reader;
        int totalValues = (int) reader.getTotalValueCount();
        List<long[]> docValues = new ArrayList<>();
        // Use primitive array to avoid boxing overhead
        long[] currentDoc = new long[8];
        int currentSize = 0;
        for (int i = 0; i < totalValues; i++) {
            int rep = reader.getCurrentRepetitionLevel();
            int def = reader.getCurrentDefinitionLevel();
            if (rep == 0 && i > 0) {
                docValues.add(java.util.Arrays.copyOf(currentDoc, currentSize));
                currentSize = 0;
            }
            if (def == 1) {
                if (currentSize == currentDoc.length) {
                    currentDoc = java.util.Arrays.copyOf(currentDoc, currentDoc.length * 2);
                }
                currentDoc[currentSize++] = reader.getLong();
            }
            reader.consume();
        }
        if (totalValues > 0) {
            docValues.add(java.util.Arrays.copyOf(currentDoc, currentSize));
        }
        return new CachedSortedNumeric(docIds, docValues.toArray(new long[0][]), fieldData.isDense);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSortedNumeric cached = sortedNumericCache.computeIfAbsent(field.name, k -> {
            try {
                return loadSortedNumeric(meta);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        int[] docIds = cached.docIds;
        long[][] allValues = cached.allValues;

        // For dense packed fields, docIds is null; use docCount from CachedSortedNumericPacked
        final int docCount = (cached instanceof CachedSortedNumericPacked)
            ? ((CachedSortedNumericPacked) cached).docCount
            : (docIds != null ? docIds.length : 0);

        if (docCount == 0) {
            return DocValues.emptySortedNumeric();
        }

        // Singleton optimization: wrap NumericDocValues with DocValues.singleton()
        // so that DocValues.unwrapSingleton() returns the underlying NumericDocValues
        // in the sort comparator path (LongValuesComparatorSource → LongComparator).
        // This enables the optimized single-valued sort path instead of the slower
        // multi-valued iterator path.
        if (cached instanceof CachedSortedNumericPacked || cached.singletonValues != null) {
            final boolean usePacked = (cached instanceof CachedSortedNumericPacked);
            final LongValues packedValues = usePacked ? ((CachedSortedNumericPacked) cached).packedValues : null;
            final long[] singleValues = usePacked ? null : cached.singletonValues;
            final int[] sDocIds = cached.docIds;

            NumericDocValues numeric;
            if (cached.isDense) {
                final int maxDoc = docCount;
                numeric = new NumericDocValues() {
                    private int doc = -1;

                    @Override
                    public long longValue() {
                        return usePacked ? packedValues.get(doc) : singleValues[doc];
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        return target < maxDoc;
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        return (++doc >= maxDoc) ? (doc = NO_MORE_DOCS) : doc;
                    }

                    @Override
                    public int advance(int target) {
                        return (target >= maxDoc) ? (doc = NO_MORE_DOCS) : (doc = target);
                    }

                    @Override
                    public long cost() {
                        return maxDoc;
                    }
                };
            } else {
                numeric = new NumericDocValues() {
                    private int idx = -1;
                    private int doc = -1;

                    @Override
                    public long longValue() {
                        return usePacked ? packedValues.get(idx) : singleValues[idx];
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        if (idx >= 0 && idx < sDocIds.length) {
                            if (sDocIds[idx] == target) {
                                return true;
                            } else if (sDocIds[idx] < target) {
                                // Forward: try linear scan first
                                int i = linearScanForward(sDocIds, idx, target);
                                if (i >= 0) {
                                    if (sDocIds[i] == target) {
                                        idx = i;
                                        return true;
                                    }
                                    idx = i - 1;
                                    return false;
                                }
                                int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, sDocIds.length);
                                int found = java.util.Arrays.binarySearch(sDocIds, lo, sDocIds.length, target);
                                if (found >= 0) {
                                    idx = found;
                                    return true;
                                }
                                idx = -found - 2;
                                return false;
                            } else {
                                // Backward: narrow search to [0, idx)
                                int found = java.util.Arrays.binarySearch(sDocIds, 0, idx, target);
                                if (found >= 0) {
                                    idx = found;
                                    return true;
                                }
                                idx = -found - 2;
                                return false;
                            }
                        }
                        // Cold start: search entire array
                        int found = java.util.Arrays.binarySearch(sDocIds, 0, sDocIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        idx++;
                        if (idx >= sDocIds.length) {
                            doc = NO_MORE_DOCS;
                        } else {
                            doc = sDocIds[idx];
                        }
                        return doc;
                    }

                    @Override
                    public int advance(int target) {
                        int start = Math.max(idx + 1, 0);
                        if (start < sDocIds.length && sDocIds[start] <= target) {
                            int i = linearScanForward(sDocIds, start, target);
                            if (i >= 0) {
                                idx = i;
                                doc = sDocIds[idx];
                                return doc;
                            }
                            start = Math.min(start + LINEAR_SCAN_THRESHOLD, sDocIds.length);
                        }
                        int found = java.util.Arrays.binarySearch(sDocIds, start, sDocIds.length, target);
                        if (found >= 0) {
                            idx = found;
                        } else {
                            idx = -found - 1;
                        }
                        if (idx >= sDocIds.length) {
                            doc = NO_MORE_DOCS;
                        } else {
                            doc = sDocIds[idx];
                        }
                        return doc;
                    }

                    @Override
                    public long cost() {
                        return sDocIds.length;
                    }
                };
            }
            return DocValues.singleton(numeric);
        }

        if (cached.isDense) {
            final int maxDoc = docIds.length;
            return new SortedNumericDocValues() {
                private int doc = -1;
                private int valueIdx;

                @Override
                public long nextValue() {
                    return allValues[doc][valueIdx++];
                }

                @Override
                public int docValueCount() {
                    return allValues[doc].length;
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    valueIdx = 0;
                    return target < maxDoc;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    valueIdx = 0;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    valueIdx = 0;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        return new SortedNumericDocValues() {
            private int idx = -1;
            private int doc = -1;
            private int valueIdx;

            @Override
            public long nextValue() {
                return allValues[idx][valueIdx++];
            }

            @Override
            public int docValueCount() {
                return allValues[idx].length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (idx >= 0 && idx < docIds.length) {
                    if (docIds[idx] == target) {
                        valueIdx = 0;
                        return true;
                    } else if (docIds[idx] < target) {
                        // Forward: try linear scan first
                        int i = linearScanForward(docIds, idx, target);
                        if (i >= 0) {
                            if (docIds[i] == target) {
                                idx = i;
                                valueIdx = 0;
                                return true;
                            }
                            idx = i - 1;
                            return false;
                        }
                        int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                        int found = java.util.Arrays.binarySearch(docIds, lo, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            valueIdx = 0;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    } else {
                        // Backward: narrow search to [0, idx)
                        int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                        if (found >= 0) {
                            idx = found;
                            valueIdx = 0;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }
                }
                // Cold start: search entire array
                int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    valueIdx = 0;
                    return true;
                }
                idx = -found - 2;
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                idx++;
                valueIdx = 0;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                int start = Math.max(idx + 1, 0);
                if (start < docIds.length && docIds[start] <= target) {
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        idx = i;
                        doc = docIds[idx];
                        valueIdx = 0;
                        return doc;
                    }
                    start = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                } else {
                    idx = -found - 1;
                }
                valueIdx = 0;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };
    }

    private CachedSortedSet loadSortedSet(FieldMeta meta) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.sortedSetType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedSortedSet(docIds, new long[0][], new BytesRef[0], fieldData.isDense);
        }
        ColumnReader reader = fieldData.reader;
        int totalValues = (int) reader.getTotalValueCount();
        List<List<BytesRef>> docBytesRefs = new ArrayList<>();
        List<BytesRef> currentDoc = new ArrayList<>();
        for (int i = 0; i < totalValues; i++) {
            int rep = reader.getCurrentRepetitionLevel();
            int def = reader.getCurrentDefinitionLevel();
            if (rep == 0 && i > 0) {
                docBytesRefs.add(new ArrayList<>(currentDoc));
                currentDoc.clear();
            }
            if (def == 1) {
                currentDoc.add(new BytesRef(reader.getBinary().getBytes().clone()));
            }
            reader.consume();
        }
        if (totalValues > 0) {
            docBytesRefs.add(new ArrayList<>(currentDoc));
        }
        Map<BytesRef, Integer> valueToOrd = new HashMap<>();
        List<BytesRef> sortedDict = new ArrayList<>();
        for (List<BytesRef> docVals : docBytesRefs) {
            for (BytesRef val : docVals) {
                if (valueToOrd.containsKey(val) == false) {
                    valueToOrd.put(val, sortedDict.size());
                    sortedDict.add(val);
                }
            }
        }
        sortedDict.sort(BytesRef::compareTo);
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }
        BytesRef[] dict = sortedDict.toArray(new BytesRef[0]);
        long[][] docOrds = new long[docBytesRefs.size()][];
        for (int d = 0; d < docBytesRefs.size(); d++) {
            List<BytesRef> vals = docBytesRefs.get(d);
            long[] ordArr = new long[vals.size()];
            for (int v = 0; v < vals.size(); v++) {
                ordArr[v] = valueToOrd.get(vals.get(v));
            }
            java.util.Arrays.sort(ordArr);
            docOrds[d] = ordArr;
        }
        return new CachedSortedSet(docIds, docOrds, dict, fieldData.isDense);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSortedSet cached = sortedSetCache.computeIfAbsent(field.name, k -> {
            try {
                return loadSortedSet(meta);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        int[] docIds = cached.docIds;
        long[][] docOrds = cached.docOrds;
        BytesRef[] dict = cached.dict;

        if (docIds.length == 0) {
            return DocValues.emptySortedSet();
        }

        // Singleton detection: if every document has exactly one ordinal,
        // wrap as DocValues.singleton() so DocValues.unwrapSingleton() succeeds
        // (required by GlobalOrdinalsStringTermsAggregator).
        boolean isSingleton = true;
        for (long[] ords : docOrds) {
            if (ords.length != 1) {
                isSingleton = false;
                break;
            }
        }
        if (isSingleton) {
            SortedDocValues sortedDv;
            if (cached.isDense) {
                final int maxDoc = docIds.length;
                sortedDv = new SortedDocValues() {
                    private int doc = -1;

                    @Override
                    public int ordValue() {
                        return (int) docOrds[doc][0];
                    }

                    @Override
                    public BytesRef lookupOrd(int ord) {
                        return dict[ord];
                    }

                    @Override
                    public int getValueCount() {
                        return dict.length;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        return target < maxDoc;
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        doc++;
                        if (doc >= maxDoc) {
                            doc = NO_MORE_DOCS;
                        }
                        return doc;
                    }

                    @Override
                    public int advance(int target) {
                        doc = target;
                        if (doc >= maxDoc) {
                            doc = NO_MORE_DOCS;
                        }
                        return doc;
                    }

                    @Override
                    public long cost() {
                        return maxDoc;
                    }
                };
            } else {
                sortedDv = new SortedDocValues() {
                    private int idx = -1;
                    private int doc = -1;

                    @Override
                    public int ordValue() {
                        return (int) docOrds[idx][0];
                    }

                    @Override
                    public BytesRef lookupOrd(int ord) {
                        return dict[ord];
                    }

                    @Override
                    public int getValueCount() {
                        return dict.length;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        if (idx >= 0 && idx < docIds.length) {
                            if (docIds[idx] == target) {
                                return true;
                            } else if (docIds[idx] < target) {
                                // Forward: search from current position
                                int found = java.util.Arrays.binarySearch(docIds, idx, docIds.length, target);
                                if (found >= 0) {
                                    idx = found;
                                    return true;
                                }
                                idx = -found - 2;
                                return false;
                            } else {
                                // Backward: narrow search to [0, idx)
                                int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                                if (found >= 0) {
                                    idx = found;
                                    return true;
                                }
                                idx = -found - 2;
                                return false;
                            }
                        }
                        // Cold start: search entire array
                        int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        idx++;
                        if (idx >= docIds.length) {
                            doc = NO_MORE_DOCS;
                        } else {
                            doc = docIds[idx];
                        }
                        return doc;
                    }

                    @Override
                    public int advance(int target) {
                        int start = Math.max(idx + 1, 0);
                        int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                        } else {
                            idx = -found - 1;
                        }
                        if (idx >= docIds.length) {
                            doc = NO_MORE_DOCS;
                        } else {
                            doc = docIds[idx];
                        }
                        return doc;
                    }

                    @Override
                    public long cost() {
                        return docIds.length;
                    }
                };
            }
            return DocValues.singleton(sortedDv);
        }

        if (cached.isDense) {
            final int maxDoc = docIds.length;
            return new SortedSetDocValues() {
                private int doc = -1;
                private int ordIdx;

                @Override
                public long nextOrd() {
                    return docOrds[doc][ordIdx++];
                }

                @Override
                public int docValueCount() {
                    return docOrds[doc].length;
                }

                @Override
                public BytesRef lookupOrd(long ord) {
                    return dict[(int) ord];
                }

                @Override
                public long getValueCount() {
                    return dict.length;
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    ordIdx = 0;
                    return target < maxDoc;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    ordIdx = 0;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    ordIdx = 0;
                    if (doc >= maxDoc) {
                        doc = NO_MORE_DOCS;
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        return new SortedSetDocValues() {
            private int idx = -1;
            private int doc = -1;
            private int ordIdx;

            @Override
            public long nextOrd() {
                return docOrds[idx][ordIdx++];
            }

            @Override
            public int docValueCount() {
                return docOrds[idx].length;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return dict[(int) ord];
            }

            @Override
            public long getValueCount() {
                return dict.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (idx >= 0 && idx < docIds.length) {
                    if (docIds[idx] == target) {
                        ordIdx = 0;
                        return true;
                    } else if (docIds[idx] < target) {
                        // Forward: try linear scan first
                        int i = linearScanForward(docIds, idx, target);
                        if (i >= 0) {
                            if (docIds[i] == target) {
                                idx = i;
                                ordIdx = 0;
                                return true;
                            }
                            idx = i - 1;
                            return false;
                        }
                        int lo = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                        int found = java.util.Arrays.binarySearch(docIds, lo, docIds.length, target);
                        if (found >= 0) {
                            idx = found;
                            ordIdx = 0;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    } else {
                        // Backward: narrow search to [0, idx)
                        int found = java.util.Arrays.binarySearch(docIds, 0, idx, target);
                        if (found >= 0) {
                            idx = found;
                            ordIdx = 0;
                            return true;
                        }
                        idx = -found - 2;
                        return false;
                    }
                }
                // Cold start: search entire array
                int found = java.util.Arrays.binarySearch(docIds, 0, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                    ordIdx = 0;
                    return true;
                }
                idx = -found - 2;
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                idx++;
                ordIdx = 0;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                int start = Math.max(idx + 1, 0);
                if (start < docIds.length && docIds[start] <= target) {
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        idx = i;
                        doc = docIds[idx];
                        ordIdx = 0;
                        return doc;
                    }
                    start = Math.min(start + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
                if (found >= 0) {
                    idx = found;
                } else {
                    idx = -found - 1;
                }
                ordIdx = 0;
                if (idx >= docIds.length) {
                    doc = NO_MORE_DOCS;
                } else {
                    doc = docIds[idx];
                }
                return doc;
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        BlockPackedData bpd = skipperCache.get(field.name);
        if (bpd == null || bpd.numBlocks == 0) {
            return null;
        }
        // Compute global min/max across all blocks
        long globalMin = bpd.blockMinValues[0];
        long globalMax = bpd.blockMaxValues[0];
        for (int b = 1; b < bpd.numBlocks; b++) {
            globalMin = Math.min(globalMin, bpd.blockMinValues[b]);
            globalMax = Math.max(globalMax, bpd.blockMaxValues[b]);
        }
        final long gMin = globalMin;
        final long gMax = globalMax;
        final int gDocCount = bpd.docCount;
        final int numBlocks = bpd.numBlocks;
        final long[] blockMinValues = bpd.blockMinValues;
        final long[] blockMaxValues = bpd.blockMaxValues;
        final boolean isDense = bpd.isDense;
        final int[] docIds = bpd.docIds;
        final int blockShift = ParquetDocValuesWriter.BLOCK_SHIFT;
        final int blockSize = ParquetDocValuesWriter.BLOCK_SIZE;
        final int docCount = bpd.docCount;

        // GROUP_SHIFT=4 means each higher level groups 16 entries from the level below
        final int GROUP_SHIFT = 4;
        final int GROUP_SIZE = 1 << GROUP_SHIFT; // 16

        // Pre-compute level-0 per-block minDocID/maxDocID/docCount arrays
        final int[] block0MinDocIDs = new int[numBlocks];
        final int[] block0MaxDocIDs = new int[numBlocks];
        final int[] block0DocCounts = new int[numBlocks];
        for (int b = 0; b < numBlocks; b++) {
            if (isDense) {
                block0MinDocIDs[b] = b << blockShift;
                int blockEnd = Math.min((b + 1) << blockShift, docCount);
                block0MaxDocIDs[b] = blockEnd - 1;
                block0DocCounts[b] = blockEnd - block0MinDocIDs[b];
            } else {
                int startIdx = b << blockShift;
                int endIdx = Math.min((b + 1) << blockShift, docIds.length) - 1;
                block0MinDocIDs[b] = docIds[startIdx];
                block0MaxDocIDs[b] = docIds[endIdx];
                block0DocCounts[b] = endIdx - startIdx + 1;
            }
        }

        // Build higher levels by aggregating groups of GROUP_SIZE from the level below.
        // Each level stores: minValues, maxValues, minDocIDs, maxDocIDs, docCounts.
        // We only add a level if it has >1 group (otherwise it doesn't help skipping).
        // levelData[i] corresponds to level i+1 (level 0 is the per-block data).
        int maxExtraLevels = 3; // levels 1, 2, 3
        // Arrays of arrays for each extra level
        long[][] lvlMinValues = new long[maxExtraLevels][];
        long[][] lvlMaxValues = new long[maxExtraLevels][];
        int[][] lvlMinDocIDs = new int[maxExtraLevels][];
        int[][] lvlMaxDocIDs = new int[maxExtraLevels][];
        int[][] lvlDocCounts = new int[maxExtraLevels][];
        int actualExtraLevels = 0;

        // Source arrays for aggregation: start with level-0 block data
        long[] srcMin = blockMinValues;
        long[] srcMax = blockMaxValues;
        int[] srcMinDoc = block0MinDocIDs;
        int[] srcMaxDoc = block0MaxDocIDs;
        int[] srcDocCount = block0DocCounts;
        int srcLen = numBlocks;

        for (int lvl = 0; lvl < maxExtraLevels; lvl++) {
            int numGroups = (srcLen + GROUP_SIZE - 1) >>> GROUP_SHIFT;
            if (numGroups <= 1) break; // This level wouldn't help skipping
            long[] lMin = new long[numGroups];
            long[] lMax = new long[numGroups];
            int[] lMinDoc = new int[numGroups];
            int[] lMaxDoc = new int[numGroups];
            int[] lDocCount = new int[numGroups];
            for (int g = 0; g < numGroups; g++) {
                int from = g << GROUP_SHIFT;
                int to = Math.min(from + GROUP_SIZE, srcLen);
                long mn = srcMin[from];
                long mx = srcMax[from];
                int mnDoc = srcMinDoc[from];
                int mxDoc = srcMaxDoc[from];
                int dc = srcDocCount[from];
                for (int i = from + 1; i < to; i++) {
                    mn = Math.min(mn, srcMin[i]);
                    mx = Math.max(mx, srcMax[i]);
                    mnDoc = Math.min(mnDoc, srcMinDoc[i]);
                    mxDoc = Math.max(mxDoc, srcMaxDoc[i]);
                    dc += srcDocCount[i];
                }
                lMin[g] = mn;
                lMax[g] = mx;
                lMinDoc[g] = mnDoc;
                lMaxDoc[g] = mxDoc;
                lDocCount[g] = dc;
            }
            lvlMinValues[lvl] = lMin;
            lvlMaxValues[lvl] = lMax;
            lvlMinDocIDs[lvl] = lMinDoc;
            lvlMaxDocIDs[lvl] = lMaxDoc;
            lvlDocCounts[lvl] = lDocCount;
            actualExtraLevels++;
            // Next iteration aggregates from this level's groups
            srcMin = lMin;
            srcMax = lMax;
            srcMinDoc = lMinDoc;
            srcMaxDoc = lMaxDoc;
            srcDocCount = lDocCount;
            srcLen = numGroups;
        }

        final int numLevels = 1 + actualExtraLevels;
        // Capture final references for the anonymous class
        final long[][] fLvlMinValues = lvlMinValues;
        final long[][] fLvlMaxValues = lvlMaxValues;
        final int[][] fLvlMinDocIDs = lvlMinDocIDs;
        final int[][] fLvlMaxDocIDs = lvlMaxDocIDs;
        final int[][] fLvlDocCounts = lvlDocCounts;
        final int fGroupShift = GROUP_SHIFT;

        return new DocValuesSkipper() {
            private int currentBlock = -1;
            // Level 0 state
            private int minDocID0 = -1;
            private int maxDocID0 = -1;
            private long minValue0;
            private long maxValue0;
            private int docCount0;
            // Higher level current group indices
            private final int[] currentGroup = new int[numLevels - 1];

            @Override
            public void advance(int target) throws IOException {
                if (isDense) {
                    if (target >= docCount) {
                        minDocID0 = Integer.MAX_VALUE;
                        maxDocID0 = Integer.MAX_VALUE;
                        return;
                    }
                    currentBlock = target >> blockShift;
                } else {
                    // For sparse fields, find the value index for the first doc >= target
                    int startIdx = (currentBlock + 1) << blockShift;
                    if (startIdx < 0) startIdx = 0;
                    int idx;
                    if (startIdx >= docIds.length) {
                        idx = docIds.length;
                    } else if (docIds[startIdx] >= target) {
                        // Already past target, use this position
                        idx = startIdx;
                    } else {
                        idx = java.util.Arrays.binarySearch(docIds, startIdx, docIds.length, target);
                        if (idx < 0) idx = -idx - 1;
                    }
                    if (idx >= docIds.length) {
                        minDocID0 = Integer.MAX_VALUE;
                        maxDocID0 = Integer.MAX_VALUE;
                        return;
                    }
                    currentBlock = idx >> blockShift;
                }
                if (currentBlock >= numBlocks) {
                    minDocID0 = Integer.MAX_VALUE;
                    maxDocID0 = Integer.MAX_VALUE;
                    return;
                }
                // Level 0: per-block values
                minValue0 = blockMinValues[currentBlock];
                maxValue0 = blockMaxValues[currentBlock];
                minDocID0 = block0MinDocIDs[currentBlock];
                maxDocID0 = block0MaxDocIDs[currentBlock];
                docCount0 = block0DocCounts[currentBlock];

                // Compute group index for each higher level
                int blockIdx = currentBlock;
                for (int lvl = 0; lvl < numLevels - 1; lvl++) {
                    blockIdx = blockIdx >>> fGroupShift;
                    currentGroup[lvl] = blockIdx;
                }
            }

            @Override
            public int numLevels() {
                return numLevels;
            }

            @Override
            public int minDocID(int level) {
                if (level == 0) return minDocID0;
                return fLvlMinDocIDs[level - 1][currentGroup[level - 1]];
            }

            @Override
            public int maxDocID(int level) {
                if (level == 0) return maxDocID0;
                return fLvlMaxDocIDs[level - 1][currentGroup[level - 1]];
            }

            @Override
            public long minValue(int level) {
                if (level == 0) return minValue0;
                return fLvlMinValues[level - 1][currentGroup[level - 1]];
            }

            @Override
            public long maxValue(int level) {
                if (level == 0) return maxValue0;
                return fLvlMaxValues[level - 1][currentGroup[level - 1]];
            }

            @Override
            public int docCount(int level) {
                if (level == 0) return docCount0;
                return fLvlDocCounts[level - 1][currentGroup[level - 1]];
            }

            @Override
            public long minValue() {
                return gMin;
            }

            @Override
            public long maxValue() {
                return gMax;
            }

            @Override
            public int docCount() {
                return gDocCount;
            }
        };
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(dataIn);
    }

    /**
     * No-op GroupConverter required by ColumnReadStoreImpl. We read values directly
     * from the ColumnReader, so the converter is never actually invoked.
     */
    static class NoOpGroupConverter extends GroupConverter {
        private static final PrimitiveConverter NO_OP_PRIMITIVE = new PrimitiveConverter() {
        };

        @Override
        public Converter getConverter(int fieldIndex) {
            return NO_OP_PRIMITIVE;
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
    }
}
