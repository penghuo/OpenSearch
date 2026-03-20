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
import org.apache.lucene.util.BytesRef;
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
    private final Map<String, FieldMeta> fields = new HashMap<>();

    // Segment-level caches for decoded field data, keyed by field name.
    // Each cache stores the fully decoded arrays so repeated calls avoid disk I/O.
    private final ConcurrentHashMap<String, CachedNumeric> numericCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedBinary> binaryCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSorted> sortedCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSortedNumeric> sortedNumericCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSortedSet> sortedSetCache = new ConcurrentHashMap<>();

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

        CachedSortedNumeric(int[] docIds, long[][] allValues, boolean isDense) {
            this.docIds = docIds;
            this.allValues = allValues;
            this.isDense = isDense;
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
            CodecUtil.checkIndexHeader(
                dataInput,
                dataCodec,
                ParquetDocValuesWriter.VERSION_CURRENT,
                ParquetDocValuesWriter.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            metaInput = state.directory.openChecksumInput(metaFileName);
            CodecUtil.checkIndexHeader(
                metaInput,
                metaCodec,
                ParquetDocValuesWriter.VERSION_CURRENT,
                ParquetDocValuesWriter.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            readFields(metaInput);
            CodecUtil.checkFooter(metaInput);

            this.dataIn = dataInput;
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

    private void readFields(IndexInput metaIn) throws IOException {
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

    private CachedNumeric loadNumeric(FieldMeta meta) throws IOException {
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
        long[] values = cached.values;

        if (docIds.length == 0) {
            return DocValues.emptyNumeric();
        }

        if (cached.isDense) {
            final int maxDoc = docIds.length;
            return new NumericDocValues() {
                private int doc = -1;

                @Override
                public long longValue() {
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

        return new NumericDocValues() {
            private int idx = -1;
            private int doc = -1;

            @Override
            public long longValue() {
                return values[idx];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                int start = 0;
                // Linear probe for sequential (forward) access pattern
                if (idx >= 0 && idx < docIds.length && docIds[idx] <= target) {
                    start = idx;
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        if (docIds[i] == target) {
                            idx = i;
                            return true;
                        }
                        idx = i - 1;
                        return false;
                    }
                    start = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
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
                int start = 0;
                if (idx >= 0 && idx < docIds.length && docIds[idx] <= target) {
                    start = idx;
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        if (docIds[i] == target) {
                            idx = i;
                            return true;
                        }
                        idx = i - 1;
                        return false;
                    }
                    start = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
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
                int start = 0;
                if (idx >= 0 && idx < docIds.length && docIds[idx] <= target) {
                    start = idx;
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        if (docIds[i] == target) {
                            idx = i;
                            return true;
                        }
                        idx = i - 1;
                        return false;
                    }
                    start = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
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

        if (docIds.length == 0) {
            return DocValues.emptySortedNumeric();
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
                int start = 0;
                if (idx >= 0 && idx < docIds.length && docIds[idx] <= target) {
                    start = idx;
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        if (docIds[i] == target) {
                            idx = i;
                            valueIdx = 0;
                            return true;
                        }
                        idx = i - 1;
                        return false;
                    }
                    start = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
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
                        int found = java.util.Arrays.binarySearch(docIds, Math.max(idx, 0), docIds.length, target);
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
                int start = 0;
                if (idx >= 0 && idx < docIds.length && docIds[idx] <= target) {
                    start = idx;
                    int i = linearScanForward(docIds, start, target);
                    if (i >= 0) {
                        if (docIds[i] == target) {
                            idx = i;
                            ordIdx = 0;
                            return true;
                        }
                        idx = i - 1;
                        return false;
                    }
                    start = Math.min(idx + LINEAR_SCAN_THRESHOLD, docIds.length);
                }
                int found = java.util.Arrays.binarySearch(docIds, start, docIds.length, target);
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
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            return null;
        }
        String dvType = meta.dvTypeName;
        long globalMin = 0;
        long globalMax = 0;
        int globalDocCount = 0;
        int firstDocId = DocIdSetIterator.NO_MORE_DOCS;
        int lastDocId = DocIdSetIterator.NO_MORE_DOCS;

        if (dvType.equals("NUMERIC")) {
            CachedNumeric cached = computeNumericCache(field.name, meta);
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for (long v : cached.values) {
                    min = Math.min(min, v);
                    max = Math.max(max, v);
                }
                globalMin = min;
                globalMax = max;
            }
        } else if (dvType.equals("SORTED_NUMERIC")) {
            CachedSortedNumeric cached = sortedNumericCache.computeIfAbsent(field.name, k -> {
                try {
                    return loadSortedNumeric(meta);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for (long[] vals : cached.allValues) {
                    for (long v : vals) {
                        min = Math.min(min, v);
                        max = Math.max(max, v);
                    }
                }
                globalMin = min;
                globalMax = max;
            }
        } else if (dvType.equals("SORTED")) {
            CachedSorted cached = sortedCache.computeIfAbsent(field.name, k -> {
                try {
                    return loadSorted(meta);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                globalMin = 0;
                globalMax = cached.dict.length - 1;
            }
        } else if (dvType.equals("SORTED_SET")) {
            CachedSortedSet cached = sortedSetCache.computeIfAbsent(field.name, k -> {
                try {
                    return loadSortedSet(meta);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                globalMin = 0;
                globalMax = Math.max(0, cached.dict.length - 1);
            }
        } else {
            // BINARY type
            CachedBinary cached = binaryCache.computeIfAbsent(field.name, k -> {
                try {
                    return loadBinary(meta);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
            }
        }

        final int fFirstDocId = firstDocId;
        final int fLastDocId = lastDocId;
        final int fGlobalDocCount = globalDocCount;
        final long fGlobalMin = globalMin;
        final long fGlobalMax = globalMax;

        return new DocValuesSkipper() {
            private int currentMinDocId = -1;
            private int currentMaxDocId = -1;
            private boolean exhausted = fGlobalDocCount == 0;

            @Override
            public void advance(int target) {
                if (fGlobalDocCount == 0 || target > fLastDocId) {
                    currentMinDocId = DocIdSetIterator.NO_MORE_DOCS;
                    currentMaxDocId = DocIdSetIterator.NO_MORE_DOCS;
                    exhausted = true;
                } else {
                    currentMinDocId = fFirstDocId;
                    currentMaxDocId = fLastDocId;
                    exhausted = false;
                }
            }

            @Override
            public int numLevels() {
                return exhausted ? 0 : 1;
            }

            @Override
            public int minDocID(int level) {
                return currentMinDocId;
            }

            @Override
            public int maxDocID(int level) {
                return currentMaxDocId;
            }

            @Override
            public long minValue(int level) {
                return fGlobalMin;
            }

            @Override
            public long maxValue(int level) {
                return fGlobalMax;
            }

            @Override
            public int docCount(int level) {
                return exhausted ? 0 : fGlobalDocCount;
            }

            @Override
            public long minValue() {
                return fGlobalMin;
            }

            @Override
            public long maxValue() {
                return fGlobalMax;
            }

            @Override
            public int docCount() {
                return fGlobalDocCount;
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
