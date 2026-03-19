/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

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

        CachedNumeric(int[] docIds, long[] values) {
            this.docIds = docIds;
            this.values = values;
        }
    }

    /** Cached decoded data for BINARY fields. */
    private static class CachedBinary {
        final int[] docIds;
        final BytesRef[] values;

        CachedBinary(int[] docIds, BytesRef[] values) {
            this.docIds = docIds;
            this.values = values;
        }
    }

    /** Cached decoded data for SORTED fields. */
    private static class CachedSorted {
        final int[] docIds;
        final int[] ords;
        final BytesRef[] dict;

        CachedSorted(int[] docIds, int[] ords, BytesRef[] dict) {
            this.docIds = docIds;
            this.ords = ords;
            this.dict = dict;
        }
    }

    /** Cached decoded data for SORTED_NUMERIC fields. */
    private static class CachedSortedNumeric {
        final int[] docIds;
        final long[][] allValues;

        CachedSortedNumeric(int[] docIds, long[][] allValues) {
            this.docIds = docIds;
            this.allValues = allValues;
        }
    }

    /** Cached decoded data for SORTED_SET fields. */
    private static class CachedSortedSet {
        final int[] docIds;
        final long[][] docOrds;
        final BytesRef[] dict;

        CachedSortedSet(int[] docIds, long[][] docOrds, BytesRef[] dict) {
            this.docIds = docIds;
            this.docOrds = docOrds;
            this.dict = dict;
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
        int docCount = docIdSlice.readInt();
        int[] docIds = new int[docCount];
        for (int i = 0; i < docCount; i++) {
            docIds[i] = docIdSlice.readInt();
        }

        if (docCount == 0) {
            return new FieldData(docIds, null);
        }

        // Read data pages from the beginning of the slice
        List<DataPage> dataPages = new ArrayList<>();
        long totalValueCount = 0;
        long totalRowCount = 0;
        for (int i = 0; i < meta.pageCount; i++) {
            int pageBytesLen = slice.readInt();
            int valueCount = slice.readInt();
            int rowCount = slice.readInt();
            String valuesEncodingName = slice.readString();
            String rlEncodingName = slice.readString();
            String dlEncodingName = slice.readString();
            byte[] pageBytes = new byte[pageBytesLen];
            slice.readBytes(pageBytes, 0, pageBytesLen);
            dataPages.add(
                new DataPageV1(
                    BytesInput.from(pageBytes),
                    valueCount,
                    pageBytesLen,
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
            int dictBytesLen = slice.readInt();
            int dictSize = slice.readInt();
            String dictEncodingName = slice.readString();
            byte[] dictBytes = new byte[dictBytesLen];
            slice.readBytes(dictBytes, 0, dictBytesLen);
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
        return new FieldData(docIds, readStore.getColumnReader(descriptor));
    }

    /** Holds doc IDs and the column reader for a field. */
    private static class FieldData {
        final int[] docIds;
        final ColumnReader reader;

        FieldData(int[] docIds, ColumnReader reader) {
            this.docIds = docIds;
            this.reader = reader;
        }
    }

    private CachedNumeric loadNumeric(FieldMeta meta) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.numericType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedNumeric(docIds, new long[0]);
        }
        ColumnReader reader = fieldData.reader;
        long[] values = new long[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            values[i] = reader.getLong();
            reader.consume();
        }
        return new CachedNumeric(docIds, values);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedNumeric cached = numericCache.get(field.name);
        if (cached == null) {
            cached = loadNumeric(meta);
            numericCache.put(field.name, cached);
        }
        int[] docIds = cached.docIds;
        long[] values = cached.values;

        if (docIds.length == 0) {
            return DocValues.emptyNumeric();
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
                int found = java.util.Arrays.binarySearch(docIds, target);
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
                int found = java.util.Arrays.binarySearch(docIds, Math.max(idx + 1, 0), docIds.length, target);
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

    private CachedBinary loadBinary(FieldMeta meta) throws IOException {
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(meta.fieldName);
        FieldData fieldData = readFieldData(meta, parquetType);
        int[] docIds = fieldData.docIds;
        if (docIds.length == 0) {
            return new CachedBinary(docIds, new BytesRef[0]);
        }
        ColumnReader reader = fieldData.reader;
        BytesRef[] values = new BytesRef[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            Binary bin = reader.getBinary();
            values[i] = new BytesRef(bin.getBytes().clone());
            reader.consume();
        }
        return new CachedBinary(docIds, values);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedBinary cached = binaryCache.get(field.name);
        if (cached == null) {
            cached = loadBinary(meta);
            binaryCache.put(field.name, cached);
        }
        int[] docIds = cached.docIds;
        BytesRef[] values = cached.values;

        if (docIds.length == 0) {
            return DocValues.emptyBinary();
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
                int found = java.util.Arrays.binarySearch(docIds, target);
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
                int found = java.util.Arrays.binarySearch(docIds, Math.max(idx + 1, 0), docIds.length, target);
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
            return new CachedSorted(docIds, new int[0], new BytesRef[0]);
        }
        ColumnReader reader = fieldData.reader;
        Binary[] rawValues = new Binary[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            rawValues[i] = reader.getBinary().copy();
            reader.consume();
        }
        Map<Binary, Integer> valueToOrd = new HashMap<>();
        List<Binary> sortedDict = new ArrayList<>();
        for (Binary val : rawValues) {
            if (valueToOrd.containsKey(val) == false) {
                valueToOrd.put(val, sortedDict.size());
                sortedDict.add(val);
            }
        }
        sortedDict.sort((a, b) -> new BytesRef(a.getBytes()).compareTo(new BytesRef(b.getBytes())));
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }
        int[] ords = new int[docIds.length];
        for (int i = 0; i < docIds.length; i++) {
            ords[i] = valueToOrd.get(rawValues[i]);
        }
        BytesRef[] dict = new BytesRef[sortedDict.size()];
        for (int i = 0; i < sortedDict.size(); i++) {
            dict[i] = new BytesRef(sortedDict.get(i).getBytes());
        }
        return new CachedSorted(docIds, ords, dict);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSorted cached = sortedCache.get(field.name);
        if (cached == null) {
            cached = loadSorted(meta);
            sortedCache.put(field.name, cached);
        }
        int[] docIds = cached.docIds;
        int[] ords = cached.ords;
        BytesRef[] dict = cached.dict;

        if (docIds.length == 0) {
            return DocValues.emptySorted();
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
                int found = java.util.Arrays.binarySearch(docIds, target);
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
                int found = java.util.Arrays.binarySearch(docIds, Math.max(idx + 1, 0), docIds.length, target);
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
            return new CachedSortedNumeric(docIds, new long[0][]);
        }
        ColumnReader reader = fieldData.reader;
        int totalValues = (int) reader.getTotalValueCount();
        List<long[]> docValues = new ArrayList<>();
        List<Long> currentDoc = new ArrayList<>();
        for (int i = 0; i < totalValues; i++) {
            int rep = reader.getCurrentRepetitionLevel();
            int def = reader.getCurrentDefinitionLevel();
            if (rep == 0 && i > 0) {
                docValues.add(currentDoc.stream().mapToLong(Long::longValue).toArray());
                currentDoc.clear();
            }
            if (def == 1) {
                currentDoc.add(reader.getLong());
            }
            reader.consume();
        }
        if (totalValues > 0) {
            docValues.add(currentDoc.stream().mapToLong(Long::longValue).toArray());
        }
        return new CachedSortedNumeric(docIds, docValues.toArray(new long[0][]));
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSortedNumeric cached = sortedNumericCache.get(field.name);
        if (cached == null) {
            cached = loadSortedNumeric(meta);
            sortedNumericCache.put(field.name, cached);
        }
        int[] docIds = cached.docIds;
        long[][] allValues = cached.allValues;

        if (docIds.length == 0) {
            return DocValues.emptySortedNumeric();
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
                int found = java.util.Arrays.binarySearch(docIds, target);
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
                int found = java.util.Arrays.binarySearch(docIds, Math.max(idx + 1, 0), docIds.length, target);
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
            return new CachedSortedSet(docIds, new long[0][], new BytesRef[0]);
        }
        ColumnReader reader = fieldData.reader;
        int totalValues = (int) reader.getTotalValueCount();
        List<List<Binary>> docBinaries = new ArrayList<>();
        List<Binary> currentDoc = new ArrayList<>();
        for (int i = 0; i < totalValues; i++) {
            int rep = reader.getCurrentRepetitionLevel();
            int def = reader.getCurrentDefinitionLevel();
            if (rep == 0 && i > 0) {
                docBinaries.add(new ArrayList<>(currentDoc));
                currentDoc.clear();
            }
            if (def == 1) {
                currentDoc.add(reader.getBinary().copy());
            }
            reader.consume();
        }
        if (totalValues > 0) {
            docBinaries.add(new ArrayList<>(currentDoc));
        }
        Map<Binary, Integer> valueToOrd = new HashMap<>();
        List<Binary> sortedDict = new ArrayList<>();
        for (List<Binary> docVals : docBinaries) {
            for (Binary val : docVals) {
                if (valueToOrd.containsKey(val) == false) {
                    valueToOrd.put(val, sortedDict.size());
                    sortedDict.add(val);
                }
            }
        }
        sortedDict.sort((a, b) -> new BytesRef(a.getBytes()).compareTo(new BytesRef(b.getBytes())));
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }
        BytesRef[] dict = new BytesRef[sortedDict.size()];
        for (int i = 0; i < sortedDict.size(); i++) {
            dict[i] = new BytesRef(sortedDict.get(i).getBytes());
        }
        long[][] docOrds = new long[docBinaries.size()][];
        for (int d = 0; d < docBinaries.size(); d++) {
            List<Binary> vals = docBinaries.get(d);
            long[] ordArr = new long[vals.size()];
            for (int v = 0; v < vals.size(); v++) {
                ordArr[v] = valueToOrd.get(vals.get(v));
            }
            java.util.Arrays.sort(ordArr);
            docOrds[d] = ordArr;
        }
        return new CachedSortedSet(docIds, docOrds, dict);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        CachedSortedSet cached = sortedSetCache.get(field.name);
        if (cached == null) {
            cached = loadSortedSet(meta);
            sortedSetCache.put(field.name, cached);
        }
        int[] docIds = cached.docIds;
        long[][] docOrds = cached.docOrds;
        BytesRef[] dict = cached.dict;

        if (docIds.length == 0) {
            return DocValues.emptySortedSet();
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
                int found = java.util.Arrays.binarySearch(docIds, target);
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
                int found = java.util.Arrays.binarySearch(docIds, Math.max(idx + 1, 0), docIds.length, target);
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
            CachedNumeric cached = numericCache.get(field.name);
            if (cached == null) {
                cached = loadNumeric(meta);
                numericCache.put(field.name, cached);
            }
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
            CachedSortedNumeric cached = sortedNumericCache.get(field.name);
            if (cached == null) {
                cached = loadSortedNumeric(meta);
                sortedNumericCache.put(field.name, cached);
            }
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
            CachedSorted cached = sortedCache.get(field.name);
            if (cached == null) {
                cached = loadSorted(meta);
                sortedCache.put(field.name, cached);
            }
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                globalMin = 0;
                globalMax = cached.dict.length - 1;
            }
        } else if (dvType.equals("SORTED_SET")) {
            CachedSortedSet cached = sortedSetCache.get(field.name);
            if (cached == null) {
                cached = loadSortedSet(meta);
                sortedSetCache.put(field.name, cached);
            }
            if (cached.docIds.length > 0) {
                firstDocId = cached.docIds[0];
                lastDocId = cached.docIds[cached.docIds.length - 1];
                globalDocCount = cached.docIds.length;
                globalMin = 0;
                globalMax = Math.max(0, cached.dict.length - 1);
            }
        } else {
            // BINARY type
            CachedBinary cached = binaryCache.get(field.name);
            if (cached == null) {
                cached = loadBinary(meta);
                binaryCache.put(field.name, cached);
            }
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
