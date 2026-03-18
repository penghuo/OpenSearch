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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Encoding;
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
 * @opensearch.experimental
 */
public class ParquetDocValuesReader extends DocValuesProducer {

    private final IndexInput dataIn;
    private final Map<String, FieldMeta> fields = new HashMap<>();

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

        FieldMeta(
            String fieldName,
            String dvTypeName,
            String repetition,
            String primitiveTypeName,
            long dataStartOffset,
            long dataLength,
            int pageCount,
            boolean hasDictionary
        ) {
            this.fieldName = fieldName;
            this.dvTypeName = dvTypeName;
            this.repetition = repetition;
            this.primitiveTypeName = primitiveTypeName;
            this.dataStartOffset = dataStartOffset;
            this.dataLength = dataLength;
            this.pageCount = pageCount;
            this.hasDictionary = hasDictionary;
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
            fields.put(
                fieldName,
                new FieldMeta(fieldName, dvTypeName, repetition, primitiveTypeName, dataStartOffset, dataLength, pageCount, hasDictionary)
            );
            fieldName = metaIn.readString();
        }
    }

    /**
     * Reads all values for a field from the data file using Parquet's ColumnReader.
     * Returns decoded longs for INT64 fields or Binary values for BINARY fields.
     */
    private ColumnReader readColumn(FieldMeta meta, PrimitiveType parquetType) throws IOException {
        MessageType schema = ParquetTypeMapping.messageType(meta.fieldName, parquetType);
        ColumnDescriptor descriptor = ParquetTypeMapping.columnDescriptor(schema);

        IndexInput slice = dataIn.slice("field:" + meta.fieldName, meta.dataStartOffset, meta.dataLength);

        DictionaryPage dictPage = null;
        if (meta.hasDictionary) {
            int dictBytesLen = slice.readInt();
            int dictSize = slice.readInt();
            String dictEncodingName = slice.readString();
            byte[] dictBytes = new byte[dictBytesLen];
            slice.readBytes(dictBytes, 0, dictBytesLen);
            dictPage = new DictionaryPage(BytesInput.from(dictBytes), dictSize, Encoding.valueOf(dictEncodingName));
        }

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
        return readStore.getColumnReader(descriptor);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        PrimitiveType parquetType = ParquetTypeMapping.numericType(field.name);
        ColumnReader reader = readColumn(meta, parquetType);

        int totalValues = (int) reader.getTotalValueCount();
        long[] values = new long[totalValues];
        for (int i = 0; i < totalValues; i++) {
            values[i] = reader.getLong();
            reader.consume();
        }

        return new NumericDocValues() {
            private int doc = -1;

            @Override
            public long longValue() {
                return values[doc];
            }

            @Override
            public boolean advanceExact(int target) {
                if (target < values.length) {
                    doc = target;
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                return doc < values.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < values.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return values.length;
            }
        };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        PrimitiveType parquetType = ParquetTypeMapping.binaryType(field.name);
        ColumnReader reader = readColumn(meta, parquetType);

        int totalValues = (int) reader.getTotalValueCount();
        BytesRef[] values = new BytesRef[totalValues];
        for (int i = 0; i < totalValues; i++) {
            Binary bin = reader.getBinary();
            byte[] bytes = bin.getBytes();
            values[i] = new BytesRef(bytes, 0, bytes.length);
            reader.consume();
        }

        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return values[doc];
            }

            @Override
            public boolean advanceExact(int target) {
                if (target < values.length) {
                    doc = target;
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                return doc < values.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < values.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return values.length;
            }
        };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        PrimitiveType parquetType = ParquetTypeMapping.sortedType(field.name);
        ColumnReader reader = readColumn(meta, parquetType);

        int totalValues = (int) reader.getTotalValueCount();
        // Read all binary values, build dictionary and ord mapping
        Binary[] rawValues = new Binary[totalValues];
        for (int i = 0; i < totalValues; i++) {
            rawValues[i] = reader.getBinary();
            reader.consume();
        }

        // Build sorted dictionary: collect unique values, sort them, assign ords
        Map<Binary, Integer> valueToOrd = new HashMap<>();
        List<Binary> sortedDict = new ArrayList<>();
        for (Binary val : rawValues) {
            if (valueToOrd.containsKey(val) == false) {
                valueToOrd.put(val, sortedDict.size());
                sortedDict.add(val);
            }
        }
        sortedDict.sort((a, b) -> {
            byte[] ab = a.getBytes();
            byte[] bb = b.getBytes();
            return new BytesRef(ab).compareTo(new BytesRef(bb));
        });
        // Reassign ords after sorting
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }

        int[] ords = new int[totalValues];
        for (int i = 0; i < totalValues; i++) {
            ords[i] = valueToOrd.get(rawValues[i]);
        }

        BytesRef[] dict = new BytesRef[sortedDict.size()];
        for (int i = 0; i < sortedDict.size(); i++) {
            byte[] bytes = sortedDict.get(i).getBytes();
            dict[i] = new BytesRef(bytes, 0, bytes.length);
        }

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
                if (target < ords.length) {
                    doc = target;
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                return doc < ords.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < ords.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return ords.length;
            }
        };
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        PrimitiveType parquetType = ParquetTypeMapping.sortedNumericType(field.name);
        ColumnReader reader = readColumn(meta, parquetType);

        // Decode repeated INT64 using repetition/definition levels
        // rep=0 starts a new record, rep=1 continues current record
        // def=0 means null (empty record), def=1 means value present
        int totalValues = (int) reader.getTotalValueCount();
        List<long[]> docValues = new ArrayList<>();
        List<Long> currentDoc = new ArrayList<>();

        for (int i = 0; i < totalValues; i++) {
            int rep = reader.getCurrentRepetitionLevel();
            int def = reader.getCurrentDefinitionLevel();

            if (rep == 0 && i > 0) {
                // New record — flush previous
                docValues.add(currentDoc.stream().mapToLong(Long::longValue).toArray());
                currentDoc.clear();
            }

            if (def == 1) {
                currentDoc.add(reader.getLong());
            }
            // def == 0 means null/empty — don't add value but still start new record

            reader.consume();
        }
        // Flush last record
        if (totalValues > 0) {
            docValues.add(currentDoc.stream().mapToLong(Long::longValue).toArray());
        }

        long[][] allValues = docValues.toArray(new long[0][]);

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
                if (target < allValues.length) {
                    doc = target;
                    valueIdx = 0;
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                valueIdx = 0;
                if (doc >= allValues.length) {
                    return NO_MORE_DOCS;
                }
                // Skip docs with no values
                while (doc < allValues.length && allValues[doc].length == 0) {
                    doc++;
                }
                return doc < allValues.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target - 1;
                return nextDoc();
            }

            @Override
            public long cost() {
                return allValues.length;
            }
        };
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        FieldMeta meta = fields.get(field.name);
        if (meta == null) {
            throw new IllegalArgumentException("No parquet doc values for field: " + field.name);
        }
        PrimitiveType parquetType = ParquetTypeMapping.sortedSetType(field.name);
        ColumnReader reader = readColumn(meta, parquetType);

        // Decode repeated BINARY using repetition/definition levels
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
                currentDoc.add(reader.getBinary());
            }

            reader.consume();
        }
        if (totalValues > 0) {
            docBinaries.add(new ArrayList<>(currentDoc));
        }

        // Build sorted global dictionary from all unique values
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
        sortedDict.sort((a, b) -> {
            byte[] ab = a.getBytes();
            byte[] bb = b.getBytes();
            return new BytesRef(ab).compareTo(new BytesRef(bb));
        });
        for (int i = 0; i < sortedDict.size(); i++) {
            valueToOrd.put(sortedDict.get(i), i);
        }

        BytesRef[] dict = new BytesRef[sortedDict.size()];
        for (int i = 0; i < sortedDict.size(); i++) {
            byte[] bytes = sortedDict.get(i).getBytes();
            dict[i] = new BytesRef(bytes, 0, bytes.length);
        }

        // Convert each doc's values to sorted ord arrays
        long[][] docOrds = new long[docBinaries.size()][];
        for (int d = 0; d < docBinaries.size(); d++) {
            List<Binary> vals = docBinaries.get(d);
            long[] ordArr = new long[vals.size()];
            for (int v = 0; v < vals.size(); v++) {
                ordArr[v] = valueToOrd.get(vals.get(v));
            }
            // Sort ords within each doc (Lucene requires sorted ords)
            java.util.Arrays.sort(ordArr);
            docOrds[d] = ordArr;
        }

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
                if (target < docOrds.length) {
                    doc = target;
                    ordIdx = 0;
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                ordIdx = 0;
                if (doc >= docOrds.length) {
                    return NO_MORE_DOCS;
                }
                // Skip docs with no values
                while (doc < docOrds.length && docOrds[doc].length == 0) {
                    doc++;
                }
                return doc < docOrds.length ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target - 1;
                return nextDoc();
            }

            @Override
            public long cost() {
                return docOrds.length;
            }
        };
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return null;
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
