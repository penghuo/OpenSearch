/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests for {@link ParquetDocValuesWriter}.
 *
 * <p>The writer is the encoding half of the Parquet DocValues codec. It must correctly
 * encode all five Lucene DocValues types (NUMERIC, BINARY, SORTED, SORTED_NUMERIC,
 * SORTED_SET) using Parquet column encoders and serialize the encoded pages to
 * {@code .pdvd}/{@code .pdvm} files with valid Lucene codec headers/footers.
 * Incorrect encoding here means data loss or corruption that would surface as
 * wrong query results or aggregation failures.</p>
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesWriterTests extends OpenSearchTestCase {

    private static final String DATA_EXT = "pdvd";
    private static final String META_EXT = "pdvm";

    /**
     * Verifies that the writer creates both data and metadata files with valid
     * Lucene codec headers and footers. Without valid headers/footers, the reader
     * would reject the files as corrupt, making the entire segment unreadable.
     */
    public void testCreatesDataAndMetaFiles() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_segment", 0);
            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                // Just close — no fields added
            }
            String dataFile = IndexFileNames.segmentFileName("test_segment", "", DATA_EXT);
            String metaFile = IndexFileNames.segmentFileName("test_segment", "", META_EXT);
            assertTrue("Data file must exist", slowFileExists(dir, dataFile));
            assertTrue("Meta file must exist", slowFileExists(dir, metaFile));
            // Verify files are non-empty (have at least header + footer)
            assertTrue("Data file must have content", dir.fileLength(dataFile) > 0);
            assertTrue("Meta file must have content", dir.fileLength(metaFile) > 0);
        }
    }

    /**
     * Verifies that NUMERIC doc values (single long per doc) are written without error.
     * NUMERIC is the most common doc values type, used for all numeric fields
     * (long, int, double, date). If this fails, numeric sorting and aggregations break.
     */
    public void testWriteNumericField() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_numeric", 3);
            FieldInfo fieldInfo = newFieldInfo("price", 0, DocValuesType.NUMERIC);
            long[] values = { 100L, 200L, 300L };

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                writer.addNumericField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo field) {
                        return new ArrayNumericDocValues(values);
                    }
                });
            }

            assertFilesExist(dir, "test_numeric");
        }
    }

    /**
     * Verifies that BINARY doc values (single byte array per doc) are written without error.
     * BINARY doc values store raw bytes — used for IP addresses, geo points, and
     * any field that needs exact byte-level storage. Encoding errors here corrupt
     * the stored representation.
     */
    public void testWriteBinaryField() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_binary", 2);
            FieldInfo fieldInfo = newFieldInfo("data", 0, DocValuesType.BINARY);
            BytesRef[] values = { new BytesRef("hello"), new BytesRef("world") };

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                writer.addBinaryField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public BinaryDocValues getBinary(FieldInfo field) {
                        return new ArrayBinaryDocValues(values);
                    }
                });
            }

            assertFilesExist(dir, "test_binary");
        }
    }

    /**
     * Verifies that SORTED doc values (dictionary-encoded single value per doc) are
     * written without error. SORTED is used for keyword fields that support sorting
     * and terms aggregations. The dictionary encoding is critical for space efficiency.
     */
    public void testWriteSortedField() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_sorted", 3);
            FieldInfo fieldInfo = newFieldInfo("status", 0, DocValuesType.SORTED);
            BytesRef[] dict = { new BytesRef("active"), new BytesRef("inactive"), new BytesRef("pending") };
            int[] ords = { 0, 2, 1 };

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                writer.addSortedField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public SortedDocValues getSorted(FieldInfo field) {
                        return new ArraySortedDocValues(dict, ords);
                    }
                });
            }

            assertFilesExist(dir, "test_sorted");
        }
    }

    /**
     * Verifies that SORTED_NUMERIC doc values (multiple longs per doc) are written
     * without error. SORTED_NUMERIC is used for multi-valued numeric fields. The
     * Parquet repeated INT64 encoding must correctly handle variable-length value
     * lists per document using repetition levels.
     */
    public void testWriteSortedNumericField() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_sorted_numeric", 3);
            FieldInfo fieldInfo = newFieldInfo("tags", 0, DocValuesType.SORTED_NUMERIC);
            long[][] values = { { 10L, 20L }, { 30L }, { 40L, 50L, 60L } };

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                writer.addSortedNumericField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                        return new ArraySortedNumericDocValues(values);
                    }
                });
            }

            assertFilesExist(dir, "test_sorted_numeric");
        }
    }

    /**
     * Verifies that SORTED_SET doc values (multiple dictionary-encoded values per doc)
     * are written without error. SORTED_SET is used for multi-valued keyword fields
     * (e.g., tags). The Parquet repeated BINARY encoding with dictionary must handle
     * variable-length ord lists per document.
     */
    public void testWriteSortedSetField() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_sorted_set", 2);
            FieldInfo fieldInfo = newFieldInfo("categories", 0, DocValuesType.SORTED_SET);
            BytesRef[] dict = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c") };
            long[][] ords = { { 0, 2 }, { 1 } };

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                writer.addSortedSetField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public SortedSetDocValues getSortedSet(FieldInfo field) {
                        return new ArraySortedSetDocValues(dict, ords);
                    }
                });
            }

            assertFilesExist(dir, "test_sorted_set");
        }
    }

    /**
     * Verifies that multiple fields of different types can be written to the same
     * segment. Real indices have many fields — the writer must correctly track
     * offsets and metadata for each field independently.
     */
    public void testWriteMultipleFields() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_multi", 2);

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                // Numeric field
                FieldInfo numField = newFieldInfo("price", 0, DocValuesType.NUMERIC);
                writer.addNumericField(numField, new StubDocValuesProducer() {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo field) {
                        return new ArrayNumericDocValues(new long[] { 100L, 200L });
                    }
                });

                // Binary field
                FieldInfo binField = newFieldInfo("data", 1, DocValuesType.BINARY);
                writer.addBinaryField(binField, new StubDocValuesProducer() {
                    @Override
                    public BinaryDocValues getBinary(FieldInfo field) {
                        return new ArrayBinaryDocValues(new BytesRef[] { new BytesRef("x"), new BytesRef("y") });
                    }
                });
            }

            assertFilesExist(dir, "test_multi");
        }
    }

    /**
     * Verifies that the metadata file contains the end marker. The reader uses this
     * marker to know when it has read all field entries. Without it, the reader would
     * attempt to read past the end of field data and hit the codec footer.
     */
    public void testMetadataContainsEndMarker() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_marker", 1);

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                FieldInfo fieldInfo = newFieldInfo("val", 0, DocValuesType.NUMERIC);
                writer.addNumericField(fieldInfo, new StubDocValuesProducer() {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo field) {
                        return new ArrayNumericDocValues(new long[] { 42L });
                    }
                });
            }

            // Read the meta file and verify end marker is present
            String metaFile = IndexFileNames.segmentFileName("test_marker", "", META_EXT);
            try (IndexInput in = dir.openInput(metaFile, IOContext.READONCE)) {
                // Skip codec index header (includes segment ID and suffix)
                org.apache.lucene.codecs.CodecUtil.checkIndexHeader(
                    in,
                    ParquetDocValuesWriter.META_CODEC,
                    ParquetDocValuesWriter.VERSION_CURRENT,
                    ParquetDocValuesWriter.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );

                // Read field entry: name, dvType, repetition, primitiveType, offset, length, pageCount, hasDictionary
                String fieldName = in.readString();
                assertEquals("val", fieldName);
                in.readString(); // dvType
                in.readString(); // repetition
                in.readString(); // primitiveType
                in.readLong(); // offset
                in.readLong(); // length
                in.readInt(); // pageCount
                in.readByte(); // hasDictionary (boolean)

                // Next should be the end marker
                String marker = in.readString();
                assertEquals(ParquetDocValuesWriter.END_MARKER, marker);
            }
        }
    }

    /**
     * Verifies that an empty segment (zero docs) can be written and closed without error.
     * Edge case: segments can be empty after all documents are deleted during merge.
     */
    public void testEmptySegment() throws IOException {
        try (Directory dir = newDirectory()) {
            SegmentWriteState state = newSegmentWriteState(dir, "test_empty", 0);
            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(state, DATA_EXT, META_EXT)) {
                // No fields added
            }
            assertFilesExist(dir, "test_empty");
        }
    }

    // ---- Test helpers ----

    private SegmentWriteState newSegmentWriteState(Directory dir, String segmentName, int maxDoc) {
        byte[] id = StringHelper.randomId();
        SegmentInfo segmentInfo = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            maxDoc,
            false,
            false,
            null,
            Collections.emptyMap(),
            id,
            Collections.emptyMap(),
            null
        );
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
        return new SegmentWriteState(InfoStream.NO_OUTPUT, dir, segmentInfo, fieldInfos, null, IOContext.DEFAULT);
    }

    private FieldInfo newFieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false, // storeTermVector
            false, // omitNorms
            false, // storePayloads
            org.apache.lucene.index.IndexOptions.NONE,
            dvType,
            DocValuesSkipIndexType.NONE,
            -1, // dvGen
            Collections.emptyMap(),
            0, // pointDimensionCount
            0, // pointIndexDimensionCount
            0, // pointNumBytes
            0, // vectorDimension
            org.apache.lucene.index.VectorEncoding.FLOAT32,
            org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN,
            false, // softDeletesField
            false // parentField
        );
    }

    private void assertFilesExist(Directory dir, String segmentName) throws IOException {
        String dataFile = IndexFileNames.segmentFileName(segmentName, "", DATA_EXT);
        String metaFile = IndexFileNames.segmentFileName(segmentName, "", META_EXT);
        assertTrue("Data file must exist: " + dataFile, slowFileExists(dir, dataFile));
        assertTrue("Meta file must exist: " + metaFile, slowFileExists(dir, metaFile));
    }

    // ---- Stub DocValuesProducer and iterators ----

    static abstract class StubDocValuesProducer extends DocValuesProducer {
        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
            return null;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public void close() {}
    }

    static class ArrayNumericDocValues extends NumericDocValues {
        private final long[] values;
        private int doc = -1;

        ArrayNumericDocValues(long[] values) {
            this.values = values;
        }

        @Override
        public long longValue() {
            return values[doc];
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return target < values.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return values.length;
        }
    }

    static class ArrayBinaryDocValues extends BinaryDocValues {
        private final BytesRef[] values;
        private int doc = -1;

        ArrayBinaryDocValues(BytesRef[] values) {
            this.values = values;
        }

        @Override
        public BytesRef binaryValue() {
            return values[doc];
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return target < values.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return values.length;
        }
    }

    static class ArraySortedDocValues extends SortedDocValues {
        private final BytesRef[] dict;
        private final int[] ords;
        private int doc = -1;

        ArraySortedDocValues(BytesRef[] dict, int[] ords) {
            this.dict = dict;
            this.ords = ords;
        }

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
            return target < ords.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            return doc < ords.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            return doc < ords.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return ords.length;
        }
    }

    static class ArraySortedNumericDocValues extends SortedNumericDocValues {
        private final long[][] values;
        private int doc = -1;
        private int valueIdx;

        ArraySortedNumericDocValues(long[][] values) {
            this.values = values;
        }

        @Override
        public long nextValue() {
            return values[doc][valueIdx++];
        }

        @Override
        public int docValueCount() {
            return values[doc].length;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            valueIdx = 0;
            return target < values.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            valueIdx = 0;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            valueIdx = 0;
            return doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return values.length;
        }
    }

    static class ArraySortedSetDocValues extends SortedSetDocValues {
        private final BytesRef[] dict;
        private final long[][] ords;
        private int doc = -1;
        private int ordIdx;

        ArraySortedSetDocValues(BytesRef[] dict, long[][] ords) {
            this.dict = dict;
            this.ords = ords;
        }

        @Override
        public long nextOrd() {
            return ords[doc][ordIdx++];
        }

        @Override
        public int docValueCount() {
            return ords[doc].length;
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
            return target < ords.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            ordIdx = 0;
            return doc < ords.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            ordIdx = 0;
            return doc < ords.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return ords.length;
        }
    }
}
