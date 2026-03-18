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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

/**
 * Round-trip tests for {@link ParquetDocValuesReader}.
 *
 * <p>The reader is the decoding half of the Parquet DocValues codec. Each test writes
 * data using {@link ParquetDocValuesWriter}, then reads it back with
 * {@link ParquetDocValuesReader} and verifies exact value equality. This validates
 * that the reader correctly parses the binary format (codec headers, metadata entries,
 * Parquet-encoded pages, dictionary pages) and reconstructs proper Lucene DocValues
 * iterators. Without correct round-trip behavior, queries, aggregations, and sorting
 * would return wrong results.</p>
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesReaderTests extends OpenSearchTestCase {

    private static final String DATA_EXT = "pdvd";
    private static final String META_EXT = "pdvm";

    /**
     * NUMERIC doc values are the most common type — used for all numeric fields
     * (long, int, double, date). Verifies that single long values per document
     * survive a write→read round trip exactly.
     */
    public void testRoundTripNumeric() throws IOException {
        try (Directory dir = newDirectory()) {
            long[] expected = { 100L, -42L, Long.MAX_VALUE, 0L, Long.MIN_VALUE };
            byte[] segId = writeNumericField(dir, "seg", "price", expected);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, expected.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                FieldInfo fi = newFieldInfo("price", 0, DocValuesType.NUMERIC);
                NumericDocValues dv = reader.getNumeric(fi);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(i, dv.nextDoc());
                    assertEquals("doc " + i, expected[i], dv.longValue());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    /**
     * BINARY doc values store raw byte arrays — used for IP addresses, geo points,
     * and any field needing exact byte-level storage. Verifies byte-exact round trip.
     */
    public void testRoundTripBinary() throws IOException {
        try (Directory dir = newDirectory()) {
            BytesRef[] expected = { new BytesRef("hello"), new BytesRef("world"), new BytesRef("") };
            byte[] segId = writeBinaryField(dir, "seg", "data", expected);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, expected.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                FieldInfo fi = newFieldInfo("data", 0, DocValuesType.BINARY);
                BinaryDocValues dv = reader.getBinary(fi);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(i, dv.nextDoc());
                    assertEquals("doc " + i, expected[i], dv.binaryValue());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    /**
     * SORTED doc values are dictionary-encoded single values per doc — used for keyword
     * fields supporting sorting and terms aggregations. Verifies that the reader
     * reconstructs a correct sorted dictionary and assigns correct ordinals.
     */
    public void testRoundTripSorted() throws IOException {
        try (Directory dir = newDirectory()) {
            BytesRef[] dict = { new BytesRef("active"), new BytesRef("inactive"), new BytesRef("pending") };
            int[] ords = { 0, 2, 1, 0 }; // 4 docs
            byte[] segId = writeSortedField(dir, "seg", "status", dict, ords);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, ords.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                FieldInfo fi = newFieldInfo("status", 0, DocValuesType.SORTED);
                SortedDocValues dv = reader.getSorted(fi);
                assertEquals(3, dv.getValueCount());

                // Verify dictionary is sorted
                for (int i = 1; i < dv.getValueCount(); i++) {
                    assertTrue("Dictionary must be sorted", dv.lookupOrd(i - 1).compareTo(dv.lookupOrd(i)) < 0);
                }

                // Verify each doc's value by looking up through ord
                for (int i = 0; i < ords.length; i++) {
                    assertEquals(i, dv.nextDoc());
                    BytesRef expectedValue = dict[ords[i]];
                    BytesRef actualValue = dv.lookupOrd(dv.ordValue());
                    assertEquals("doc " + i, expectedValue, actualValue);
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    /**
     * SORTED_NUMERIC doc values store multiple longs per doc — used for multi-valued
     * numeric fields. Verifies that repetition levels correctly delimit per-document
     * value boundaries and that all values are decoded exactly.
     */
    public void testRoundTripSortedNumeric() throws IOException {
        try (Directory dir = newDirectory()) {
            long[][] expected = { { 10L, 20L }, { 30L }, { 40L, 50L, 60L } };
            byte[] segId = writeSortedNumericField(dir, "seg", "tags", expected);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, expected.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                FieldInfo fi = newFieldInfo("tags", 0, DocValuesType.SORTED_NUMERIC);
                SortedNumericDocValues dv = reader.getSortedNumeric(fi);
                for (int d = 0; d < expected.length; d++) {
                    assertEquals(d, dv.nextDoc());
                    assertEquals("doc " + d + " count", expected[d].length, dv.docValueCount());
                    for (int v = 0; v < expected[d].length; v++) {
                        assertEquals("doc " + d + " value " + v, expected[d][v], dv.nextValue());
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    /**
     * SORTED_SET doc values store multiple dictionary-encoded values per doc — used
     * for multi-valued keyword fields (e.g., tags). Verifies that the reader builds
     * a correct global sorted dictionary and assigns correct per-doc ord sets.
     */
    public void testRoundTripSortedSet() throws IOException {
        try (Directory dir = newDirectory()) {
            BytesRef[] dict = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c") };
            long[][] ords = { { 0, 2 }, { 1 } };
            byte[] segId = writeSortedSetField(dir, "seg", "cats", dict, ords);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, ords.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                FieldInfo fi = newFieldInfo("cats", 0, DocValuesType.SORTED_SET);
                SortedSetDocValues dv = reader.getSortedSet(fi);
                assertEquals(3, dv.getValueCount());

                // Doc 0: should have values "a" and "c" (ords 0 and 2)
                assertEquals(0, dv.nextDoc());
                assertEquals(2, dv.docValueCount());
                // Values looked up by ord should match original dict entries
                long ord1 = dv.nextOrd();
                long ord2 = dv.nextOrd();
                BytesRef val1 = dv.lookupOrd(ord1);
                BytesRef val2 = dv.lookupOrd(ord2);
                assertEquals(new BytesRef("a"), val1);
                assertEquals(new BytesRef("c"), val2);

                // Doc 1: should have value "b"
                assertEquals(1, dv.nextDoc());
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("b"), dv.lookupOrd(dv.nextOrd()));

                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    /**
     * Multiple fields of different types in the same segment must be independently
     * readable. Real indices have many fields — the reader must correctly use metadata
     * offsets to seek to each field's data independently.
     */
    public void testRoundTripMultipleFields() throws IOException {
        try (Directory dir = newDirectory()) {
            byte[] segId = StringHelper.randomId();
            SegmentWriteState writeState = newSegmentWriteState(dir, "seg", segId, 2);

            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(writeState, DATA_EXT, META_EXT)) {
                FieldInfo numFi = newFieldInfo("price", 0, DocValuesType.NUMERIC);
                writer.addNumericField(numFi, new StubProducer() {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo f) {
                        return new ArrayNumericDocValues(new long[] { 10L, 20L });
                    }
                });
                FieldInfo binFi = newFieldInfo("data", 1, DocValuesType.BINARY);
                writer.addBinaryField(binFi, new StubProducer() {
                    @Override
                    public BinaryDocValues getBinary(FieldInfo f) {
                        return new ArrayBinaryDocValues(new BytesRef[] { new BytesRef("x"), new BytesRef("y") });
                    }
                });
            }

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, 2);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                // Read numeric
                NumericDocValues numDv = reader.getNumeric(newFieldInfo("price", 0, DocValuesType.NUMERIC));
                assertEquals(0, numDv.nextDoc());
                assertEquals(10L, numDv.longValue());
                assertEquals(1, numDv.nextDoc());
                assertEquals(20L, numDv.longValue());

                // Read binary
                BinaryDocValues binDv = reader.getBinary(newFieldInfo("data", 1, DocValuesType.BINARY));
                assertEquals(0, binDv.nextDoc());
                assertEquals(new BytesRef("x"), binDv.binaryValue());
                assertEquals(1, binDv.nextDoc());
                assertEquals(new BytesRef("y"), binDv.binaryValue());
            }
        }
    }

    /**
     * advanceExact() is used by Lucene for random-access doc value lookups during
     * scoring and aggregation. Must return true for valid doc IDs and false for
     * out-of-range targets.
     */
    public void testAdvanceExact() throws IOException {
        try (Directory dir = newDirectory()) {
            long[] values = { 100L, 200L, 300L };
            byte[] segId = writeNumericField(dir, "seg", "val", values);

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, values.length);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                NumericDocValues dv = reader.getNumeric(newFieldInfo("val", 0, DocValuesType.NUMERIC));
                assertTrue(dv.advanceExact(2));
                assertEquals(300L, dv.longValue());
                assertTrue(dv.advanceExact(0));
                assertEquals(100L, dv.longValue());
                assertFalse(dv.advanceExact(3));
            }
        }
    }

    /**
     * checkIntegrity() validates the data file checksum. This is called during
     * segment opening to detect corruption from disk errors or incomplete writes.
     */
    public void testCheckIntegrity() throws IOException {
        try (Directory dir = newDirectory()) {
            byte[] segId = writeNumericField(dir, "seg", "val", new long[] { 1L });

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, 1);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                reader.checkIntegrity(); // Should not throw
            }
        }
    }

    /**
     * An empty segment (no fields written) must be readable without error.
     * This occurs after all documents in a segment are deleted during merge.
     */
    public void testEmptySegment() throws IOException {
        try (Directory dir = newDirectory()) {
            byte[] segId = StringHelper.randomId();
            SegmentWriteState writeState = newSegmentWriteState(dir, "seg", segId, 0);
            try (ParquetDocValuesWriter writer = new ParquetDocValuesWriter(writeState, DATA_EXT, META_EXT)) {
                // No fields
            }

            SegmentReadState readState = newSegmentReadState(dir, "seg", segId, 0);
            try (ParquetDocValuesReader reader = new ParquetDocValuesReader(readState, DATA_EXT, META_EXT)) {
                reader.checkIntegrity();
            }
        }
    }

    // ---- Write helpers ----

    private byte[] writeNumericField(Directory dir, String seg, String fieldName, long[] values) throws IOException {
        byte[] segId = StringHelper.randomId();
        SegmentWriteState ws = newSegmentWriteState(dir, seg, segId, values.length);
        try (ParquetDocValuesWriter w = new ParquetDocValuesWriter(ws, DATA_EXT, META_EXT)) {
            w.addNumericField(newFieldInfo(fieldName, 0, DocValuesType.NUMERIC), new StubProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo f) {
                    return new ArrayNumericDocValues(values);
                }
            });
        }
        return segId;
    }

    private byte[] writeBinaryField(Directory dir, String seg, String fieldName, BytesRef[] values) throws IOException {
        byte[] segId = StringHelper.randomId();
        SegmentWriteState ws = newSegmentWriteState(dir, seg, segId, values.length);
        try (ParquetDocValuesWriter w = new ParquetDocValuesWriter(ws, DATA_EXT, META_EXT)) {
            w.addBinaryField(newFieldInfo(fieldName, 0, DocValuesType.BINARY), new StubProducer() {
                @Override
                public BinaryDocValues getBinary(FieldInfo f) {
                    return new ArrayBinaryDocValues(values);
                }
            });
        }
        return segId;
    }

    private byte[] writeSortedField(Directory dir, String seg, String fieldName, BytesRef[] dict, int[] ords) throws IOException {
        byte[] segId = StringHelper.randomId();
        SegmentWriteState ws = newSegmentWriteState(dir, seg, segId, ords.length);
        try (ParquetDocValuesWriter w = new ParquetDocValuesWriter(ws, DATA_EXT, META_EXT)) {
            w.addSortedField(newFieldInfo(fieldName, 0, DocValuesType.SORTED), new StubProducer() {
                @Override
                public SortedDocValues getSorted(FieldInfo f) {
                    return new ArraySortedDocValues(dict, ords);
                }
            });
        }
        return segId;
    }

    private byte[] writeSortedNumericField(Directory dir, String seg, String fieldName, long[][] values) throws IOException {
        byte[] segId = StringHelper.randomId();
        SegmentWriteState ws = newSegmentWriteState(dir, seg, segId, values.length);
        try (ParquetDocValuesWriter w = new ParquetDocValuesWriter(ws, DATA_EXT, META_EXT)) {
            w.addSortedNumericField(newFieldInfo(fieldName, 0, DocValuesType.SORTED_NUMERIC), new StubProducer() {
                @Override
                public SortedNumericDocValues getSortedNumeric(FieldInfo f) {
                    return new ArraySortedNumericDocValues(values);
                }
            });
        }
        return segId;
    }

    private byte[] writeSortedSetField(Directory dir, String seg, String fieldName, BytesRef[] dict, long[][] ords) throws IOException {
        byte[] segId = StringHelper.randomId();
        SegmentWriteState ws = newSegmentWriteState(dir, seg, segId, ords.length);
        try (ParquetDocValuesWriter w = new ParquetDocValuesWriter(ws, DATA_EXT, META_EXT)) {
            w.addSortedSetField(newFieldInfo(fieldName, 0, DocValuesType.SORTED_SET), new StubProducer() {
                @Override
                public SortedSetDocValues getSortedSet(FieldInfo f) {
                    return new ArraySortedSetDocValues(dict, ords);
                }
            });
        }
        return segId;
    }

    // ---- State helpers ----

    private SegmentWriteState newSegmentWriteState(Directory dir, String segName, byte[] segId, int maxDoc) {
        SegmentInfo si = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            segName,
            maxDoc,
            false,
            false,
            null,
            Collections.emptyMap(),
            segId,
            Collections.emptyMap(),
            null
        );
        return new SegmentWriteState(InfoStream.NO_OUTPUT, dir, si, new FieldInfos(new FieldInfo[0]), null, IOContext.DEFAULT);
    }

    private SegmentReadState newSegmentReadState(Directory dir, String segName, byte[] segId, int maxDoc) {
        SegmentInfo si = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            segName,
            maxDoc,
            false,
            false,
            null,
            Collections.emptyMap(),
            segId,
            Collections.emptyMap(),
            null
        );
        return new SegmentReadState(dir, si, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    }

    private FieldInfo newFieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            org.apache.lucene.index.IndexOptions.NONE,
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            org.apache.lucene.index.VectorEncoding.FLOAT32,
            org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    // ---- Stub producer and iterators ----

    static abstract class StubProducer extends DocValuesProducer {
        @Override
        public NumericDocValues getNumeric(FieldInfo f) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo f) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSorted(FieldInfo f) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo f) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo f) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo f) throws IOException {
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
            return ++doc < values.length ? doc : NO_MORE_DOCS;
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
            return ++doc < values.length ? doc : NO_MORE_DOCS;
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
            return ++doc < ords.length ? doc : NO_MORE_DOCS;
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
            return doc < values.length ? doc : NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            valueIdx = 0;
            return doc < values.length ? doc : NO_MORE_DOCS;
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
            return doc < ords.length ? doc : NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            ordIdx = 0;
            return doc < ords.length ? doc : NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return ords.length;
        }
    }
}
