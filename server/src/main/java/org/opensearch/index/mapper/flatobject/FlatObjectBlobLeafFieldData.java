/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.variant.Variant;
import org.opensearch.common.variant.VariantType;
import org.opensearch.index.fielddata.AbstractSortedNumericDocValues;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One segment's view of a single path inside a {@code flat_object}'s Variant blob column.
 *
 * <p>Holds no cursor of its own. Lucene doc-values iterators are forward-only and single-threaded while a
 * {@link LeafNumericFieldData} may be shared, so every value view opens its own {@link VariantBlobPathReader} and every
 * piece of mutable state lives inside that reader for the life of one iteration.
 *
 * <p>Nothing is materialised, so {@link #ramBytesUsed()} is zero: the cost of this field is doc-values reads, not heap.
 *
 * @opensearch.internal
 */
final class FlatObjectBlobLeafFieldData implements LeafNumericFieldData {

    private final LeafReader reader;
    private final String blobFieldName;
    private final String blobNamesFieldName;
    private final String parentFieldName;
    private final String path;

    FlatObjectBlobLeafFieldData(LeafReader reader, String blobFieldName, String blobNamesFieldName, String parentFieldName, String path) {
        this.reader = reader;
        this.blobFieldName = blobFieldName;
        this.blobNamesFieldName = blobNamesFieldName;
        this.parentFieldName = parentFieldName;
        this.path = path;
    }

    private VariantBlobPathReader open() throws IOException {
        return VariantBlobPathReader.open(reader, blobFieldName, blobNamesFieldName, parentFieldName, path);
    }

    /**
     * Collects the readable values at the path for one document, ascending.
     *
     * <p>Arrays are flattened recursively, matching what a declared field does with the same JSON: a real {@code long} field
     * given {@code [[80,443],8080]} produces three doc values, so this must too. Anything that cannot be read as the
     * requested type -- a word asked for as a number, a nested object -- is dropped rather than failing the request.
     *
     * <p>Ascending order is a contract, not a nicety: {@code MultiValueMode.MIN} takes the first value and {@code MAX} walks
     * to the last, so leaving {@code [443, 80]} in document order would report a minimum of 443.
     */
    private static void flatten(Variant value, List<Variant> out) {
        if (value.type() == VariantType.ARRAY) {
            int size = value.arraySize();
            for (int i = 0; i < size; i++) {
                flatten(value.arrayGet(i), out);
            }
        } else {
            out.add(value);
        }
    }

    /**
     * @return the coerced values at the path for {@code docId}, ascending, or an empty list
     */
    private static List<Object> read(VariantBlobPathReader reader, int docId, ValueType type) throws IOException {
        List<Variant> resolved = new ArrayList<>(1);
        reader.advance(docId, resolved);
        if (resolved.isEmpty()) {
            return List.of();
        }
        List<Variant> scalars = new ArrayList<>(4);
        for (Variant value : resolved) {
            flatten(value, scalars);
        }
        List<Object> values = new ArrayList<>(scalars.size());
        for (Variant scalar : scalars) {
            if (scalar.type() == VariantType.OBJECT) {
                // Reconstructing it would need key names, and no scalar reading of an object is meaningful anyway.
                continue;
            }
            Object raw = scalar.toJavaObject();
            if (raw == null) {
                // Present and explicitly null: no value, and not a failure to read one.
                continue;
            }
            Object coerced = ValueCoercion.coerce(raw, type);
            if (coerced == ValueCoercion.FAILED) {
                continue;
            }
            values.add(coerced);
        }
        return values;
    }

    @Override
    public SortedNumericDocValues getLongValues() {
        final VariantBlobPathReader pathReader;
        try {
            pathReader = open();
        } catch (IOException e) {
            throw new UncheckedIOExceptionWrapper(e);
        }
        if (pathReader == null) {
            return org.apache.lucene.index.DocValues.emptySortedNumeric();
        }
        return new AbstractSortedNumericDocValues() {
            private long[] values = new long[0];
            private int count;
            private int next;

            @Override
            public boolean advanceExact(int target) throws IOException {
                List<Object> read = read(pathReader, target, ValueType.LONG);
                count = read.size();
                if (count == 0) {
                    return false;
                }
                if (values.length < count) {
                    values = new long[count];
                }
                for (int i = 0; i < count; i++) {
                    values[i] = ((Number) read.get(i)).longValue();
                }
                Arrays.sort(values, 0, count);
                next = 0;
                return true;
            }

            @Override
            public long nextValue() {
                return values[next++];
            }

            @Override
            public int docValueCount() {
                return count;
            }
        };
    }

    @Override
    public SortedNumericDoubleValues getDoubleValues() {
        final VariantBlobPathReader pathReader;
        try {
            pathReader = open();
        } catch (IOException e) {
            throw new UncheckedIOExceptionWrapper(e);
        }
        if (pathReader == null) {
            return org.opensearch.index.fielddata.FieldData.emptySortedNumericDoubles();
        }
        return new SortedNumericDoubleValues() {
            private double[] values = new double[0];
            private int count;
            private int next;

            @Override
            public boolean advanceExact(int target) throws IOException {
                List<Object> read = read(pathReader, target, ValueType.DOUBLE);
                count = read.size();
                if (count == 0) {
                    return false;
                }
                if (values.length < count) {
                    values = new double[count];
                }
                for (int i = 0; i < count; i++) {
                    values[i] = ((Number) read.get(i)).doubleValue();
                }
                Arrays.sort(values, 0, count);
                next = 0;
                return true;
            }

            @Override
            public double nextValue() {
                return values[next++];
            }

            @Override
            public int docValueCount() {
                return count;
            }
        };
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        final VariantBlobPathReader pathReader;
        try {
            pathReader = open();
        } catch (IOException e) {
            throw new UncheckedIOExceptionWrapper(e);
        }
        if (pathReader == null) {
            return org.opensearch.index.fielddata.FieldData.emptySortedBinary();
        }
        return new SortedBinaryDocValues() {
            private BytesRef[] values = new BytesRef[0];
            private int count;
            private int next;

            @Override
            public boolean advanceExact(int docId) throws IOException {
                List<Object> read = read(pathReader, docId, ValueType.STRING);
                count = read.size();
                if (count == 0) {
                    return false;
                }
                if (values.length < count) {
                    values = new BytesRef[count];
                }
                for (int i = 0; i < count; i++) {
                    values[i] = new BytesRef(((String) read.get(i)).getBytes(StandardCharsets.UTF_8));
                }
                Arrays.sort(values, 0, count);
                next = 0;
                return true;
            }

            @Override
            public BytesRef nextValue() {
                return values[next++];
            }

            @Override
            public int docValueCount() {
                return count;
            }
        };
    }

    /**
     * Doubles, matching {@link FlatObjectBlobIndexFieldData#getNumericType()}.
     *
     * <p>Reaching this means a script subscripted a keyed path -- {@code doc['attributes.status']} rather than
     * {@code doc['attributes']}. Implemented rather than refused because the interface requires it, and because the
     * alternative is worse: the field's existing fielddata hands back every attribute in the document as text.
     */
    @Override
    public ScriptDocValues<?> getScriptValues() {
        return new ScriptDocValues.Doubles(getDoubleValues());
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public void close() {}

    /** Lets the IOException from opening a segment cursor cross {@code getXValues()}, which cannot declare it. */
    static final class UncheckedIOExceptionWrapper extends RuntimeException {
        UncheckedIOExceptionWrapper(IOException cause) {
            super(cause);
        }
    }
}
