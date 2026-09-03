/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Fielddata for the {@code flat_object} field itself, so {@code doc['attributes']} hands a script the whole value.
 *
 * <p>Bytes, not numeric, and deliberately so: {@code ValuesSourceConfig} reads the values-source type straight off the
 * fielddata, so declaring numeric here would make {@code sum} on the bare parent field resolve and return nonsense. The
 * parent stays un-aggregatable -- {@code isAggregatable()} is false for it and {@code docValueFormat} still refuses it --
 * so in practice a script is the only thing that reaches this.
 *
 * <p>Sorting is not supported. The parent is an object; there is no order to put objects in.
 *
 * @opensearch.internal
 */
public final class FlatObjectBlobObjectIndexFieldData implements IndexFieldData<FlatObjectBlobObjectIndexFieldData.Leaf> {

    private final String fieldName;
    private final String blobFieldName;
    private final String blobNamesFieldName;

    public FlatObjectBlobObjectIndexFieldData(String fieldName, String blobFieldName, String blobNamesFieldName) {
        this.fieldName = fieldName;
        this.blobFieldName = blobFieldName;
        this.blobNamesFieldName = blobNamesFieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    public Leaf load(LeafReaderContext context) {
        return new Leaf(context.reader(), blobFieldName, blobNamesFieldName, fieldName);
    }

    @Override
    public Leaf loadDirect(LeafReaderContext context) {
        return load(context);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException("Cannot sort on [" + fieldName + "]: it is an object, not a value");
    }

    @Override
    public BucketedSort newBucketedSort(
        org.opensearch.common.util.BigArrays bigArrays,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("Cannot sort on [" + fieldName + "]: it is an object, not a value");
    }

    /**
     * One segment's view, holding no cursor of its own.
     *
     * <p>A {@code LeafFieldData} may be shared, while the doc-values iterators underneath are forward-only and
     * single-threaded, so each value view opens its own reader. Holding one reader on the leaf and handing it to every
     * caller would have them share a cursor and a read-mutated name cache.
     */
    public static final class Leaf implements LeafFieldData {

        private final LeafReader reader;
        private final String blobFieldName;
        private final String blobNamesFieldName;
        private final String parentFieldName;

        Leaf(LeafReader reader, String blobFieldName, String blobNamesFieldName, String parentFieldName) {
            this.reader = reader;
            this.blobFieldName = blobFieldName;
            this.blobNamesFieldName = blobNamesFieldName;
            this.parentFieldName = parentFieldName;
        }

        private VariantBlobObjectReader open() {
            try {
                return VariantBlobObjectReader.open(reader, blobFieldName, blobNamesFieldName, parentFieldName);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return new FlatObjectScriptDocValues(open());
        }

        /**
         * The document's scalar values, for anything that asks the parent field for bytes.
         *
         * <p>Nothing in the query path does today -- the parent is not aggregatable and cannot be sorted on -- but
         * returning nothing would be a silent answer where an approximation of the field's own values is at least honest.
         */
        @Override
        public SortedBinaryDocValues getBytesValues() {
            final VariantBlobObjectReader objectReader = open();
            return new SortedBinaryDocValues() {
                private List<BytesRef> values = List.of();
                private int next;

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    Map<String, Object> view = objectReader.advance(docId);
                    values = view == null ? List.of() : objectReader.valuesOf(view);
                    next = 0;
                    return values.isEmpty() == false;
                }

                @Override
                public int docValueCount() {
                    return values.size();
                }

                @Override
                public BytesRef nextValue() {
                    return values.get(next++);
                }
            };
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /** Builder for the field type to hand back. Nothing is held, so the cache and breaker have nothing to do. */
    public static class Builder implements IndexFieldData.Builder {

        private final String fieldName;
        private final String blobFieldName;
        private final String blobNamesFieldName;

        public Builder(String fieldName, String blobFieldName, String blobNamesFieldName) {
            this.fieldName = fieldName;
            this.blobFieldName = blobFieldName;
            this.blobNamesFieldName = blobNamesFieldName;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new FlatObjectBlobObjectIndexFieldData(fieldName, blobFieldName, blobNamesFieldName);
        }
    }
}
