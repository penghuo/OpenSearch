/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

/**
 * Fielddata for one typed path inside a {@code flat_object} Variant column.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectBlobIndexFieldData implements IndexFieldData<FlatObjectBlobLeafFieldData> {

    private final String fieldName;
    private final String blobFieldName;
    private final String blobNamesFieldName;
    private final String parentFieldName;
    private final FlatObjectPath path;

    public FlatObjectBlobIndexFieldData(
        String fieldName,
        String blobFieldName,
        String blobNamesFieldName,
        String parentFieldName,
        FlatObjectPath path
    ) {
        this.fieldName = fieldName;
        this.blobFieldName = blobFieldName;
        this.blobNamesFieldName = blobNamesFieldName;
        this.parentFieldName = parentFieldName;
        this.path = path;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return FlatObjectValuesSourceType.FLAT_OBJECT;
    }

    @Override
    public FlatObjectBlobLeafFieldData load(LeafReaderContext context) {
        return new FlatObjectBlobLeafFieldData(context.reader(), blobFieldName, blobNamesFieldName, parentFieldName, path);
    }

    @Override
    public FlatObjectBlobLeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw unsupported();
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
        throw unsupported();
    }

    private IllegalArgumentException unsupported() {
        return new IllegalArgumentException(
            "Field ["
                + fieldName
                + "] of type [flat_object] cannot be aggregated or sorted on: its Variant doc-values column has no declared "
                + "type or arity. Read it from a script, or map the path as its own field."
        );
    }

    public static class Builder implements IndexFieldData.Builder {

        private final String fieldName;
        private final String blobFieldName;
        private final String blobNamesFieldName;
        private final String parentFieldName;
        private final FlatObjectPath path;

        public Builder(String fieldName, String blobFieldName, String blobNamesFieldName, String parentFieldName, FlatObjectPath path) {
            this.fieldName = fieldName;
            this.blobFieldName = blobFieldName;
            this.blobNamesFieldName = blobNamesFieldName;
            this.parentFieldName = parentFieldName;
            this.path = path;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new FlatObjectBlobIndexFieldData(fieldName, blobFieldName, blobNamesFieldName, parentFieldName, path);
        }
    }
}
