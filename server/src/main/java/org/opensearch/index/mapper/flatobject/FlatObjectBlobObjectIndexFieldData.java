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
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Fielddata for reading a complete {@code flat_object} value from its Variant column.
 *
 * @opensearch.internal
 */
@InternalApi
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
        return FlatObjectValuesSourceType.FLAT_OBJECT;
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
            "Field [" + fieldName + "] of type [flat_object] cannot be aggregated or sorted on: it is an object, not a value"
        );
    }

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

        @Override
        public SortedBinaryDocValues getBytesValues() {
            throw new IllegalArgumentException(
                "Field [" + parentFieldName + "] of type [flat_object] cannot be aggregated or sorted on: it is an object, not a value"
            );
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

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
