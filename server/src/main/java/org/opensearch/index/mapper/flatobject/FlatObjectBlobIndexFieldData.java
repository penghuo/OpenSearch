/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

/**
 * Fielddata for one path inside a {@code flat_object}'s Variant blob column, so {@code attributes.status} can be
 * aggregated and sorted with no script and no derived field.
 *
 * <p>Numeric because that is what makes a schemaless path useful: {@code sum}, {@code avg}, {@code min}, {@code max} and a
 * numeric sort all need a numeric values source. A {@code terms} aggregation asking for strings still works, because
 * {@code CoreValuesSourceType.BYTES} falls back to a plain {@code ValuesSource.Bytes.FieldData} for fielddata that is not
 * ordinal-based, and that needs only {@code getBytesValues()}.
 *
 * <p><b>Width.</b> {@link NumericType#DOUBLE}, because the column is schemaless and nothing declares a width. Integers
 * above 2^53 therefore lose precision through an aggregation. A sort escapes this: {@code numeric_type: long} is passed
 * through to {@code sortField} and {@link FlatObjectBlobLeafFieldData#getLongValues()} reads the stored value directly
 * rather than casting a double, so an exact ordering is available when asked for.
 *
 * <p><b>Not cached.</b> {@code load} and {@code loadDirect} are the same thing. There is nothing to cache -- the leaf holds
 * no materialised state -- and building fresh keeps every doc-values cursor confined to one iteration, which is what makes
 * a shared leaf safe.
 *
 * @opensearch.internal
 */
public final class FlatObjectBlobIndexFieldData extends IndexNumericFieldData {

    /** The full keyed name, e.g. {@code attributes.status}. */
    private final String fieldName;
    /** The Lucene doc-values columns, both named after the parent field. */
    private final String blobFieldName;
    private final String blobNamesFieldName;
    /** The parent field's own name, used only to tell a broken index from a segment with no such field. */
    private final String parentFieldName;
    /** The path within the blob, e.g. {@code status}. */
    private final String path;

    public FlatObjectBlobIndexFieldData(
        String fieldName,
        String blobFieldName,
        String blobNamesFieldName,
        String parentFieldName,
        String path
    ) {
        this.fieldName = fieldName;
        this.blobFieldName = blobFieldName;
        this.blobNamesFieldName = blobNamesFieldName;
        this.parentFieldName = parentFieldName;
        this.path = path;
    }

    /**
     * The keyed name, not the parent's.
     *
     * <p>It labels the {@code SortField} and {@code LongValuesComparatorSource} asserts on it, so it has to be the name the
     * user asked for. That it names no Lucene field is deliberate and safe: with neither points nor a doc-values skipper
     * behind it, Lucene finds nothing to build competitive iteration from and simply reads every document.
     */
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DOUBLE;
    }

    /**
     * Always true, and load-bearing.
     *
     * <p>When this is false, {@code IndexNumericFieldData.sortField} short-circuits to a raw
     * {@code SortedNumericSortField(getFieldName(), ...)} for a MIN/MAX sort -- a direct Lucene read of a doc-values column
     * called {@code attributes.status}, which does not exist. Lucene returns an empty iterator for an absent field rather
     * than failing, so the sort would silently place every document in the missing bucket.
     */
    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        return new FlatObjectBlobLeafFieldData(context.reader(), blobFieldName, blobNamesFieldName, parentFieldName, path);
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    /**
     * Builder for the field type to hand back.
     *
     * <p>The cache and breaker are ignored on purpose: nothing is held, so there is nothing to cache or to account.
     */
    public static class Builder implements IndexFieldData.Builder {

        private final String fieldName;
        private final String blobFieldName;
        private final String blobNamesFieldName;
        private final String parentFieldName;
        private final String path;

        public Builder(String fieldName, String blobFieldName, String blobNamesFieldName, String parentFieldName, String path) {
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
