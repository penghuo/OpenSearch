/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.script.AggregationScript;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.FieldContext;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.util.function.LongSupplier;

/**
 * An unregistered values-source type used to reject aggregations on Variant-backed {@code flat_object} fields.
 *
 * @opensearch.internal
 */
enum FlatObjectValuesSourceType implements ValuesSourceType {

    FLAT_OBJECT;

    @Override
    public ValuesSource getEmpty() {
        return CoreValuesSourceType.BYTES.getEmpty();
    }

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        return CoreValuesSourceType.BYTES.getScript(script, scriptValueType);
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        return CoreValuesSourceType.BYTES.getField(fieldContext, script);
    }

    @Override
    public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
        return CoreValuesSourceType.BYTES.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
    }

    @Override
    public String typeName() {
        return "flat_object";
    }

}
