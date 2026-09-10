/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Map;

/**
 * Reads values out of a {@code flat_object} field's value, abstracting over where that value is stored.
 *
 * <p>Two implementations exist -- one backed by {@code _source}, one by a Variant blob in a {@code BinaryDocValues}
 * column -- and everything above this interface is shared, so a caller cannot tell them apart except by what they cost.
 * That is the point: the equivalence tests read the same paths through both and require the same answers, which is what
 * makes the column safe to prefer over {@code _source}.
 *
 * <p>Implementations are <b>not</b> thread safe and are bound to one segment at a time via {@link #setNextReader}, the
 * same contract Lucene's own per-segment readers use.
 *
 * @opensearch.internal
 */
interface FlatObjectValueAccessor {

    /**
     * Binds this accessor to a segment. Must be called before reading any document in that segment.
     */
    void setNextReader(LeafReaderContext context) throws IOException;

    /**
     * Reads the value at {@code path} within the field, with its JSON type kept.
     *
     * <p>Three outcomes, all distinct: {@link PathResolver#MISSING} for an absent path, {@code null} for a path holding
     * JSON null, and otherwise the value -- {@code Long}, {@code Double}, {@code BigDecimal}, {@code Boolean},
     * {@code String}, {@code Map} or {@code List}. Nothing is coerced.
     *
     * @param docId the segment-local document id
     * @param path  a compiled path within the field value
     */
    Object get(int docId, FlatObjectPath path) throws IOException;

    /**
     * Reconstructs the field's whole value for a document, when that value is an object.
     *
     * <p>Used by the whole-value equivalence check. An empty map represents a document without an object value.
     */
    Map<String, Object> getAll(int docId) throws IOException;

    /**
     * Whether the backing store is present for this segment at all.
     *
     * <p>False for the {@code _source} implementation on an index with {@code _source} disabled, where there is nothing to
     * read a value out of.
     */
    boolean valueStoreAvailable();

    /**
     * A short stable name for the backing store, for use in messages.
     */
    String storeName();
}
