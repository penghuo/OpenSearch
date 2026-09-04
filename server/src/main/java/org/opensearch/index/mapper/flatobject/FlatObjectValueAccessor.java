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
 * Reads typed values out of a {@code flat_object} field's value, abstracting over where that value is stored.
 *
 * <p>Two implementations exist — one backed by {@code _source}, one by a Variant blob in a {@code BinaryDocValues}
 * column — and everything above this interface is shared, so a caller cannot tell them apart except by what they cost.
 *
 * <p>Implementations are <b>not</b> thread safe and are bound to one segment at a time via {@link #setNextReader}, the
 * same contract Lucene's own per-segment readers use.
 *
 * @opensearch.internal
 */
public interface FlatObjectValueAccessor {

    /**
     * Binds this accessor to a segment. Must be called before reading any document in that segment.
     */
    void setNextReader(LeafReaderContext context) throws IOException;

    /**
     * Reads the value at {@code path} within the field, coerced to {@code type}.
     *
     * <p>Returns {@code null} both when the path is absent and when it holds a stored null. Callers that need to tell
     * those apart, or that need to know a value was dropped because it could not be coerced, should consult
     * {@link #coercionFailures()}.
     *
     * @param docId the segment-local document id
     * @param path  a dotted path within the field value, resolved per {@link PathResolver}
     * @param type  the type to return the value as
     * @return the value, or {@code null} if absent, null, or not coercible
     */
    Object get(int docId, String path, ValueType type) throws IOException;

    /**
     * Reconstructs the field's whole value for a document.
     *
     * <p>Used by the whole-value equivalence check, which asserts that both stores can rebuild the original object.
     *
     * @return the reconstructed value, or an empty map if the document has no value for this field
     */
    Map<String, Object> getAll(int docId) throws IOException;

    /**
     * The number of values that existed but could not be coerced to the requested type since this accessor was created.
     *
     * <p>Both implementations must exclude exactly the same values, so this is a contract rather than a diagnostic.
     */
    long coercionFailures();

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
