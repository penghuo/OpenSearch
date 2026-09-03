/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.flatobject.ValueType;
import org.opensearch.index.mapper.flatobject.VariantBlobValueAccessor;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Script-facing accessor for a {@code flat_object} field's Variant blob column.
 *
 * <p>Gives a derived field script a typed read of one path — {@code variant('attributes').getLong('status')} — reading only
 * that field's bytes rather than the document's {@code _source}.
 *
 * <p>Every getter returns a boxed type and {@code null} when the path is absent, holds null, or cannot be represented as
 * the requested type. A primitive return would have no way to say "not there", and scripts would have to guess a sentinel.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class VariantFieldAccess {

    private final String field;
    private final VariantBlobValueAccessor accessor;
    private final boolean available;
    /** The segment this instance is bound to, so a cached instance can be detected as stale. */
    private final LeafReaderContext boundTo;
    private int docId = -1;

    public VariantFieldAccess(String field, LeafReaderContext context) throws IOException {
        this.field = field;
        this.boundTo = context;
        this.accessor = new VariantBlobValueAccessor(field);
        this.accessor.setNextReader(context);
        this.available = accessor.valueStoreAvailable();
    }

    /** Whether this instance is still valid for a given segment. */
    public boolean isBoundTo(LeafReaderContext context) {
        return boundTo == context;
    }

    public String field() {
        return field;
    }

    public void setDocument(int docId) {
        this.docId = docId;
    }

    /** Whether this segment has a blob column for the field. */
    public boolean available() {
        return available;
    }

    public Object get(String path) {
        return read(path, ValueType.RAW);
    }

    public Long getLong(String path) {
        return (Long) read(path, ValueType.LONG);
    }

    public Double getDouble(String path) {
        return (Double) read(path, ValueType.DOUBLE);
    }

    public String getString(String path) {
        return (String) read(path, ValueType.STRING);
    }

    public Boolean getBoolean(String path) {
        return (Boolean) read(path, ValueType.BOOLEAN);
    }

    private Object read(String path, ValueType type) {
        if (available == false || docId < 0) {
            return null;
        }
        try {
            return accessor.get(docId, path, type);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read [" + path + "] from the Variant blob", e);
        }
    }
}
