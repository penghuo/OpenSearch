/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

/**
 * Script values for one path in a Variant-backed {@code flat_object}.
 *
 * <p>A resolved path is one logical value even when that value is a list, so {@link #size()} reports presence rather than
 * list cardinality.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectPathScriptDocValues extends ScriptDocValues<Object> {

    private final VariantBlobPathReader reader;
    private final FlatObjectPath path;

    private Object value;
    private int count;

    FlatObjectPathScriptDocValues(VariantBlobPathReader reader, FlatObjectPath path) {
        this.reader = reader;
        this.path = path;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        Object resolved = reader == null ? PathResolver.MISSING : reader.resolve(docId, path);
        if (resolved == PathResolver.MISSING) {
            value = null;
            count = 0;
        } else {
            value = resolved;
            count = 1;
        }
    }

    public Object getValue() {
        return value;
    }

    @Override
    public Object get(int index) {
        if (index != 0 || count == 0) {
            throw new IndexOutOfBoundsException("a flat_object path yields one value per document, so index " + index + " does not exist");
        }
        return value;
    }

    @Override
    public int size() {
        return count;
    }
}
