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
import java.util.Collections;
import java.util.Map;

/**
 * Script values for a complete Variant-backed {@code flat_object}.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectScriptDocValues extends ScriptDocValues<Map<String, Object>> {

    private final VariantBlobObjectReader reader;
    private Map<String, Object> value = Collections.emptyMap();
    private int count;

    FlatObjectScriptDocValues(VariantBlobObjectReader reader) {
        this.reader = reader;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        Map<String, Object> read = reader.advance(docId);
        if (read == null) {
            value = Collections.emptyMap();
            count = 0;
        } else {
            value = read;
            count = 1;
        }
    }

    public Map<String, Object> getValue() {
        return value;
    }

    @Override
    public Map<String, Object> get(int index) {
        if (index != 0 || count == 0) {
            throw new IndexOutOfBoundsException("A flat_object holds one value per document, so index " + index + " does not exist");
        }
        return value;
    }

    @Override
    public int size() {
        return count;
    }
}
