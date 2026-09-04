/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * What {@code doc['attributes']} gives a script: the {@code flat_object}'s whole value, as a map.
 *
 * <p>Exactly one element per document, because the mapper refuses more than one object for a field. So {@code .value} is
 * the object, and the list shape is only there because {@code doc[...]} is declared to return a {@link ScriptDocValues}
 * and every {@code ScriptDocValues} is a {@code List}.
 *
 * <p>{@link #getValue()} is declared as {@link Map}, which is what lets a script write
 * {@code doc['attributes'].value['status']} with nothing else whitelisted: painless dispatches the subscript through
 * {@code java.util.Map}, already allowed. The map itself is a lazy view -- reaching one key reads one value and no key
 * names at all; see {@link VariantBlobObjectReader}.
 *
 * @opensearch.internal
 */
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

    /** The field's value for the current document, or an empty map if it has none. */
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
