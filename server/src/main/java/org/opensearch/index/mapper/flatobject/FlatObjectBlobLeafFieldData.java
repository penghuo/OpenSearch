/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReader;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.mapper.DocValueFetcher;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * One segment's view of a path inside a {@code flat_object} Variant column.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectBlobLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String blobFieldName;
    private final String blobNamesFieldName;
    private final String parentFieldName;
    private final FlatObjectPath path;

    FlatObjectBlobLeafFieldData(
        LeafReader reader,
        String blobFieldName,
        String blobNamesFieldName,
        String parentFieldName,
        FlatObjectPath path
    ) {
        this.reader = reader;
        this.blobFieldName = blobFieldName;
        this.blobNamesFieldName = blobNamesFieldName;
        this.parentFieldName = parentFieldName;
        this.path = path;
    }

    private VariantBlobPathReader open() {
        try {
            return VariantBlobPathReader.open(reader, blobFieldName, blobNamesFieldName, parentFieldName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ScriptDocValues<?> getScriptValues() {
        return new FlatObjectPathScriptDocValues(open(), path);
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new IllegalArgumentException(
            "Field ["
                + path
                + "] of type [flat_object] cannot be aggregated or sorted on: its Variant doc-values column has no declared "
                + "type or arity. Read it from a script, or map the path as its own field."
        );
    }

    @Override
    public DocValueFetcher.Leaf getLeafValueFetcher(DocValueFormat format) {
        VariantBlobPathReader pathReader = open();
        return new DocValueFetcher.Leaf() {
            private Object value;
            private int count;
            private boolean consumed;

            @Override
            public boolean advanceExact(int docId) throws IOException {
                Object resolved = pathReader == null ? PathResolver.MISSING : pathReader.resolve(docId, path);
                if (resolved == PathResolver.MISSING) {
                    value = null;
                    count = 0;
                    consumed = false;
                    return false;
                }
                value = resolved;
                count = 1;
                consumed = false;
                return true;
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public Object nextValue() {
                if (count == 0 || consumed) {
                    throw new IllegalStateException("no flat_object value is positioned for [" + path + "]");
                }
                consumed = true;
                return value;
            }
        };
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public void close() {}
}
