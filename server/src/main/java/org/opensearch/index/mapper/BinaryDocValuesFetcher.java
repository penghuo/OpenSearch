/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * FieldValueFetcher for binary doc values, used for text fields that store their values
 * as BinaryDocValues for parquet-backed _source reconstruction.
 *
 * <p>Caches the doc values iterator per thread per LeafReader to avoid creating a new
 * iterator for every document during _source reconstruction.</p>
 *
 * @opensearch.internal
 */
public class BinaryDocValuesFetcher extends FieldValueFetcher {

    private final ThreadLocal<CachedIterator> cached = new ThreadLocal<>();

    public BinaryDocValuesFetcher(MappedFieldType mappedFieldType, String simpleName) {
        super(simpleName);
        this.mappedFieldType = mappedFieldType;
    }

    private BinaryDocValues getDv(LeafReader reader) throws IOException {
        CachedIterator c = cached.get();
        if (c != null && c.reader == reader) {
            return c.dv;
        }
        BinaryDocValues dv = reader.getBinaryDocValues(mappedFieldType.name());
        cached.set(new CachedIterator(reader, dv));
        return dv;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        try {
            final BinaryDocValues dv = getDv(reader);
            if (dv == null || !dv.advanceExact(docId)) {
                return Collections.emptyList();
            }
            return Collections.singletonList(BytesRef.deepCopyOf(dv.binaryValue()));
        } catch (IOException e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
    }

    @Override
    public Object convert(Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return mappedFieldType.valueForDisplay(value);
    }

    private static class CachedIterator {
        final LeafReader reader;
        final BinaryDocValues dv;

        CachedIterator(LeafReader reader, BinaryDocValues dv) {
            this.reader = reader;
            this.dv = dv;
        }
    }
}
