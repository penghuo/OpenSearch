/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FieldValueFetcher for sorted set doc values, for a doc, values will be deduplicated and sorted while stored in
 * lucene.
 *
 * <p>Caches the doc values iterator per thread per LeafReader to avoid creating a new
 * iterator for every document during _source reconstruction. This eliminates the dominant
 * cost in parquet-backed _source reconstruction: per-document iterator allocation and
 * cache lookups in ParquetDocValuesReader. The cache is thread-safe via ThreadLocal
 * since DerivedFieldGenerator instances are shared across search threads.</p>
 *
 * @opensearch.internal
 */
public class SortedSetDocValuesFetcher extends FieldValueFetcher {

    private final ThreadLocal<CachedIterator> cached = new ThreadLocal<>();

    public SortedSetDocValuesFetcher(MappedFieldType mappedFieldType, String simpleName) {
        super(simpleName);
        this.mappedFieldType = mappedFieldType;
    }

    private SortedSetDocValues getDv(LeafReader reader) throws IOException {
        CachedIterator c = cached.get();
        if (c != null && c.reader == reader) {
            return c.dv;
        }
        SortedSetDocValues dv = reader.getSortedSetDocValues(mappedFieldType.name());
        cached.set(new CachedIterator(reader, dv));
        return dv;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        try {
            final SortedSetDocValues dv = getDv(reader);
            if (dv == null || !dv.advanceExact(docId)) {
                return Collections.emptyList();
            }
            int count = dv.docValueCount();
            if (count == 1) {
                BytesRef value = dv.lookupOrd(dv.nextOrd());
                return Collections.singletonList(BytesRef.deepCopyOf(value));
            }
            List<Object> values = new ArrayList<>(count);
            for (int ord = 0; ord < count; ord++) {
                BytesRef value = dv.lookupOrd(dv.nextOrd());
                values.add(BytesRef.deepCopyOf(value));
            }
            return values;
        } catch (IOException e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
    }

    /**
     * Writes keyword doc values directly to the builder, avoiding BytesRef.deepCopyOf and
     * intermediate List allocation. The BytesRef from lookupOrd is only valid until the next
     * lookupOrd call, but convert() processes it immediately so no copy is needed.
     */
    @Override
    void writeDirect(XContentBuilder builder, LeafReader reader, int docId) throws IOException {
        try {
            final SortedSetDocValues dv = getDv(reader);
            if (dv == null || !dv.advanceExact(docId)) {
                return;
            }
            int count = dv.docValueCount();
            if (count == 1) {
                BytesRef value = dv.lookupOrd(dv.nextOrd());
                builder.field(simpleName, convert(value));
            } else {
                builder.startArray(simpleName);
                for (int i = 0; i < count; i++) {
                    BytesRef value = dv.lookupOrd(dv.nextOrd());
                    builder.value(convert(value));
                }
                builder.endArray();
            }
        } catch (Exception e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
    }

    @Override
    public Object convert(Object value) {
        return mappedFieldType.valueForDisplay(value);
    }

    private static class CachedIterator {
        final LeafReader reader;
        final SortedSetDocValues dv;

        CachedIterator(LeafReader reader, SortedSetDocValues dv) {
            this.reader = reader;
            this.dv = dv;
        }
    }
}
