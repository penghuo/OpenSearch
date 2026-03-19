/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FieldValueFetcher for sorted numeric doc values, for a doc, values will be stored in
 * sorted order in lucene.
 *
 * <p>Caches the doc values iterator per thread per LeafReader to avoid creating a new
 * iterator for every document during _source reconstruction. This eliminates the dominant
 * cost in parquet-backed _source reconstruction: per-document iterator allocation and
 * cache lookups in ParquetDocValuesReader. The cache is thread-safe via ThreadLocal
 * since DerivedFieldGenerator instances are shared across search threads.</p>
 *
 * @opensearch.internal
 */
public class SortedNumericDocValuesFetcher extends FieldValueFetcher {

    private final ThreadLocal<CachedIterator> cached = new ThreadLocal<>();

    public SortedNumericDocValuesFetcher(MappedFieldType mappedFieldType, String SimpleName) {
        super(SimpleName);
        this.mappedFieldType = mappedFieldType;
    }

    private SortedNumericDocValues getDv(LeafReader reader) throws IOException {
        CachedIterator c = cached.get();
        if (c != null && c.reader == reader) {
            return c.dv;
        }
        SortedNumericDocValues dv = reader.getSortedNumericDocValues(mappedFieldType.name());
        cached.set(new CachedIterator(reader, dv));
        return dv;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        try {
            final SortedNumericDocValues dv = getDv(reader);
            if (dv == null || !dv.advanceExact(docId)) {
                return Collections.emptyList();
            }
            int count = dv.docValueCount();
            if (count == 1) {
                return Collections.singletonList(dv.nextValue());
            }
            List<Object> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(dv.nextValue());
            }
            return values;
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
        final SortedNumericDocValues dv;

        CachedIterator(LeafReader reader, SortedNumericDocValues dv) {
            this.reader = reader;
            this.dv = dv;
        }
    }
}
