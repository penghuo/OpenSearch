/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Reads the field's value out of {@code _source}.
 *
 * <p>Per access this decompresses the stored-fields block holding the document and parses the <b>whole</b> document's
 * JSON in order to reach one path.
 *
 * <p>The parsed document is cached for the <b>current document only</b>, which is how {@code SourceLookup} behaves
 * during a real search: reading three paths from one document parses it once, and moving to the next document drops it.
 *
 * @opensearch.internal
 */
public class SourceValueAccessor implements FlatObjectValueAccessor {

    public static final String STORE_NAME = "source";

    private final String fieldName;
    private final FieldsVisitor visitor = new FieldsVisitor(true);

    private StoredFields storedFields;
    private boolean sourceAvailable;

    private int cachedDocId = -1;
    private Map<String, Object> cachedSource;

    private long coercionFailures;

    public SourceValueAccessor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        this.storedFields = reader.storedFields();
        // An index created with `_source: {enabled: false}` has no such field in its field infos at all.
        this.sourceAvailable = reader.getFieldInfos().fieldInfo(SourceFieldMapper.NAME) != null;
        this.cachedDocId = -1;
        this.cachedSource = null;
    }

    @Override
    public Object get(int docId, String path, ValueType type) throws IOException {
        Object raw = rawValue(docId, path);
        if (raw == PathResolver.MISSING) {
            return null;
        }
        Object coerced = ValueCoercion.coerce(raw, type);
        if (coerced == ValueCoercion.FAILED) {
            coercionFailures++;
            return null;
        }
        return coerced;
    }

    /**
     * Resolves the path and returns the value exactly as {@code _source} yielded it, without coercion.
     */
    protected Object rawValue(int docId, String path) throws IOException {
        Object fieldValue = fieldValue(docId);
        if (fieldValue == PathResolver.MISSING || fieldValue == null) {
            return PathResolver.MISSING;
        }
        return PathResolver.resolve(fieldValue, path, PathResolver.MAP_NAVIGATOR);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getAll(int docId) throws IOException {
        Object fieldValue = fieldValue(docId);
        if (fieldValue instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Collections.emptyMap();
    }

    @Override
    public long coercionFailures() {
        return coercionFailures;
    }

    @Override
    public boolean valueStoreAvailable() {
        return sourceAvailable;
    }

    @Override
    public String storeName() {
        return STORE_NAME;
    }

    private Object fieldValue(int docId) throws IOException {
        Map<String, Object> source = source(docId);
        if (source == null) {
            return PathResolver.MISSING;
        }
        // The field itself may be reached by a dotted path when it is nested under an object.
        return PathResolver.resolve(source, fieldName, PathResolver.MAP_NAVIGATOR);
    }

    private Map<String, Object> source(int docId) throws IOException {
        if (sourceAvailable == false) {
            return null;
        }
        if (docId == cachedDocId) {
            return cachedSource;
        }
        visitor.reset();
        storedFields.document(docId, visitor);
        BytesReference sourceBytes = visitor.source();
        Map<String, Object> parsed = null;
        if (sourceBytes != null) {
            // A null media type lets this auto-detect, so a `_source` stored as CBOR or SMILE still works, and
            // compression is handled inside convertToMap. The cast picks the MediaType overload, which the bare null
            // would leave ambiguous against the deprecated XContentType one.
            parsed = XContentHelper.convertToMap(sourceBytes, false, (MediaType) null).v2();
        }
        cachedDocId = docId;
        cachedSource = parsed;
        return parsed;
    }
}
