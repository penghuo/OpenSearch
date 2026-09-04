/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.variant.Variant;
import org.opensearch.common.variant.VariantFormatException;
import org.opensearch.common.variant.VariantJson;
import org.opensearch.common.variant.VariantMetadata;
import org.opensearch.common.variant.VariantType;
import org.opensearch.index.mapper.FlatObjectFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Reads the field's value out of a Variant blob held in a {@code BinaryDocValues} column.
 *
 * <p>Per access this reads only the blob for one document, binary-searches the requested key in the Variant metadata, and
 * decodes the value that offset points at. Nothing outside the field's own bytes is touched, and the document's other
 * fields are never decompressed or parsed.
 *
 * @opensearch.internal
 */
public class VariantBlobValueAccessor implements FlatObjectValueAccessor {

    public static final String STORE_NAME = "variant_blob";

    private final String blobFieldName;
    private final String blobMetaFieldName;
    private final String blobNamesFieldName;

    private LeafReader reader;
    private BinaryDocValues docValues;
    /** The name column, whose ordinals a document's field ids index. */
    private SortedSetDocValues nameValues;
    /** Every name in the segment by ordinal, read once. Bounded by distinct names, so it always fits. */
    private byte[][] nameTable;
    /** Reused buffer for one document's ordinals, so a read allocates nothing. */
    private int[] documentOrdinals = new int[16];
    private boolean columnPresent;
    /** The document each iterator currently sits on, tracked so a backwards read can restart them. */
    private int iteratorDoc = -1;
    private int nameIteratorDoc = -1;

    private int cachedDocId = -1;
    private Variant cachedVariant;

    private long coercionFailures;

    public VariantBlobValueAccessor(String fieldName) {
        this.blobFieldName = FlatObjectFieldMapper.blobFieldName(fieldName);
        this.blobMetaFieldName = FlatObjectFieldMapper.blobMetaFieldName(fieldName);
        this.blobNamesFieldName = FlatObjectFieldMapper.blobNamesFieldName(fieldName);
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        this.reader = context.reader();
        this.columnPresent = reader.getFieldInfos().fieldInfo(blobFieldName) != null;
        // A segment carrying a column under this name connects its field ids to the names by an explicit table, so its ids
        // are not in name order. Refuse it rather than read it by the rule below, which would return every value under the
        // wrong key and raise nothing.
        if (columnPresent && reader.getFieldInfos().fieldInfo(blobMetaFieldName) != null) {
            throw new VariantFormatException(
                "segment carries [" + blobMetaFieldName + "], which belongs to a layout this reader no longer supports; reindex the field"
            );
        }
        openIterator();
        this.cachedDocId = -1;
        this.cachedVariant = null;
        if (columnPresent) {
            this.nameTable = readNameTable();
        }
    }

    /**
     * Reads every name in the segment into memory, in ordinal order.
     *
     * <p>Ordinal order is what makes this cheap. Lucene stores sorted terms in compressed blocks of sixteen, so walking
     * ordinals consecutively decompresses each block once and takes sixteen names from it -- measured at ~230 ns per name
     * against ~14,000 ns when the same names are resolved in a scattered order.
     */
    private byte[][] readNameTable() throws IOException {
        SortedSetDocValues names = DocValues.getSortedSet(reader, blobNamesFieldName);
        int count = (int) names.getValueCount();
        byte[][] table = new byte[count][];
        for (int ord = 0; ord < count; ord++) {
            BytesRef term = names.lookupOrd(ord);
            byte[] copy = new byte[term.length];
            System.arraycopy(term.bytes, term.offset, copy, 0, term.length);
            table[ord] = copy;
        }
        return table;
    }

    private void openIterator() throws IOException {
        this.docValues = columnPresent ? DocValues.getBinary(reader, blobFieldName) : null;
        this.nameValues = columnPresent ? DocValues.getSortedSet(reader, blobNamesFieldName) : null;
        this.iteratorDoc = -1;
        this.nameIteratorDoc = -1;
    }

    /**
     * Stands in for a container that a scalar read cannot use, so the container never has to be materialised.
     *
     * <p>{@link ValueCoercion} already fails for a {@code Map} or {@code List} asked for as a number or string. Building
     * that map first, only to discard it, was the most wasteful path in the reader: a nested object would be decoded in
     * full for a guaranteed-useless result.
     */
    private static final Object CONTAINER = new Object();

    @Override
    public Object get(int docId, String path, ValueType type) throws IOException {
        Object raw = rawValue(docId, path, type);
        if (raw == PathResolver.MISSING) {
            return null;
        }
        if (raw == CONTAINER) {
            // Same outcome ValueCoercion.coerce(Map/List, scalar) gives, reached without decoding the subtree.
            coercionFailures++;
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
     * Resolves the path and returns the value as a plain Java object, without coercion.
     *
     * <p>Scalars are converted to boxed Java values here rather than in the coercion layer, so both stores hand the shared
     * coercion table the same types. Containers are only materialised when the caller actually asked for the raw value.
     */
    protected Object rawValue(int docId, String path, ValueType type) throws IOException {
        Variant root = variant(docId);
        if (root == null) {
            return PathResolver.MISSING;
        }
        Object resolved = PathResolver.resolve(root, path, VARIANT_NAVIGATOR);
        if (resolved == PathResolver.MISSING) {
            return PathResolver.MISSING;
        }
        if (resolved == null) {
            return null;
        }
        Variant node = (Variant) resolved;
        if (type != ValueType.RAW) {
            VariantType nodeType = node.type();
            if (nodeType == VariantType.OBJECT || nodeType == VariantType.ARRAY) {
                return CONTAINER;
            }
        }
        return node.toJavaObject();
    }

    @Override
    public Map<String, Object> getAll(int docId) throws IOException {
        Variant root = variant(docId);
        if (root == null) {
            return Collections.emptyMap();
        }
        return VariantJson.toMap(root);
    }

    @Override
    public long coercionFailures() {
        return coercionFailures;
    }

    @Override
    public boolean valueStoreAvailable() {
        return columnPresent;
    }

    @Override
    public String storeName() {
        return STORE_NAME;
    }

    /**
     * Navigator over a Variant, mirroring {@link PathResolver#MAP_NAVIGATOR} so both stores resolve paths identically.
     *
     * <p>A Variant holding an explicit null is reported as {@code null} rather than as missing, which is the same
     * distinction the map navigator draws with {@code containsKey}.
     */
    static final PathResolver.Navigator VARIANT_NAVIGATOR = (node, key) -> {
        if (node instanceof Variant variant && variant.type() == VariantType.OBJECT) {
            Variant child = variant.objectGet(key);
            if (child == null) {
                return PathResolver.MISSING;
            }
            return child.isNull() ? null : child;
        }
        return PathResolver.MISSING;
    };

    /**
     * Decodes the blob for a document, or returns {@code null} if it has none.
     */
    private Variant variant(int docId) throws IOException {
        if (columnPresent == false) {
            return null;
        }
        if (docId == cachedDocId) {
            return cachedVariant;
        }

        // BinaryDocValues is a forward-only iterator, but the fetch phase addresses documents out of order. Without this
        // restart a backwards read would return no value and raise nothing, which reads as "this document has no value".
        if (docId < iteratorDoc) {
            openIterator();
        }

        Variant decoded = decodeDocument(docId);
        iteratorDoc = docValues.docID();
        cachedDocId = docId;
        cachedVariant = decoded;
        return decoded;
    }

    /**
     * Reads one document: its names as ordinals, and its value tree.
     *
     * <p>A field id is a position in the ordinal list, because the writer numbered field ids in the document's own key order
     * and the name column returns that document's ordinals ascending -- which, ordinals being assigned in name order, is the
     * same order.
     */
    private Variant decodeDocument(int docId) throws IOException {
        if (docId < nameIteratorDoc) {
            this.nameValues = DocValues.getSortedSet(reader, blobNamesFieldName);
            this.nameIteratorDoc = -1;
        }
        Variant decoded = null;
        if (docValues.advanceExact(docId)) {
            // A document with no keys at all writes nothing to the name column, so its absence here means zero keys rather
            // than a missing value. Requiring it would report an empty object as no object.
            int count = nameValues.advanceExact(docId) ? nameValues.docValueCount() : 0;
            if (documentOrdinals.length < count) {
                documentOrdinals = new int[ArrayUtil.oversize(count, Integer.BYTES)];
            }
            for (int i = 0; i < count; i++) {
                documentOrdinals[i] = (int) nameValues.nextOrd();
            }
            decoded = decode(docValues.binaryValue(), new VariantMetadata(nameTable, documentOrdinals, count));
        }
        nameIteratorDoc = nameValues.docID();
        return decoded;
    }

    /**
     * Wraps a document's value bytes without copying them, pairing them with the metadata for its ordinal.
     *
     * <p>An earlier single-column version copied both halves into fresh arrays on every document, which allocated roughly
     * the blob's size per document <em>per aggregation</em>. The doc-values {@code BytesRef} already exposes
     * (array, offset, length), so the value is read in place.
     */
    private Variant decode(BytesRef value, VariantMetadata metadata) {
        return new Variant(metadata, value.bytes, value.offset, value.offset, value.offset + value.length);
    }
}
