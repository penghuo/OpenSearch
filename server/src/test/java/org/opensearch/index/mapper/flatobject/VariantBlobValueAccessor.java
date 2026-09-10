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
class VariantBlobValueAccessor implements FlatObjectValueAccessor {

    public static final String STORE_NAME = "variant_blob";

    private final String blobFieldName;
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

    public VariantBlobValueAccessor(String fieldName) {
        this.blobFieldName = FlatObjectFieldMapper.blobFieldName(fieldName);
        this.blobNamesFieldName = FlatObjectFieldMapper.blobNamesFieldName(fieldName);
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        this.reader = context.reader();
        this.columnPresent = reader.getFieldInfos().fieldInfo(blobFieldName) != null;
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

    @Override
    public Object get(int docId, FlatObjectPath path) throws IOException {
        Variant root = variant(docId);
        if (root == null || root.isNull()) {
            // A root null is the sentinel for a document whose value could not be encoded; nothing else writes one.
            return PathResolver.MISSING;
        }
        return PathResolver.resolve(root, path, VARIANT_NAVIGATOR);
    }

    @Override
    public Map<String, Object> getAll(int docId) throws IOException {
        Variant root = variant(docId);
        if (root == null || root.type() != VariantType.OBJECT) {
            return Collections.emptyMap();
        }
        return VariantJson.toMap(root);
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
     * <p>Keys are resolved by name here, not by field id: this accessor already holds the segment's whole name table, and
     * being the reference implementation for the equivalence tests it is better kept obvious than fast. The field-id route
     * the query path uses lives in {@link VariantBlobPathReader}.
     *
     * <p>A Variant holding an explicit null is reported as {@code null} rather than as missing, which is the same
     * distinction the map navigator draws with {@code containsKey}.
     */
    static final PathResolver.Navigator VARIANT_NAVIGATOR = new PathResolver.Navigator() {
        @Override
        public Object child(Object node, String key) {
            if (node instanceof Variant variant && variant.type() == VariantType.OBJECT) {
                Variant child = variant.objectGet(key);
                if (child == null) {
                    return PathResolver.MISSING;
                }
                return child.isNull() ? null : child;
            }
            return PathResolver.MISSING;
        }

        @Override
        public int arraySize(Object node) {
            if (node instanceof Variant variant && variant.type() == VariantType.ARRAY) {
                return variant.arraySize();
            }
            return -1;
        }

        @Override
        public Object arrayGet(Object node, int index) {
            Variant element = ((Variant) node).arrayGet(index);
            return element.isNull() ? null : element;
        }

        @Override
        public Object materialise(Object node) {
            return ((Variant) node).toJavaObject();
        }
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

    private Variant decode(BytesRef value, VariantMetadata metadata) {
        return new Variant(metadata, value.bytes, value.offset, value.offset, value.offset + value.length);
    }
}
