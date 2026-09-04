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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.variant.Variant;
import org.opensearch.common.variant.VariantMetadata;
import org.opensearch.common.variant.VariantType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves one compiled path inside a document's Variant blob, materialising only the value it selects.
 *
 * <p>Path keys are resolved to segment ordinals once, then binary-searched in each document's sorted ordinal list to obtain
 * the document-local field id.
 *
 * <p>Not thread safe, and holds live doc-values cursors. One instance serves one pass over one segment.
 *
 * @opensearch.internal
 */
final class VariantBlobPathReader implements PathResolver.Navigator {

    private BinaryDocValues blob;
    private SortedSetDocValues names;
    private final SortedSetDocValues seeker;

    private final Map<String, Integer> ordByName = new HashMap<>();
    private final Map<Integer, byte[]> nameByOrd = new HashMap<>();
    private final VariantMetadata.NameResolver nameResolver;

    private int[] documentOrdinals = new int[16];
    private int ordinalCount;
    private int iteratorDoc = -1;
    private int nameIteratorDoc = -1;

    private final LeafReader reader;
    private final String blobField;
    private final String namesField;

    private VariantBlobPathReader(LeafReader reader, String blobField, String namesField) throws IOException {
        this.reader = reader;
        this.blobField = blobField;
        this.namesField = namesField;
        this.blob = DocValues.getBinary(reader, blobField);
        this.names = DocValues.getSortedSet(reader, namesField);
        this.seeker = DocValues.getSortedSet(reader, namesField);
        this.nameResolver = ordinal -> nameByOrd.computeIfAbsent(ordinal, ord -> {
            try {
                BytesRef term = seeker.lookupOrd(ord);
                byte[] copy = new byte[term.length];
                System.arraycopy(term.bytes, term.offset, copy, 0, term.length);
                return copy;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Opens a reader over one segment.
     *
     * @return a reader, or {@code null} when this segment has no such column and no document in it has the field
     */
    static VariantBlobPathReader open(LeafReader reader, String blobField, String namesField, String parentField) throws IOException {
        requireColumn(reader, blobField, parentField);
        if (reader.getFieldInfos().fieldInfo(blobField) == null) {
            return null;
        }
        return new VariantBlobPathReader(reader, blobField, namesField);
    }

    /**
     * Refuses a segment whose documents have the field but no column to read it from.
     *
     * <p>The index's opt-in setting, and the version guard on it, say whether the column <em>should</em> exist; they are only
     * a proxy for whether it does. Metadata can say enabled for a shard whose segments were written without it, through a
     * restore or a mixed upgrade. Where the two disagree the failure is silent: Lucene answers an absent doc-values field
     * with an empty iterator rather than an error, so a read returns a confident "no value" over documents that do have one.
     *
     * <p>The two cases are separable. If the field's own terms are in this segment then its documents do have the field, so
     * a missing column is a broken index rather than an absent value. If neither is present, no document in the segment has
     * the field at all and empty is the right answer.
     */
    private static void requireColumn(LeafReader reader, String blobField, String parentField) {
        if (reader.getFieldInfos().fieldInfo(blobField) != null) {
            return;
        }
        if (reader.getFieldInfos().fieldInfo(parentField) != null) {
            throw new IllegalStateException(
                "["
                    + parentField
                    + "] has documents in this segment but no ["
                    + blobField
                    + "] column to read them from, so a value cannot be returned. Reindex the field."
            );
        }
    }

    /**
     * Positions on a document and resolves {@code path} against its value.
     *
     * @return the materialised value, {@code null} if the path holds JSON null, a {@code List} if the path is wildcarded,
     *         or {@link PathResolver#MISSING} if the document has no value at that path
     */
    Object resolve(int docId, FlatObjectPath path) throws IOException {
        Variant root = positionAt(docId);
        if (root == null) {
            return PathResolver.MISSING;
        }
        return PathResolver.resolve(root, path, this);
    }

    /** @return this document's decoded value, or {@code null} if it has none */
    private Variant positionAt(int docId) throws IOException {
        // BinaryDocValues is a forward-only iterator while a script may address documents out of order. Without this
        // restart a backwards read would return no value and raise nothing, which reads as "this document has none".
        if (docId < iteratorDoc) {
            restart();
        }
        if (blob.advanceExact(docId) == false) {
            iteratorDoc = blob.docID();
            return null;
        }
        iteratorDoc = blob.docID();
        readOrdinals(docId);
        BytesRef bytes = blob.binaryValue();
        return new Variant(
            new VariantMetadata(nameResolver, documentOrdinals, ordinalCount),
            bytes.bytes,
            bytes.offset,
            bytes.offset,
            bytes.offset + bytes.length
        );
    }

    private void restart() throws IOException {
        this.blob = DocValues.getBinary(reader, blobField);
        this.names = DocValues.getSortedSet(reader, namesField);
        this.iteratorDoc = -1;
        this.nameIteratorDoc = -1;
    }

    private void readOrdinals(int docId) throws IOException {
        // A document with no keys at all writes nothing to the name column, so its absence means zero keys rather than a
        // missing value. Requiring it would report an empty object as no object.
        ordinalCount = names.advanceExact(docId) ? names.docValueCount() : 0;
        nameIteratorDoc = names.docID();
        if (documentOrdinals.length < ordinalCount) {
            documentOrdinals = new int[ArrayUtil.oversize(ordinalCount, Integer.BYTES)];
        }
        for (int i = 0; i < ordinalCount; i++) {
            documentOrdinals[i] = (int) names.nextOrd();
        }
    }

    @Override
    public Object child(Object node, String key) {
        if (node instanceof Variant variant && variant.type() == VariantType.OBJECT) {
            int fieldId = fieldIdOf(key);
            if (fieldId >= 0) {
                Variant value = variant.objectGetByFieldId(fieldId);
                if (value != null) {
                    return value.isNull() ? null : value;
                }
            }
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

    private int fieldIdOf(String name) {
        Integer cached = ordByName.get(name);
        if (cached == null) {
            long ord;
            try {
                ord = seeker.lookupTerm(new BytesRef(name));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            cached = ord < 0 ? Integer.MIN_VALUE : (int) ord;
            ordByName.put(name, cached);
        }
        if (cached == Integer.MIN_VALUE) {
            return -1;
        }
        // The bounded form matters: the buffer is deliberately oversized, so searching it whole would read stale ordinals
        // from a previous document.
        int position = Arrays.binarySearch(documentOrdinals, 0, ordinalCount, cached);
        return position < 0 ? -1 : position;
    }
}
