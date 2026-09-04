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
import java.util.Collections;
import java.util.Map;

/**
 * Reads a complete {@code flat_object} value from its Variant column.
 *
 * <p>The result is fully materialised before the doc-values iterators advance because script values may outlive the reader
 * position that produced them.
 *
 * <p>Not thread safe, and holds live doc-values cursors. One instance serves one pass over one segment.
 *
 * @opensearch.internal
 */
final class VariantBlobObjectReader {

    private BinaryDocValues blob;
    private SortedSetDocValues names;
    private final SortedSetDocValues seeker;

    private final LeafReader reader;
    private final String blobField;
    private final String namesField;
    private int iteratorDoc = -1;

    private final VariantMetadata.NameResolver nameResolver;

    private int[] documentOrdinals = new int[16];

    private VariantBlobObjectReader(
        LeafReader reader,
        String blobField,
        String namesField,
        BinaryDocValues blob,
        SortedSetDocValues names,
        SortedSetDocValues seeker
    ) {
        this.reader = reader;
        this.blobField = blobField;
        this.namesField = namesField;
        this.blob = blob;
        this.names = names;
        this.seeker = seeker;
        this.nameResolver = ordinal -> {
            try {
                BytesRef term = seeker.lookupOrd(ordinal);
                byte[] copy = new byte[term.length];
                System.arraycopy(term.bytes, term.offset, copy, 0, term.length);
                return copy;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    static VariantBlobObjectReader open(LeafReader reader, String blobField, String namesField, String parentField) throws IOException {
        if (reader.getFieldInfos().fieldInfo(blobField) == null && reader.getFieldInfos().fieldInfo(parentField) != null) {
            throw new IllegalStateException(
                "["
                    + parentField
                    + "] has documents in this segment but no ["
                    + blobField
                    + "] column to read them from, so a value cannot be returned. Reindex the field."
            );
        }
        return new VariantBlobObjectReader(
            reader,
            blobField,
            namesField,
            DocValues.getBinary(reader, blobField),
            DocValues.getSortedSet(reader, namesField),
            DocValues.getSortedSet(reader, namesField)
        );
    }

    /**
     * @return this document's value as a plain Java map that depends on nothing here, or {@code null} if it has none
     */
    Map<String, Object> advance(int docId) throws IOException {
        // Both cursors are forward-only while the fetch phase addresses hits out of doc-id order. Without this restart a
        // backwards read would return no value and raise nothing, which reads as "this document has none".
        if (docId < iteratorDoc) {
            this.blob = DocValues.getBinary(reader, blobField);
            this.names = DocValues.getSortedSet(reader, namesField);
            this.iteratorDoc = -1;
        }
        if (blob.advanceExact(docId) == false) {
            iteratorDoc = blob.docID();
            return null;
        }
        iteratorDoc = blob.docID();

        // A document with no keys at all writes nothing to the name column, so its absence means zero keys rather than a
        // missing value. Requiring it would report an empty object as no object.
        int ordinalCount = names.advanceExact(docId) ? names.docValueCount() : 0;
        if (documentOrdinals.length < ordinalCount) {
            documentOrdinals = new int[ArrayUtil.oversize(ordinalCount, Integer.BYTES)];
        }
        for (int i = 0; i < ordinalCount; i++) {
            documentOrdinals[i] = (int) names.nextOrd();
        }
        BytesRef bytes = blob.binaryValue();
        Variant root = new Variant(
            new VariantMetadata(nameResolver, documentOrdinals, ordinalCount),
            bytes.bytes,
            bytes.offset,
            bytes.offset,
            bytes.offset + bytes.length
        );

        return root.type() == VariantType.OBJECT ? asMap(root) : null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Variant object) {
        return Collections.unmodifiableMap((Map<String, Object>) object.toJavaObject());
    }

}
