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
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.mapper.FlatObjectFieldMapper.BLOB_META_SUFFIX;

/**
 * Reads a whole {@code flat_object} value out of its Variant blob as a {@link Map}, for {@code doc['attributes']}.
 *
 * <p><b>Why the map is lazy.</b> Reconstructing the object eagerly means turning every field id back into a name, and a
 * name lives in the segment's name column. Both ways of getting them are worse than reading {@code _source}: materialising
 * the whole segment's names is I13 again (tens of megabytes, unaccounted), and looking each one up per document is the
 * scattered access pattern -- roughly sixty times the cost of reading the same names in order, which at a hundred keys a
 * document lands the script route well behind the {@code _source} it is supposed to replace.
 *
 * <p>The way out is that a script asking {@code doc['attributes'].value['status']} does not want the object, it wants one
 * value. So this hands back a view rather than a copy:
 *
 * <ul>
 *   <li>{@code get}, {@code containsKey} and {@code size} resolve through field ids and never read a name.
 *   <li>Only enumeration -- {@code entrySet}, {@code keySet}, {@code values}, iteration -- pays for names, and only for
 *       the keys of the one document being looked at.
 * </ul>
 *
 * <p>A view is valid while the reader is positioned on its document, which is how {@code ScriptDocValues} is used: the
 * value is read before the next document is advanced to.
 *
 * <p>Not thread safe, and holds live doc-values cursors. One instance serves one pass over one segment.
 *
 * @opensearch.internal
 */
final class VariantBlobObjectReader {

    private final BinaryDocValues blob;
    /** Iterated per document for this document's ordinals. */
    private final SortedSetDocValues names;
    /** Seeked, never iterated, so it cannot interfere with the cursor above. */
    private final SortedSetDocValues seeker;

    /**
     * Name to segment ordinal, filled as scripts ask for keys.
     *
     * <p>One instance serves a whole segment, so a script scanning it looks each key up once however many documents it
     * visits. {@code Integer.MIN_VALUE} records "no such name in this segment", which is worth caching too.
     */
    private final Map<String, Integer> ordByName = new HashMap<>();
    /** Resolves a field id's name, used only when a script enumerates. */
    private final VariantMetadata.NameResolver nameResolver;

    private int[] documentOrdinals = new int[16];
    private int ordinalCount;
    /**
     * Bumped on every advance, and stamped into each view.
     *
     * <p>A view reads the reader's live cursor state -- this document's ordinals, and bytes owned by the doc-values
     * iterator -- so it is only meaningful while the reader sits on its document. That is how {@code ScriptDocValues} is
     * used, but if a caller ever holds one across a document boundary it would silently read a different document's value.
     * The stamp turns that into an error.
     */
    private int generation;

    private VariantBlobObjectReader(BinaryDocValues blob, SortedSetDocValues names, SortedSetDocValues seeker) {
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
        // The same two refusals the per-path reader makes, for the same reasons: a superseded layout would return values
        // under the wrong keys, and a missing column where the field's terms exist is a broken index rather than an absent
        // value.
        if (reader.getFieldInfos().fieldInfo(parentField + BLOB_META_SUFFIX) != null) {
            throw new IllegalStateException(
                "segment carries ["
                    + parentField
                    + BLOB_META_SUFFIX
                    + "], which belongs to a layout this reader no longer supports. Reindex the field."
            );
        }
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
            DocValues.getBinary(reader, blobField),
            DocValues.getSortedSet(reader, namesField),
            DocValues.getSortedSet(reader, namesField)
        );
    }

    /**
     * @return this document's value as a lazy map, or {@code null} if it has none
     */
    Map<String, Object> advance(int docId) throws IOException {
        generation++;
        if (blob.advanceExact(docId) == false) {
            return null;
        }
        ordinalCount = names.advanceExact(docId) ? names.docValueCount() : 0;
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
        if (root.type() != VariantType.OBJECT) {
            return null;
        }
        return new ObjectView(root);
    }

    /** @return the field id {@code name} has in the current document, or -1 */
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
        int position = Arrays.binarySearch(documentOrdinals, 0, ordinalCount, cached);
        return position < 0 ? -1 : position;
    }

    /** Containers stay lazy so that reaching one key never reconstructs its siblings. */
    private Object materialise(Variant value) {
        switch (value.type()) {
            case OBJECT:
                return new ObjectView(value);
            case ARRAY:
                int size = value.arraySize();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(materialise(value.arrayGet(i)));
                }
                return list;
            default:
                return value.toJavaObject();
        }
    }

    /**
     * A map over one Variant object, resolving a key by field id and only reading names when enumerated.
     *
     * <p>Read-only: a script mutating what it read from a document would be meaningless, and refusing is clearer than
     * silently discarding.
     */
    private final class ObjectView extends AbstractMap<String, Object> {

        private final Variant node;
        private final int bornAt;

        ObjectView(Variant node) {
            this.node = node;
            this.bornAt = generation;
        }

        private void requireCurrent() {
            if (bornAt != generation) {
                throw new IllegalStateException(
                    "this value belongs to a document the reader has already moved past, so it can no longer be read"
                );
            }
        }

        @Override
        public int size() {
            requireCurrent();
            return node.objectSize();
        }

        @Override
        public boolean containsKey(Object key) {
            requireCurrent();
            if (key instanceof String name) {
                int fieldId = fieldIdOf(name);
                return fieldId >= 0 && node.objectGetByFieldId(fieldId) != null;
            }
            return false;
        }

        @Override
        public Object get(Object key) {
            requireCurrent();
            if (key instanceof String name) {
                int fieldId = fieldIdOf(name);
                if (fieldId >= 0) {
                    Variant value = node.objectGetByFieldId(fieldId);
                    if (value != null) {
                        return materialise(value);
                    }
                }
            }
            return null;
        }

        /**
         * The one operation that needs key names, so it is the only one that pays for them -- and only for this document's
         * keys, never the segment's.
         */
        @Override
        public Set<Entry<String, Object>> entrySet() {
            requireCurrent();
            return new AbstractSet<>() {
                @Override
                public int size() {
                    return node.objectSize();
                }

                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private int at;

                        @Override
                        public boolean hasNext() {
                            return at < node.objectSize();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            int i = at++;
                            return new SimpleImmutableEntry<>(node.objectKeyAt(i), materialise(node.objectValueAt(i)));
                        }
                    };
                }
            };
        }
    }

    /** The document's scalar values as sorted UTF-8, so the parent field still has a bytes view for anything that asks. */
    List<BytesRef> valuesOf(Map<String, Object> view) {
        List<BytesRef> out = new ArrayList<>();
        collectValues(view, out);
        out.sort(BytesRef::compareTo);
        return out;
    }

    private static void collectValues(Object value, List<BytesRef> out) {
        if (value instanceof Map<?, ?> map) {
            for (Object child : map.values()) {
                collectValues(child, out);
            }
        } else if (value instanceof List<?> list) {
            for (Object child : list) {
                collectValues(child, out);
            }
        } else if (value != null) {
            out.add(new BytesRef(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        }
    }
}
