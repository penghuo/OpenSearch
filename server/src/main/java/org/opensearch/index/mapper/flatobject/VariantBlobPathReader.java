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
import org.opensearch.common.variant.VariantFormatException;
import org.opensearch.common.variant.VariantMetadata;
import org.opensearch.common.variant.VariantType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.mapper.FlatObjectFieldMapper.BLOB_META_SUFFIX;

/**
 * Resolves one dotted path inside a document's Variant blob without ever reading a key name back.
 *
 * <p><b>Why not by name.</b> {@link Variant#objectGet(String)} compares the <em>name</em> each candidate field id resolves
 * to, which means every name in the segment has to be available to the reader. For a field whose vocabulary runs to
 * hundreds of thousands of names that is tens of megabytes, materialised per reader and accounted by nothing.
 *
 * <p><b>What replaces it.</b> The write path guarantees that field id {@code i} is the document's {@code i}-th smallest key
 * name, and the name column hands a reader that document's ordinals ascending -- the same order. So a name becomes a field
 * id in two steps that touch no names at all:
 *
 * <pre>
 * once per segment    ord = names.lookupTerm(candidate)      one term-dictionary seek per candidate
 * once per document   fieldId = binarySearch(this document's ordinals, ord)
 * per container       objectGetByFieldId(fieldId)            integer compares
 * </pre>
 *
 * <p>Two things about that which are easy to get wrong. A name present in the document's ordinals only means the document
 * uses it <em>somewhere</em> -- the Variant dictionary is one per document, shared across every nesting level -- so the
 * per-container probe is still required and cannot be skipped. And the choice among candidate prefixes is decided per
 * document by probing live containers, not once per segment: two documents in one segment can resolve the same path along
 * different splits. Only the candidate <em>set</em> is precomputable, because it is purely syntactic.
 *
 * <p>Not thread safe, and holds live doc-values cursors. One instance serves one pass over one segment.
 *
 * @opensearch.internal
 */
final class VariantBlobPathReader {

    private final String path;
    private final BinaryDocValues blob;
    private final SortedSetDocValues names;
    /**
     * Segment ordinal of every candidate key name this path could probe. Absent names are simply not present, so a lookup
     * miss is the same answer as "no document has this key".
     */
    private final Map<String, Integer> candidateOrds;
    /**
     * Refuses to resolve a name, because nothing on this path should ever need one.
     *
     * <p>A loud failure rather than a slow success: if a name is ever wanted here it means a container was handed to
     * something that tried to reconstruct it, and silently paying for a name lookup per key would hide that.
     */
    private static final VariantMetadata.NameResolver FAIL_ON_NAME = ordinal -> {
        throw new VariantFormatException("this reader resolves paths by field id and does not read key names");
    };

    /** This document's ordinals, ascending. Reused across documents, so only the first {@link #ordinalCount} are live. */
    private int[] documentOrdinals = new int[16];
    private int ordinalCount;

    private VariantBlobPathReader(String path, BinaryDocValues blob, SortedSetDocValues names, Map<String, Integer> candidateOrds) {
        this.path = path;
        this.blob = blob;
        this.names = names;
        this.candidateOrds = candidateOrds;
    }

    /**
     * Opens a reader over one segment, seeking each candidate name once.
     *
     * @return a reader, or {@code null} when no document in this segment can hold the path at all
     */
    static VariantBlobPathReader open(LeafReader reader, String blobField, String namesField, String parentField, String path)
        throws IOException {
        requireColumn(reader, blobField, parentField);
        Map<String, Integer> ords = resolveCandidates(reader, namesField, path);
        if (ords == null) {
            return null;
        }
        BinaryDocValues blob = DocValues.getBinary(reader, blobField);
        SortedSetDocValues names = DocValues.getSortedSet(reader, namesField);
        return new VariantBlobPathReader(path, blob, names, ords);
    }

    /**
     * Refuses a segment whose documents have the field but no column to read it from.
     *
     * <p>The index-creation version says whether the column <em>should</em> exist, and it is only a proxy for whether it
     * does. Where the two disagree the failure is silent: Lucene answers an absent doc-values field with an empty iterator
     * rather than an error, so an aggregation returns a confident zero over documents that do have values.
     *
     * <p>The two cases are separable. If the field's own terms are in this segment then its documents do have the field, so
     * a missing column is a broken index rather than an absent value. If neither is present, no document in the segment has
     * the field at all and empty is the right answer.
     */
    private static void requireColumn(LeafReader reader, String blobField, String parentField) {
        // A segment carrying the superseded metadata column was written by a layout whose field ids are not in name order,
        // so reading it here would return values under the wrong keys and raise nothing -- the worst failure available.
        if (reader.getFieldInfos().fieldInfo(parentField + BLOB_META_SUFFIX) != null) {
            throw new IllegalStateException(
                "segment carries ["
                    + parentField
                    + BLOB_META_SUFFIX
                    + "], which belongs to a layout this reader no longer supports. Reindex the field."
            );
        }
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
     * Seeks the ordinal of every name the path could probe.
     *
     * <p>Returns {@code null} when every <em>prefix</em> candidate is absent from the segment. Resolution's first probe at
     * the root is always a prefix span, so if none of those exists no document can match and the whole segment is skipped
     * without touching a document. Only prefixes qualify for that test: a suffix span such as {@code value} may well be a
     * key somewhere else in the corpus and proves nothing about this path.
     */
    private static Map<String, Integer> resolveCandidates(LeafReader reader, String namesField, String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return null;
        }
        // A separate instance from the one iterated per document: seeking and iterating one cursor is not a mixture to rely
        // on.
        SortedSetDocValues seeker = DocValues.getSortedSet(reader, namesField);
        Map<String, Integer> ords = new HashMap<>();
        boolean anyPrefix = false;
        for (String candidate : candidateSpans(path)) {
            long ord = seeker.lookupTerm(new BytesRef(candidate));
            if (ord >= 0) {
                ords.put(candidate, (int) ord);
                if (path.startsWith(candidate) && (candidate.length() == path.length() || path.charAt(candidate.length()) == '.')) {
                    anyPrefix = true;
                }
            }
        }
        return anyPrefix ? Collections.unmodifiableMap(ords) : null;
    }

    /**
     * Every key name the longest-prefix rule could probe while resolving {@code path}: each dot-delimited span that starts
     * at the path start or just after a dot. For {@code a.b.c} that is {@code a.b.c, a.b, a, b.c, b, c} -- n(n+1)/2 spans
     * for n segments, which is a handful for any real path.
     */
    static List<String> candidateSpans(String path) {
        int length = path.length();
        List<String> spans = new ArrayList<>();
        int start = 0;
        while (true) {
            for (int end = start + 1; end <= length; end++) {
                if (end == length || path.charAt(end) == '.') {
                    spans.add(path.substring(start, end));
                }
            }
            int dot = path.indexOf('.', start);
            if (dot < 0) {
                return spans;
            }
            start = dot + 1;
        }
    }

    /**
     * Positions on a document and appends every value the path resolves to.
     *
     * <p>More than one is possible because an array on the way to the path is descended into: {@code a.b} over
     * {@code {"a": [{"b": 1}, {"b": 2}]}} is two values, which is also what the field's terms hold for it.
     *
     * @param out receives the values found, in document order; left untouched if there are none
     */
    void advance(int docId, List<Variant> out) throws IOException {
        if (blob.advanceExact(docId) == false) {
            return;
        }
        ordinalCount = names.advanceExact(docId) ? names.docValueCount() : 0;
        if (documentOrdinals.length < ordinalCount) {
            documentOrdinals = new int[ArrayUtil.oversize(ordinalCount, Integer.BYTES)];
        }
        for (int i = 0; i < ordinalCount; i++) {
            documentOrdinals[i] = (int) names.nextOrd();
        }
        if (ordinalCount == 0) {
            return;
        }
        BytesRef bytes = blob.binaryValue();
        Variant root = new Variant(
            new VariantMetadata(FAIL_ON_NAME, documentOrdinals, ordinalCount),
            bytes.bytes,
            bytes.offset,
            bytes.offset,
            bytes.offset + bytes.length
        );
        resolve(root, path, out);
    }

    /**
     * Mirrors {@link PathResolver#resolve} -- longest matching prefix at each level, no backtracking -- over field ids,
     * with one addition: an array reached before the path is exhausted is descended into rather than being a dead end.
     *
     * <p>That addition is what makes the reader agree with the field's terms. {@code flat_object} flattens an array of
     * objects, so {@code {"a": [{"b": 1}]}} has a term for {@code a.b}; without descending, this would answer that the
     * document has no {@code a.b} while a search for it matches. The same rule is what lets the whole value be an array,
     * which the terms have always accepted.
     */
    private void resolve(Variant node, String remaining, List<Variant> out) {
        if (node.type() == VariantType.ARRAY) {
            int size = node.arraySize();
            for (int i = 0; i < size; i++) {
                resolve(node.arrayGet(i), remaining, out);
            }
            return;
        }
        Variant whole = child(node, remaining);
        if (whole != null) {
            // The path is exhausted, so an array here is the value, not something to descend into. Flattening it is the
            // caller's business.
            out.add(whole);
            return;
        }
        for (int dot = remaining.lastIndexOf('.'); dot > 0; dot = remaining.lastIndexOf('.', dot - 1)) {
            Variant candidate = child(node, remaining.substring(0, dot));
            if (candidate != null) {
                if (candidate.type() != VariantType.NULL) {
                    // A prefix that holds null has nothing to descend into; anything else does.
                    resolve(candidate, remaining.substring(dot + 1), out);
                }
                return;
            }
        }
    }

    /** @return the child at {@code name}, or {@code null} if this container has no such key */
    private Variant child(Variant node, String name) {
        if (node.type() != VariantType.OBJECT) {
            return null;
        }
        Integer ordinal = candidateOrds.get(name);
        if (ordinal == null) {
            // Not a name anywhere in this segment, so no container can hold it.
            return null;
        }
        int fieldId = Arrays.binarySearch(documentOrdinals, 0, ordinalCount, ordinal);
        if (fieldId < 0) {
            // Not a name in this document. The bounded form matters: the buffer is deliberately oversized, so searching it
            // whole would read stale ordinals from a previous document.
            return null;
        }
        return node.objectGetByFieldId(fieldId);
    }
}
