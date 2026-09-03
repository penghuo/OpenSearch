/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The parsed key dictionary of a Variant value.
 *
 * <p>Held separately from {@link Variant} so that the header is parsed and validated <b>once per document</b> rather than
 * once per subtree access. Navigating into a subtree is supposed to cost a binary search and an offset computation; if
 * every step re-read the metadata header the blob arm would lose the very property it is being measured for.
 *
 * @opensearch.internal
 */
public final class VariantMetadata {

    private final byte[] bytes;
    private final int dictionarySize;
    private final int offsetSize;
    private final int offsetsStart;
    private final int bytesStart;
    private final boolean sortedStrings;
    /** First byte of this metadata within {@link #bytes}, so a shared buffer can be read without copying. */
    private final int base;
    /** Exclusive end of this metadata within {@link #bytes}. */
    private final int limit;

    // ---- indirect form: the names live outside the value, and this only records which of them each field id means ----

    /**
     * Every key name in the segment, indexed by the ordinal a name column assigned it, or {@code null} when this metadata
     * carries its own names.
     *
     * <p>Shared across every document in the segment, so it is read once rather than per document. Held as raw UTF-8 so a
     * key comparison allocates nothing, matching what the inline form achieves by comparing bytes in place.
     */
    private final byte[][] nameTable;
    /** This document's ordinals into {@link #nameTable}, ascending, which is name order. */
    private final int[] documentOrdinals;
    /**
     * Position in {@link #documentOrdinals} that each field id refers to; {@code ranks[fieldId]} is that position. Null
     * when the writer already assigned field ids in name order, so the position <em>is</em> the field id.
     */
    private final byte[] ranks;
    private final int ranksOffset;
    private final int rankWidth;
    private final int indirectSize;

    public VariantMetadata(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    /**
     * Builds metadata whose names live in a separate per-segment column and whose field ids are already in name order.
     *
     * <p>The cheapest of the three forms: a field id is a position in {@code documentOrdinals} directly, so resolving a
     * name is two array reads with nothing to parse and nothing stored per document beyond the ordinals themselves. The
     * writer earns this by relabelling field ids once at index time; see {@link Variant#relabelFieldIds}.
     *
     * @param nameTable        every name in the segment, by ordinal
     * @param documentOrdinals this document's ordinals, ascending -- which is name order, so ordinal {@code i} is the
     *                         name that field id {@code i} means
     * @param size             number of field ids
     */
    public VariantMetadata(byte[][] nameTable, int[] documentOrdinals, int size) {
        this(nameTable, documentOrdinals, null, 0, 0, size);
    }

    /**
     * Builds metadata whose names live in a separate per-segment column, with a rank list saying which name each field id
     * means.
     *
     * <p>Field ids cannot refer to segment ordinals directly, because a mapper assigns them while parsing a document and
     * the ordinals are not decided until the segment is flushed. They therefore refer to a position within this document's
     * own ordinal list, which the writer records as a rank.
     *
     * <p>Needed only where the writer could not relabel the ids into name order -- a document with more distinct keys than
     * the field-id width it already wrote can address after permutation. Prefer
     * {@link #VariantMetadata(byte[][], int[], int)}.
     *
     * @param nameTable        every name in the segment, by ordinal
     * @param documentOrdinals this document's ordinals, ascending
     * @param ranks            buffer holding {@code rank[fieldId]}, laid out by {@link #rankWidth} bytes each, or
     *                         {@code null} when field ids are already in name order
     * @param ranksOffset      first rank byte within {@code ranks}
     * @param rankWidth        bytes per rank, 1 or 2
     * @param size             number of field ids
     */
    public VariantMetadata(byte[][] nameTable, int[] documentOrdinals, byte[] ranks, int ranksOffset, int rankWidth, int size) {
        this.nameTable = nameTable;
        this.documentOrdinals = documentOrdinals;
        this.ranks = ranks;
        this.ranksOffset = ranksOffset;
        this.rankWidth = rankWidth;
        this.indirectSize = size;
        // The inline fields are unused in this form.
        this.bytes = null;
        this.base = 0;
        this.limit = 0;
        this.dictionarySize = 0;
        this.offsetSize = 0;
        this.offsetsStart = 0;
        this.bytesStart = 0;
        this.sortedStrings = false;
    }

    /** Resolves a field id to its name's raw UTF-8, without decoding a {@code String}. */
    private byte[] indirectName(int fieldId) {
        if (fieldId < 0 || fieldId >= indirectSize) {
            throw new VariantFormatException("field id " + fieldId + " out of range for " + indirectSize + " keys");
        }
        if (ranks == null) {
            return nameTable[documentOrdinals[fieldId]];
        }
        int rank = VariantEncoding.readUnsigned(ranks, ranksOffset + fieldId * rankWidth, rankWidth);
        if (rank >= indirectSize) {
            throw new VariantFormatException("field id " + fieldId + " ranks at " + rank + ", beyond " + indirectSize + " keys");
        }
        return nameTable[documentOrdinals[rank]];
    }

    /**
     * Reads metadata in place from a region of a shared buffer.
     *
     * <p>The offset form exists so a doc-values {@code BytesRef} can be decoded without copying: the blob arrives as
     * (array, offset, length) and copying it out per document was measurable allocation on the read path.
     */
    public VariantMetadata(byte[] bytes, int offset, int length) {
        this.nameTable = null;
        this.documentOrdinals = null;
        this.ranks = null;
        this.ranksOffset = 0;
        this.rankWidth = 0;
        this.indirectSize = 0;
        this.bytes = bytes;
        this.base = offset;
        this.limit = offset + length;
        if (offset < 0 || length < 0 || limit > bytes.length) {
            throw new VariantFormatException("metadata region [" + offset + ", " + limit + ") outside buffer of " + bytes.length);
        }
        if (length < 1) {
            throw new VariantFormatException("metadata is empty");
        }
        int header = bytes[base] & 0xFF;
        int version = header & VariantEncoding.VERSION_MASK;
        if (version != VariantEncoding.VERSION) {
            throw new VariantFormatException("unsupported Variant metadata version " + version);
        }
        this.sortedStrings = ((header >>> VariantEncoding.SORTED_STRINGS_SHIFT) & 0x01) != 0;
        this.offsetSize = ((header >>> VariantEncoding.OFFSET_SIZE_SHIFT) & 0x03) + 1;
        if (length < 1 + offsetSize) {
            throw new VariantFormatException("metadata truncated before dictionary size");
        }
        this.dictionarySize = VariantEncoding.readUnsigned(bytes, base + 1, offsetSize);
        if (dictionarySize < 0) {
            throw new VariantFormatException("negative dictionary size " + dictionarySize);
        }
        this.offsetsStart = base + 1 + offsetSize;
        long computedBytesStart = (long) offsetsStart + ((long) dictionarySize + 1) * offsetSize;
        if (computedBytesStart > limit) {
            throw new VariantFormatException("metadata truncated before dictionary bytes");
        }
        this.bytesStart = (int) computedBytesStart;
    }

    public int size() {
        return nameTable == null ? dictionarySize : indirectSize;
    }

    public boolean sortedStrings() {
        return sortedStrings;
    }

    public byte[] bytes() {
        // Callers that persist the metadata need exactly this region, not the whole shared buffer.
        return base == 0 && limit == bytes.length ? bytes : Arrays.copyOfRange(bytes, base, limit);
    }

    /** Length of this metadata region, for callers writing it out without a copy. */
    public int length() {
        return limit - base;
    }

    public byte[] array() {
        return bytes;
    }

    public int offset() {
        return base;
    }

    public String key(int fieldId) {
        if (nameTable != null) {
            return new String(indirectName(fieldId), StandardCharsets.UTF_8);
        }
        long range = keyRange(fieldId);
        int start = (int) (range >>> 32);
        int end = (int) range;
        return new String(bytes, start, end - start, StandardCharsets.UTF_8);
    }

    /**
     * Compares the dictionary entry against a UTF-8 probe, without decoding the entry to a {@code String}.
     *
     * <p>Called once per binary-search probe, so it allocates nothing.
     */
    public int compareKey(int fieldId, byte[] probe) {
        if (nameTable != null) {
            byte[] name = indirectName(fieldId);
            return Arrays.compareUnsigned(name, 0, name.length, probe, 0, probe.length);
        }
        long range = keyRange(fieldId);
        return Arrays.compareUnsigned(bytes, (int) (range >>> 32), (int) range, probe, 0, probe.length);
    }

    /**
     * Resolves and fully validates one dictionary entry's byte range, returned packed as {@code (start << 32) | end}.
     *
     * <p>All three conditions have to be checked together. Corrupt metadata can leave the offsets out of order, which
     * yields a negative length, and a negative length reaches {@code String} and {@code Arrays} as an unchecked
     * out-of-bounds failure rather than as a format error. Validating the end alone is not enough.
     */
    private long keyRange(int fieldId) {
        if (fieldId < 0 || fieldId >= dictionarySize) {
            throw new VariantFormatException("field id " + fieldId + " out of range for dictionary of " + dictionarySize);
        }
        long start = bytesStart + (VariantEncoding.readUnsigned(bytes, offsetsStart + fieldId * offsetSize, offsetSize) & 0xFFFFFFFFL);
        long end = bytesStart + (VariantEncoding.readUnsigned(bytes, offsetsStart + (fieldId + 1) * offsetSize, offsetSize) & 0xFFFFFFFFL);
        if (start < bytesStart || end < start || end > bytes.length) {
            throw new VariantFormatException(
                "dictionary entry " + fieldId + " has an invalid range [" + start + ", " + end + ") in a region ending at " + limit
            );
        }
        return (start << 32) | end;
    }
}
