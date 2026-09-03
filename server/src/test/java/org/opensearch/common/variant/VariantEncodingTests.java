/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HexFormat;

/**
 * Byte-for-byte tests against expected encodings derived by hand from the Variant specification.
 *
 * <p>These matter more than they look. A round-trip test passes just as happily against a wrong-but-self-consistent bit
 * layout — swap the object and array {@code is_large} positions, say, and every encode/decode pair still agrees while the
 * output is unreadable by Spark or parquet-java. Only fixed expected bytes catch that.
 *
 * <p>Every expectation below is annotated with the derivation so it can be checked against the spec without rerunning the
 * code.
 */
public class VariantEncodingTests extends OpenSearchTestCase {

    /** Metadata for a value with no object keys: version 1, 1-byte offsets, zero entries, one terminating offset. */
    private static final String EMPTY_METADATA = "010000";

    private static void assertValue(String expectedValueHex, VariantBuilder builder) {
        Variant variant = builder.finish();
        assertEquals("value bytes", expectedValueHex, HexFormat.of().formatHex(variant.valueBytes()));
    }

    private static void assertMetadata(String expectedMetadataHex, VariantBuilder builder) {
        Variant variant = builder.finish();
        assertEquals("metadata bytes", expectedMetadataHex, HexFormat.of().formatHex(variant.metadataBytes()));
    }

    private static VariantBuilder builder() {
        return new VariantBuilder();
    }

    // ------------------------------------------------------------- metadata

    public void testEmptyMetadata() {
        VariantBuilder builder = builder();
        builder.appendNull();
        // header 0x01 = version 1, sorted_strings 0, offset_size_minus_one 0; then dictionary_size 0; then one offset 0.
        assertMetadata(EMPTY_METADATA, builder);
    }

    public void testMetadataWithOneKey() {
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("a");
        builder.appendLong(1);
        builder.endObject();
        // header 0x01; dictionary_size 1; offsets [0, 1]; bytes "a" = 0x61.
        assertMetadata("010100" + "01" + "61", builder);
    }

    /**
     * Dictionary ids stay in insertion order, which is what lets the builder avoid rewriting field ids at the end.
     */
    public void testMetadataKeepsInsertionOrder() {
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("b");
        builder.appendLong(1);
        builder.appendKey("a");
        builder.appendLong(2);
        builder.endObject();
        // dictionary_size 2; offsets [0, 1, 2]; bytes "ba" — "b" first because it was seen first.
        assertMetadata("01" + "02" + "000102" + "6261", builder);
    }

    public void testSortedStringsFlagIsZero() {
        VariantBuilder builder = builder();
        builder.appendNull();
        Variant variant = builder.finish();
        assertFalse("this implementation does not sort the dictionary", variant.metadata().sortedStrings());
    }

    // ----------------------------------------------------------- primitives

    public void testNull() {
        // basic_type 0, primitive id 0 -> 0x00
        VariantBuilder builder = builder();
        builder.appendNull();
        assertValue("00", builder);
    }

    public void testTrue() {
        // basic_type 0, primitive id 1 -> 1 << 2 = 0x04
        VariantBuilder builder = builder();
        builder.appendBoolean(true);
        assertValue("04", builder);
    }

    public void testFalse() {
        // basic_type 0, primitive id 2 -> 2 << 2 = 0x08
        VariantBuilder builder = builder();
        builder.appendBoolean(false);
        assertValue("08", builder);
    }

    public void testInt8() {
        // primitive id 3 -> 3 << 2 = 0x0c, then one byte
        VariantBuilder builder = builder();
        builder.appendLong(5);
        assertValue("0c05", builder);
    }

    public void testInt8Negative() {
        VariantBuilder builder = builder();
        builder.appendLong(-1);
        assertValue("0cff", builder);
    }

    public void testInt16() {
        // 300 does not fit in a byte; primitive id 4 -> 0x10, then 0x012c little-endian
        VariantBuilder builder = builder();
        builder.appendLong(300);
        assertValue("102c01", builder);
    }

    public void testInt32() {
        // primitive id 5 -> 0x14, then 100000 = 0x000186a0 little-endian
        VariantBuilder builder = builder();
        builder.appendLong(100_000);
        assertValue("14a0860100", builder);
    }

    public void testInt64() {
        // primitive id 6 -> 0x18, then 2^40 little-endian
        VariantBuilder builder = builder();
        builder.appendLong(1L << 40);
        assertValue("180000000000010000", builder);
    }

    public void testDouble() {
        // primitive id 7 -> 0x1c, then the raw bits of 1.5 (0x3ff8000000000000) little-endian
        VariantBuilder builder = builder();
        builder.appendDouble(1.5);
        assertValue("1c000000000000f83f", builder);
    }

    public void testFloat() {
        // primitive id 14 -> 14 << 2 = 0x38, then the raw bits of 1.5f (0x3fc00000) little-endian
        VariantBuilder builder = builder();
        builder.appendFloat(1.5f);
        assertValue("380000c03f", builder);
    }

    public void testBinary() {
        // primitive id 15 -> 0x3c, then a 4-byte little-endian length, then the bytes
        VariantBuilder builder = builder();
        builder.appendBinary(new byte[] { 1, 2, 3 });
        assertValue("3c03000000" + "010203", builder);
    }

    // ---------------------------------------------------------------- strings

    public void testEmptyShortString() {
        // basic_type 1, length 0 -> 1 | (0 << 2) = 0x01
        VariantBuilder builder = builder();
        builder.appendString("");
        assertValue("01", builder);
    }

    public void testOneCharShortString() {
        // 1 | (1 << 2) = 0x05, then 'a'
        VariantBuilder builder = builder();
        builder.appendString("a");
        assertValue("0561", builder);
    }

    public void testMaximumShortString() {
        // 63 bytes is the longest short string: 1 | (63 << 2) = 0xfd
        VariantBuilder builder = builder();
        builder.appendString("a".repeat(63));
        assertValue("fd" + "61".repeat(63), builder);
    }

    public void testStringOverShortLimitBecomesLongString() {
        // 64 bytes crosses to primitive id 16 -> 16 << 2 = 0x40, then a 4-byte length
        VariantBuilder builder = builder();
        builder.appendString("a".repeat(64));
        assertValue("40" + "40000000" + "61".repeat(64), builder);
    }

    public void testMultiByteCharacterCountsBytesNotCharacters() {
        // A 2-byte character repeated 32 times is 64 UTF-8 bytes, so it must use the long form even though it is only
        // 32 characters. Length is defined in bytes.
        VariantBuilder builder = builder();
        builder.appendString("é".repeat(32));
        assertValue("40" + "40000000" + "c3a9".repeat(32), builder);
    }

    // ---------------------------------------------------------------- objects

    public void testEmptyObject() {
        // 0x02 = basic_type 2 with an all-zero header (1-byte ids and offsets, not large);
        // then num_elements 0; then the single terminating offset 0.
        VariantBuilder builder = builder();
        builder.startObject();
        builder.endObject();
        assertValue("020000", builder);
    }

    public void testSingleMemberObject() {
        // header 0x02; num_elements 1; field_id 0; offsets [0, 2]; then the int8 value 1.
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("a");
        builder.appendLong(1);
        builder.endObject();
        assertValue("02" + "01" + "00" + "0002" + "0c01", builder);
    }

    /**
     * Field ids must be ordered by their key strings, not by insertion order, or a reader's binary search cannot work.
     */
    public void testObjectMembersAreOrderedByKey() {
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("b");
        builder.appendLong(1);
        builder.appendKey("a");
        builder.appendLong(2);
        builder.endObject();
        // "a" has dictionary id 1 and sits at data offset 2; "b" has id 0 at offset 0. Sorted by key, the id array is
        // [1, 0] and the offset array is [2, 0, 4].
        assertValue("02" + "02" + "0100" + "020004" + "0c01" + "0c02", builder);
    }

    public void testNestedObject() {
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("outer");
        builder.startObject();
        builder.appendKey("inner");
        builder.appendLong(7);
        builder.endObject();
        builder.endObject();
        Variant variant = builder.finish();
        // Checked structurally rather than byte-wise; the byte-level object layout is already pinned above.
        assertEquals(VariantType.OBJECT, variant.type());
        assertEquals(1, variant.objectSize());
        Variant inner = variant.objectGet("outer");
        assertNotNull(inner);
        assertEquals(VariantType.OBJECT, inner.type());
        assertEquals(7L, inner.objectGet("inner").getLong());
    }

    // ----------------------------------------------------------------- arrays

    public void testEmptyArray() {
        // 0x03 = basic_type 3 with an all-zero header; num_elements 0; terminating offset 0.
        VariantBuilder builder = builder();
        builder.startArray();
        builder.endArray();
        assertValue("030000", builder);
    }

    public void testArrayOfTwo() {
        // header 0x03; num_elements 2; offsets [0, 2, 4]; then two int8 values.
        VariantBuilder builder = builder();
        builder.startArray();
        builder.appendLong(1);
        builder.appendLong(2);
        builder.endArray();
        assertValue("03" + "02" + "000204" + "0c01" + "0c02", builder);
    }

    // ------------------------------------------------------- header bit fields

    /**
     * The bit position of {@code is_large} differs between objects and arrays. Asserting both from the same test keeps the
     * asymmetry visible.
     */
    public void testIsLargeBitPositionsDiffer() {
        VariantBuilder objectBuilder = builder();
        objectBuilder.startObject();
        for (int i = 0; i <= 256; i++) {
            objectBuilder.appendKey("k" + i);
            objectBuilder.appendLong(i);
        }
        objectBuilder.endObject();
        Variant object = objectBuilder.finish();
        int objectHeader = VariantEncoding.valueHeader(object.valueBytes()[0]);
        assertEquals("object is_large lives at value-header bit 4", 1, (objectHeader >>> VariantEncoding.OBJ_IS_LARGE_SHIFT) & 1);
        assertEquals(257, object.objectSize());

        VariantBuilder arrayBuilder = builder();
        arrayBuilder.startArray();
        for (int i = 0; i <= 256; i++) {
            arrayBuilder.appendLong(i);
        }
        arrayBuilder.endArray();
        Variant array = arrayBuilder.finish();
        int arrayHeader = VariantEncoding.valueHeader(array.valueBytes()[0]);
        assertEquals("array is_large lives at value-header bit 2", 1, (arrayHeader >>> VariantEncoding.ARR_IS_LARGE_SHIFT) & 1);
        assertEquals(257, array.arraySize());
    }

    public void testSmallContainersAreNotLarge() {
        VariantBuilder builder = builder();
        builder.startArray();
        for (int i = 0; i < 255; i++) {
            builder.appendLong(1);
        }
        builder.endArray();
        Variant array = builder.finish();
        int header = VariantEncoding.valueHeader(array.valueBytes()[0]);
        assertEquals(0, (header >>> VariantEncoding.ARR_IS_LARGE_SHIFT) & 1);
        assertEquals(255, array.arraySize());
    }

    /**
     * Offsets widen once the data they address no longer fits in a byte.
     */
    public void testOffsetWidthGrowsWithDataSize() {
        VariantBuilder builder = builder();
        builder.startArray();
        // Each element is a 100-byte string, so the array data comfortably exceeds 255 bytes.
        for (int i = 0; i < 5; i++) {
            builder.appendString("x".repeat(100));
        }
        builder.endArray();
        Variant array = builder.finish();
        int header = VariantEncoding.valueHeader(array.valueBytes()[0]);
        int offsetSize = ((header >>> VariantEncoding.ARR_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;
        assertTrue("offsets must widen past one byte, but were " + offsetSize, offsetSize >= 2);
        assertEquals(5, array.arraySize());
        assertEquals("x".repeat(100), array.arrayGet(4).getString());
    }

    public void testMinUnsignedWidth() {
        assertEquals(1, VariantEncoding.minUnsignedWidth(0));
        assertEquals(1, VariantEncoding.minUnsignedWidth(255));
        assertEquals(2, VariantEncoding.minUnsignedWidth(256));
        assertEquals(2, VariantEncoding.minUnsignedWidth(65535));
        assertEquals(3, VariantEncoding.minUnsignedWidth(65536));
        assertEquals(4, VariantEncoding.minUnsignedWidth(1 << 24));
    }

    // ------------------------------------------------------------- validation

    public void testDuplicateKeysAreRejected() {
        VariantBuilder builder = builder();
        builder.startObject();
        builder.appendKey("a");
        builder.appendLong(1);
        builder.appendKey("a");
        builder.appendLong(2);
        IllegalStateException e = expectThrows(IllegalStateException.class, builder::endObject);
        assertTrue(e.getMessage(), e.getMessage().contains("duplicate object key"));
    }

    public void testUnclosedContainerIsRejected() {
        VariantBuilder builder = builder();
        builder.startObject();
        IllegalStateException e = expectThrows(IllegalStateException.class, builder::finish);
        assertTrue(e.getMessage(), e.getMessage().contains("left open"));
    }

    public void testMismatchedContainerIsRejected() {
        VariantBuilder builder = builder();
        builder.startObject();
        expectThrows(IllegalStateException.class, builder::endArray);
    }

    public void testAppendKeyOutsideObjectIsRejected() {
        VariantBuilder builder = builder();
        builder.startArray();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.appendKey("a"));
        assertTrue(e.getMessage(), e.getMessage().contains("only valid directly inside an object"));
    }
}
