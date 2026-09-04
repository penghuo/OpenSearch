/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VariantRoundTripTests extends OpenSearchTestCase {

    private static Variant encode(String json) throws Exception {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                null,
                json.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            parser.nextToken();
            VariantBuilder builder = new VariantBuilder();
            VariantJson.encode(parser, builder);
            return builder.finish();
        }
    }

    /**
     * Relabels a Variant's field ids into key-name order and returns a reader over it, exercising the in-place transform
     * independently of the mapper's deferred writer.
     *
     * <p>Relabelling leaves the Variant's own dictionary no longer describing its ids, so the value has to be re-read
     * through the indirect metadata form -- sorted names, identity ordinals -- which is what a segment's name column
     * supplies.
     */
    private static Variant relabelIntoNameOrder(Variant encoded) {
        int n = encoded.dictionarySize();
        byte[][] keyBytes = new byte[n][];
        Integer[] byName = new Integer[n];
        for (int i = 0; i < n; i++) {
            keyBytes[i] = encoded.dictionaryKey(i).getBytes(StandardCharsets.UTF_8);
            byName[i] = i;
        }
        Arrays.sort(byName, (a, b) -> Arrays.compareUnsigned(keyBytes[a], keyBytes[b]));

        int[] idMap = new int[n];
        byte[][] nameTable = new byte[n][];
        int[] ordinals = new int[n];
        for (int rank = 0; rank < n; rank++) {
            idMap[byName[rank]] = rank;
            nameTable[rank] = keyBytes[byName[rank]];
            ordinals[rank] = rank;
        }
        encoded.relabelFieldIds(idMap);
        return new Variant(new VariantMetadata(nameTable, ordinals, n), encoded.valueBytes(), 0);
    }

    /**
     * Searching by field id must find exactly what searching by name finds, for every key at every depth.
     *
     * <p>This is the invariant the columnar read path rests on: it never resolves a name, so if the two searches could
     * disagree it would silently return a value from the wrong key. The names are only present here in order to have
     * something to check the id search against.
     */
    public void testFieldIdSearchAgreesWithNameSearch() throws Exception {
        List<String> documents = List.of(
            "{\"zebra\":1,\"apple\":2,\"mango\":3}",
            "{\"a\":{\"c\":1,\"b\":2},\"z\":{\"y\":{\"x\":3}}}",
            "{\"k8s.namespace\":\"ns\",\"k8s\":{\"namespace\":\"other\"}}",
            "{\"status\":200,\"status_code\":404,\"stat\":1}",
            "{\"\\uFFFFhigh\":1,\"emoji\":2,\"ascii\":3}",
            "{\"outer\":{\"inner\":[1,2,{\"deep\":4}]},\"after\":5}",
            "{\"only\":1}",
            "{}"
        );
        for (String json : documents) {
            Variant reader = relabelIntoNameOrder(encode(json));
            Map<String, Integer> rankByName = new HashMap<>();
            for (int rank = 0; rank < reader.dictionarySize(); rank++) {
                rankByName.put(reader.dictionaryKey(rank), rank);
            }
            assertAgrees(json, reader, rankByName, 0);
        }
    }

    /** Walks every object in the tree, asserting the two searches agree on each member and on absent ids. */
    private static void assertAgrees(String json, Variant node, Map<String, Integer> rankByName, int depth) {
        assertTrue("runaway recursion in " + json, depth < 10);
        if (node.type() == VariantType.OBJECT) {
            for (int i = 0; i < node.objectSize(); i++) {
                String name = node.objectKeyAt(i);
                Integer rank = rankByName.get(name);
                assertNotNull(json + " key [" + name + "] is not in the dictionary", rank);

                Variant byName = node.objectGet(name);
                Variant byId = node.objectGetByFieldId(rank);
                assertNotNull(json + " name search missed [" + name + "]", byName);
                assertNotNull(json + " id search missed [" + name + "] at id " + rank, byId);
                assertEquals(json + " [" + name + "] resolved to a different value", byName.offset(), byId.offset());
                assertEquals(json + " [" + name + "]", byName.toJavaObject(), byId.toJavaObject());

                assertAgrees(json, node.objectValueAt(i), rankByName, depth + 1);
            }
            // An id the document has but this object does not must miss in both searches.
            for (Map.Entry<String, Integer> entry : rankByName.entrySet()) {
                if (node.objectGet(entry.getKey()) == null) {
                    assertNull(
                        json + " id search found [" + entry.getKey() + "] where the name search did not",
                        node.objectGetByFieldId(entry.getValue())
                    );
                }
            }
            // And an id beyond the dictionary must simply miss rather than throw.
            assertNull(json + " out-of-range id", node.objectGetByFieldId(rankByName.size() + 7));
        } else if (node.type() == VariantType.ARRAY) {
            for (int i = 0; i < node.arraySize(); i++) {
                assertAgrees(json, node.arrayGet(i), rankByName, depth + 1);
            }
        }
    }

    private static Map<String, Object> parseJson(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, MediaTypeRegistry.JSON).v2();
    }

    /**
     * Widens {@link Integer} to {@link Long} recursively so that a comparison is about the value rather than about which
     * boxed type each side happened to choose. The width difference is itself examined in the fidelity tests below.
     */
    private static Object widen(Object value) {
        if (value instanceof Integer integer) {
            return integer.longValue();
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(k, widen(v)));
            return result;
        }
        if (value instanceof List<?> list) {
            List<Object> result = new ArrayList<>(list.size());
            list.forEach(element -> result.add(widen(element)));
            return result;
        }
        return value;
    }

    private void assertRoundTrip(String json) throws Exception {
        Variant variant = encode(json);
        assertEquals("round trip of " + json, widen(parseJson(json)), widen(variant.toJavaObject()));
    }

    public void testAllValueKinds() throws Exception {
        assertRoundTrip("{\"i\":200}");
        assertRoundTrip("{\"d\":1.5}");
        assertRoundTrip("{\"s\":\"hello\"}");
        assertRoundTrip("{\"b\":true}");
        assertRoundTrip("{\"b\":false}");
        assertRoundTrip("{\"n\":null}");
        assertRoundTrip("{\"a\":[1,2,3]}");
        assertRoundTrip("{\"o\":{\"x\":1,\"y\":2}}");
        assertRoundTrip("{\"k8s.namespace\":\"ns-01\"}");
        assertRoundTrip("{}");
        assertRoundTrip("{\"empty_obj\":{},\"empty_arr\":[]}");
    }

    public void testMixedArray() throws Exception {
        assertRoundTrip("{\"a\":[1,\"two\",3.0,true,null,{\"x\":1},[2]]}");
    }

    public void testDeepNesting() throws Exception {
        assertRoundTrip("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":42}}}}}");
    }

    public void testManyKeys() throws Exception {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("\"key_").append(i).append("\":").append(i);
        }
        assertRoundTrip(json.append('}').toString());
    }

    public void testLargeObjectForcesLargeHeader() throws Exception {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 300; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("\"key_").append(i).append("\":\"value_").append(i).append('"');
        }
        assertRoundTrip(json.append('}').toString());
    }

    public void testLargeArrayForcesLargeHeader() throws Exception {
        StringBuilder json = new StringBuilder("{\"a\":[");
        for (int i = 0; i < 300; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(i);
        }
        assertRoundTrip(json.append("]}").toString());
    }

    public void testUnicodeKeysAndValues() throws Exception {
        assertRoundTrip("{\"ключ\":\"значение\",\"emoji\":\"a😀b\",\"é\":\"ü\"}");
    }

    public void testEmptyStringKeyAndValue() throws Exception {
        assertRoundTrip("{\"\":\"\"}");
    }

    public void testObjectLookupFindsEveryKey() throws Exception {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 64; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("\"k").append(i).append("\":").append(i);
        }
        Variant variant = encode(json.append('}').toString());
        for (int i = 0; i < 64; i++) {
            Variant member = variant.objectGet("k" + i);
            assertNotNull("key k" + i + " must be found by binary search", member);
            assertEquals(i, (int) member.getLong());
        }
        assertNull("an absent key must return null", variant.objectGet("nope"));
    }

    public void testLookupAfterKeysWithSharedPrefixes() throws Exception {
        // Prefix relationships are where a byte comparison that stops early would go wrong.
        Variant variant = encode("{\"a\":1,\"ab\":2,\"abc\":3,\"b\":4}");
        assertEquals(1L, variant.objectGet("a").getLong());
        assertEquals(2L, variant.objectGet("ab").getLong());
        assertEquals(3L, variant.objectGet("abc").getLong());
        assertEquals(4L, variant.objectGet("b").getLong());
        assertNull(variant.objectGet("abcd"));
    }

    public void testSubtreeAccessSharesTheBackingArray() throws Exception {
        Variant variant = encode("{\"outer\":{\"inner\":1}}");
        Variant inner = variant.objectGet("outer");
        assertSame(variant.valueBytes(), inner.valueBytes());
        assertTrue("the subtree must be addressed by offset", inner.offset() > 0);
    }

    public void testArrayIndexing() throws Exception {
        Variant variant = encode("{\"a\":[10,20,30]}");
        Variant array = variant.objectGet("a");
        assertEquals(3, array.arraySize());
        assertEquals(10L, array.arrayGet(0).getLong());
        assertEquals(30L, array.arrayGet(2).getLong());
        expectThrows(VariantFormatException.class, () -> array.arrayGet(3));
        expectThrows(VariantFormatException.class, () -> array.arrayGet(-1));
    }

    /**
     * Narrowing must be a function of the value alone, so the same document always encodes identically.
     */
    public void testIntegerNarrowingIsDeterministicAndMinimal() {
        assertEquals(VariantEncoding.P_INT8, primitiveOf(0L));
        assertEquals(VariantEncoding.P_INT8, primitiveOf(127L));
        assertEquals(VariantEncoding.P_INT8, primitiveOf(-128L));
        assertEquals(VariantEncoding.P_INT16, primitiveOf(128L));
        assertEquals(VariantEncoding.P_INT16, primitiveOf(32767L));
        assertEquals(VariantEncoding.P_INT16, primitiveOf(-32768L));
        assertEquals(VariantEncoding.P_INT32, primitiveOf(32768L));
        assertEquals(VariantEncoding.P_INT32, primitiveOf((long) Integer.MAX_VALUE));
        assertEquals(VariantEncoding.P_INT64, primitiveOf(Integer.MAX_VALUE + 1L));
        assertEquals(VariantEncoding.P_INT64, primitiveOf(Long.MAX_VALUE));
        assertEquals(VariantEncoding.P_INT64, primitiveOf(Long.MIN_VALUE));
    }

    private static int primitiveOf(long value) {
        VariantBuilder builder = new VariantBuilder();
        builder.appendLong(value);
        return builder.finish().primitiveTypeId();
    }

    public void testExtremeIntegersRoundTrip() {
        for (long value : new long[] {
            0L,
            1L,
            -1L,
            Byte.MAX_VALUE,
            Byte.MIN_VALUE,
            Short.MAX_VALUE,
            Short.MIN_VALUE,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            Long.MAX_VALUE,
            Long.MIN_VALUE }) {
            VariantBuilder builder = new VariantBuilder();
            builder.appendLong(value);
            assertEquals("integer " + value, value, builder.finish().getLong());
        }
    }

    public void testBigIntegerBeyondLongRoundTrips() {
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(1000));
        VariantBuilder builder = new VariantBuilder();
        builder.appendBigInteger(value);
        Variant variant = builder.finish();
        assertEquals(VariantType.DECIMAL, variant.type());
        assertEquals("a big integer must survive exactly", value, variant.toJavaObject());
    }

    public void testNegativeBigIntegerBeyondLongRoundTrips() {
        BigInteger value = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(1000));
        VariantBuilder builder = new VariantBuilder();
        builder.appendBigInteger(value);
        assertEquals(value, builder.finish().toJavaObject());
    }

    public void testBigIntegerInsideLongRangeIsStoredAsAnInteger() {
        VariantBuilder builder = new VariantBuilder();
        builder.appendBigInteger(BigInteger.valueOf(200));
        Variant variant = builder.finish();
        assertEquals(VariantType.LONG, variant.type());
        assertEquals(200L, variant.getLong());
    }

    /**
     * The bound is decimal digits, not bytes. A signed 16-byte integer reaches 39 digits, which the encoding does not allow,
     * so the largest accepted value is the largest 38-digit one and one more than that is refused.
     */
    public void testIntegerBeyondThirtyEightDigitsIsRejected() {
        BigInteger largest = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
        VariantBuilder accepts = new VariantBuilder();
        accepts.appendBigInteger(largest);
        assertEquals(0, largest.compareTo(accepts.finish().getDecimal().toBigIntegerExact()));

        for (BigInteger tooLarge : List.of(BigInteger.TEN.pow(38), BigInteger.ONE.shiftLeft(200))) {
            VariantBuilder builder = new VariantBuilder();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.appendBigInteger(tooLarge));
            assertTrue(e.getMessage(), e.getMessage().contains("more than 38 decimal digits"));
        }
    }

    /** Exact decimals at each width boundary, and the values a double could not have carried. */
    public void testDecimalsRoundTripExactly() {
        for (String text : List.of(
            "0",
            "1",
            "0.1",
            "-0.1",
            "123456789",                                  // 9 digits: decimal4
            "1000000000",                                 // 10 digits: decimal8, not decimal4
            "123456789012345678",                         // 18 digits: decimal8
            "1000000000000000000",                        // 19 digits: decimal16, not decimal8
            "12345678901234567890.12345",                 // 25 digits, no double holds it
            "0.1000000000000000055511151231257827",       // 34 digits
            "-12345678901234567890123456789012345678",    // 38 digits, the widest allowed
            "99999999999999999999999999999999999999"
        )) {
            BigDecimal value = new BigDecimal(text);
            VariantBuilder builder = new VariantBuilder();
            builder.appendBigDecimal(value);
            Variant variant = builder.finish();
            BigDecimal read = variant.getDecimal();
            assertEquals(text + " must survive exactly", 0, value.compareTo(read));
        }
    }

    /** Trailing zeros and a negative scale are canonicalised, which keeps the number and drops only the spelling. */
    public void testTrailingZerosAndNegativeScaleAreCanonicalised() {
        for (String text : List.of("1.2000", "100.000", "1E+2", "1.0E+3", "0.00")) {
            BigDecimal value = new BigDecimal(text);
            VariantBuilder builder = new VariantBuilder();
            builder.appendBigDecimal(value);
            BigDecimal read = builder.finish().getDecimal();
            assertEquals(text + " must keep its value", 0, value.compareTo(read));
            assertTrue(text + " must have a non-negative scale once stored", read.scale() >= 0);
        }
    }

    /** Past 38 digits, or a scale past 38, there is no exact form, and refusing is the only honest answer. */
    public void testDecimalsBeyondTheEncodingAreRejected() {
        for (String text : List.of(
            "1." + "1".repeat(39),                        // 40 digits
            "1" + "0".repeat(38),                         // 39 digits
            "0." + "0".repeat(38) + "1"                   // scale 39
        )) {
            BigDecimal value = new BigDecimal(text);
            assertFalse(text + " must not claim to be representable", VariantBuilder.canRepresentExactly(value));
            VariantBuilder builder = new VariantBuilder();
            expectThrows(IllegalArgumentException.class, () -> builder.appendBigDecimal(value));
        }
    }

    /** Each decimal width may use one fewer digit than its bytes allow, and a payload using the extra one is corrupt. */
    public void testCorruptDecimalPayloadsAreRefused() {
        // decimal4 physically holds 10 digits; the format allows 9.
        assertCorruptDecimal(VariantEncoding.P_DECIMAL4, 4, 0, BigInteger.valueOf(1_000_000_000L), "decimal4 holds at most 9");
        // decimal8 physically holds 19; the format allows 18.
        assertCorruptDecimal(VariantEncoding.P_DECIMAL8, 8, 0, BigInteger.valueOf(1_000_000_000_000_000_000L), "decimal8 holds at most 18");
        // And a scale past 38 is refused at any width.
        assertCorruptDecimal(VariantEncoding.P_DECIMAL4, 4, 39, BigInteger.ONE, "decimal scale 39 exceeds 38");
        assertCorruptDecimal(VariantEncoding.P_DECIMAL16, 16, 255, BigInteger.ONE, "decimal scale 255 exceeds 38");
    }

    /**
     * An offset pointing outside its own container's data region is refused rather than decoded.
     *
     * <p>What this pins is containment, not exact extents. An object member is bounded by its object's data region, so a
     * corrupt offset that leaves the object is caught; one that stays inside it and lands on a later member of the same
     * object is not, because finding a member's exact end is a linear scan of the offset table and this is the path a PPL
     * projection runs per document. Array elements do get exact extents, since the next offset supplies one in a single
     * read -- {@link #testACorruptArrayOffsetCannotReachTheNextElement} pins that.
     */
    public void testACorruptOffsetOutsideItsContainerIsRefused() {
        // {"a": {"b": 1}, "c": 2} -- so an over-large offset inside the inner object would land in `c`'s bytes.
        VariantBuilder builder = new VariantBuilder();
        builder.startObject();
        builder.appendKey("a");
        builder.startObject();
        builder.appendKey("b");
        builder.appendLong(1);
        builder.endObject();
        builder.appendKey("c");
        builder.appendLong(2);
        builder.endObject();
        Variant original = builder.finish();
        assertEquals(1L, original.objectGet("a").objectGet("b").getLong());

        byte[] bytes = original.valueBytes();
        // Walk the outer object's offset table and corrupt the inner object's single element offset to point past its data.
        Variant inner = original.objectGet("a");
        int innerPos = inner.offset();
        // Inner layout: header, count (1 byte, small), one field id, two offsets. Push element 0's offset past the terminal.
        int offsetsStart = innerPos + 1 + 1 + 1;
        int terminal = bytes[offsetsStart + 1] & 0xFF;
        bytes[offsetsStart] = (byte) (terminal + 1);

        Variant corrupt = new Variant(new VariantMetadata(original.metadataBytes()), bytes, 0, 0, bytes.length);
        VariantFormatException e = expectThrows(VariantFormatException.class, () -> corrupt.objectGet("a").objectGet("b"));
        assertTrue(e.getMessage(), e.getMessage().contains("lies outside its own object's data"));
    }

    /**
     * An array element cannot read the element after it, because the next offset gives its exact end.
     */
    public void testACorruptArrayOffsetCannotReachTheNextElement() {
        VariantBuilder builder = new VariantBuilder();
        builder.startArray();
        builder.appendLong(1);
        builder.appendLong(2);
        builder.endArray();
        Variant original = builder.finish();
        assertEquals(1L, original.arrayGet(0).getLong());
        assertEquals(2L, original.arrayGet(1).getLong());

        byte[] bytes = original.valueBytes();
        // Array layout: header, count, offsets[0..2]. Make element 0 start where element 1 does.
        int offsetsStart = original.offset() + 1 + 1;
        bytes[offsetsStart] = bytes[offsetsStart + 1];

        Variant corrupt = new Variant(new VariantMetadata(original.metadataBytes()), bytes, 0, 0, bytes.length);
        VariantFormatException e = expectThrows(VariantFormatException.class, () -> corrupt.arrayGet(0));
        assertTrue(e.getMessage(), e.getMessage().contains("is not after its start"));
    }

    /** And a terminal offset larger than the value itself is refused rather than trusted. */
    public void testACorruptTerminalOffsetIsRefused() {
        VariantBuilder builder = new VariantBuilder();
        builder.startArray();
        builder.appendLong(1);
        builder.endArray();
        Variant original = builder.finish();
        assertEquals(1L, original.arrayGet(0).getLong());

        byte[] bytes = original.valueBytes();
        // Array layout: header, count, offsets[0], offsets[1]=data size. Claim a data region past the end of the value.
        int terminalAt = original.offset() + 1 + 1 + 1;
        bytes[terminalAt] = (byte) 0xFF;

        Variant corrupt = new Variant(new VariantMetadata(original.metadataBytes()), bytes, 0, 0, bytes.length);
        VariantFormatException e = expectThrows(VariantFormatException.class, () -> corrupt.arrayGet(0));
        assertTrue(e.getMessage(), e.getMessage().contains("exceeds the value"));
    }

    /** Hand-builds one decimal value's bytes, bypassing the builder's own checks, and requires the decoder to refuse them. */
    private void assertCorruptDecimal(int typeId, int width, int scale, BigInteger unscaled, String expected) {
        byte[] value = new byte[2 + width];
        value[0] = (byte) (VariantEncoding.BASIC_PRIMITIVE | (typeId << 2));
        value[1] = (byte) scale;
        byte[] magnitude = unscaled.toByteArray();
        byte fill = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
        for (int i = 0; i < width; i++) {
            value[2 + i] = i < magnitude.length ? magnitude[magnitude.length - 1 - i] : fill;
        }
        Variant variant = new Variant(new VariantMetadata(new byte[] { 1, 0, 0 }), value, 0, 0, value.length);
        VariantFormatException e = expectThrows(VariantFormatException.class, variant::getDecimal);
        assertTrue(e.getMessage(), e.getMessage().contains(expected));
    }

    public void testNegativeZeroIsPreservedBitForBit() {
        VariantBuilder builder = new VariantBuilder();
        builder.appendDouble(-0.0);
        double result = builder.finish().getDouble();
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(result));
    }

    public void testSpecialDoublesRoundTrip() {
        for (double value : new double[] {
            0.0,
            -0.0,
            1.0,
            -1.0,
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            Double.NaN,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY }) {
            VariantBuilder builder = new VariantBuilder();
            builder.appendDouble(value);
            double result = builder.finish().getDouble();
            assertEquals("double " + value, Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(result));
        }
    }

    public void testFloatKeepsItsOwnWidth() {
        VariantBuilder builder = new VariantBuilder();
        builder.appendFloat(1.5f);
        Variant variant = builder.finish();
        assertEquals(VariantType.FLOAT, variant.type());
        assertEquals(1.5f, variant.getFloat(), 0.0f);
    }

    /**
     * JSON gives a reader no way to tell {@code 200}, {@code 200.0} and {@code 2e2} apart beyond
     * integer-versus-floating-point, so the encoder cannot either. Pinned to make the limit explicit: what Variant adds is
     * a type tag over the parser's decision, not a recovery of information the text never carried.
     */
    public void testJsonNumberFormsThatCollapse() throws Exception {
        assertEquals(VariantType.LONG, encode("{\"v\":200}").objectGet("v").type());
        assertEquals(VariantType.DOUBLE, encode("{\"v\":200.0}").objectGet("v").type());
        assertEquals(VariantType.DOUBLE, encode("{\"v\":2e2}").objectGet("v").type());

        // 200.0 and 2e2 are indistinguishable after parsing; 200 stays distinct because it is an integer.
        assertEquals(200.0, encode("{\"v\":200.0}").objectGet("v").getDouble(), 0.0);
        assertEquals(200.0, encode("{\"v\":2e2}").objectGet("v").getDouble(), 0.0);
        assertEquals(200L, encode("{\"v\":200}").objectGet("v").getLong());
    }

    public void testLeadingZeroStringStaysAString() throws Exception {
        Variant variant = encode("{\"v\":\"007\"}");
        assertEquals(VariantType.STRING, variant.objectGet("v").type());
        assertEquals("007", variant.objectGet("v").getString());
    }

    /**
     * A number and its decimal text must stay distinguishable, which is what {@code _source} cannot do.
     */
    public void testNumericStringIsDistinctFromNumber() throws Exception {
        assertEquals(VariantType.STRING, encode("{\"v\":\"200\"}").objectGet("v").type());
        assertEquals(VariantType.LONG, encode("{\"v\":200}").objectGet("v").type());
    }

    public void testBigIntegerFromJson() throws Exception {
        String big = "123456789012345678901234567890";
        Variant variant = encode("{\"v\":" + big + "}");
        assertEquals(VariantType.DECIMAL, variant.objectGet("v").type());
        assertEquals(new BigInteger(big), variant.objectGet("v").toJavaObject());
        // And the same value read back from _source-style parsing agrees, so the two stores will not diverge here.
        assertEquals(new BigInteger(big), parseJson("{\"v\":" + big + "}").get("v"));
    }

    public void testStringLengthBoundary() {
        for (int length : new int[] { 0, 1, 62, 63, 64, 65, 1000 }) {
            String value = "x".repeat(length);
            VariantBuilder builder = new VariantBuilder();
            builder.appendString(value);
            assertEquals("string of length " + length, value, builder.finish().getString());
        }
    }

    public void testBinaryRoundTrip() {
        byte[] value = new byte[300];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        VariantBuilder builder = new VariantBuilder();
        builder.appendBinary(value);
        Variant variant = builder.finish();
        assertEquals(VariantType.BINARY, variant.type());
        assertArrayEquals(value, variant.getBinary());
    }

    public void testTypedGettersRejectWrongTypes() {
        VariantBuilder builder = new VariantBuilder();
        builder.appendString("hello");
        Variant variant = builder.finish();
        expectThrows(VariantFormatException.class, variant::getLong);
        expectThrows(VariantFormatException.class, variant::getDouble);
        expectThrows(VariantFormatException.class, variant::getBoolean);
        expectThrows(VariantFormatException.class, variant::objectSize);
        expectThrows(VariantFormatException.class, variant::arraySize);
    }

    public void testEncodeObjectMatchesParserPath() throws Exception {
        String json = "{\"i\":200,\"d\":1.5,\"s\":\"x\",\"b\":true,\"n\":null,\"a\":[1,2],\"o\":{\"k\":\"v\"}}";
        Variant viaParser = encode(json);
        VariantBuilder builder = new VariantBuilder();
        VariantJson.encodeObject(widen(parseJson(json)), builder);
        Variant viaObject = builder.finish();
        assertEquals(widen(viaParser.toJavaObject()), widen(viaObject.toJavaObject()));
    }
}
