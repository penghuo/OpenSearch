/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Property tests for the codec (test C3.2), plus robustness against malformed bytes.
 *
 * <p>Randomness comes from {@link OpenSearchTestCase}, so any failure reproduces from the reported {@code tests.seed}.
 */
public class VariantFuzzTests extends OpenSearchTestCase {

    private static final int MAX_DEPTH = 4;

    /**
     * C3.2: a random value tree must survive encode and decode unchanged.
     */
    public void testRandomTreeRoundTrips() {
        for (int iteration = 0; iteration < 200; iteration++) {
            Map<String, Object> original = randomObject(0);
            VariantBuilder builder = new VariantBuilder();
            VariantJson.encodeObject(original, builder);
            Variant variant = builder.finish();
            assertEquals("iteration " + iteration, original, variant.toJavaObject());
        }
    }

    /**
     * Every key must remain findable by binary search, whatever the key set looks like.
     */
    public void testEveryKeyIsFindable() {
        for (int iteration = 0; iteration < 100; iteration++) {
            Map<String, Object> original = randomObject(MAX_DEPTH - 1);
            VariantBuilder builder = new VariantBuilder();
            VariantJson.encodeObject(original, builder);
            Variant variant = builder.finish();
            for (Map.Entry<String, Object> entry : original.entrySet()) {
                Variant member = variant.objectGet(entry.getKey());
                assertNotNull("key [" + entry.getKey() + "] must be findable", member);
                assertEquals("value for [" + entry.getKey() + "]", entry.getValue(), member.toJavaObject());
            }
            assertNull(variant.objectGet("\u0000definitely-absent"));
        }
    }

    public void testKeyOrderDoesNotAffectTheDecodedValue() {
        Map<String, Object> original = randomObject(1);
        List<String> keys = new ArrayList<>(original.keySet());

        VariantBuilder first = new VariantBuilder();
        VariantJson.encodeObject(original, first);

        // The same members inserted in a different order must decode to the same value, because the encoder sorts them.
        java.util.Collections.shuffle(keys, random());
        Map<String, Object> reordered = new LinkedHashMap<>();
        for (String key : keys) {
            reordered.put(key, original.get(key));
        }
        VariantBuilder second = new VariantBuilder();
        VariantJson.encodeObject(reordered, second);

        assertEquals(first.finish().toJavaObject(), second.finish().toJavaObject());
    }

    /**
     * Truncated bytes must produce a {@link VariantFormatException}, never an unchecked failure.
     *
     * <p>These values are decoded on the search path, so a corrupt blob has to fail as a recognisable error rather than as
     * an {@link ArrayIndexOutOfBoundsException} surfacing from deep inside a decode.
     */
    public void testTruncatedValueIsRejectedCleanly() {
        Variant original = encodeRandom();
        byte[] metadata = original.metadataBytes();
        byte[] value = original.valueBytes();

        for (int length = 0; length < value.length; length++) {
            byte[] truncated = Arrays.copyOf(value, length);
            assertCleanFailureOrSuccess("value truncated to " + length, () -> walk(new Variant(metadata, truncated, 0)));
        }
    }

    public void testTruncatedMetadataIsRejectedCleanly() {
        Variant original = encodeRandom();
        byte[] metadata = original.metadataBytes();
        byte[] value = original.valueBytes();

        for (int length = 0; length < metadata.length; length++) {
            byte[] truncated = Arrays.copyOf(metadata, length);
            assertCleanFailureOrSuccess("metadata truncated to " + length, () -> walk(new Variant(truncated, value, 0)));
        }
    }

    public void testSingleByteCorruptionIsRejectedCleanly() {
        for (int iteration = 0; iteration < 500; iteration++) {
            Variant original = encodeRandom();
            byte[] metadata = original.metadataBytes().clone();
            byte[] value = original.valueBytes().clone();

            if (randomBoolean()) {
                value[randomIntBetween(0, value.length - 1)] = randomByte();
            } else {
                metadata[randomIntBetween(0, metadata.length - 1)] = randomByte();
            }
            assertCleanFailureOrSuccess("iteration " + iteration, () -> walk(new Variant(metadata, value, 0)));
        }
    }

    public void testRandomBytesAreRejectedCleanly() {
        for (int iteration = 0; iteration < 500; iteration++) {
            byte[] metadata = randomByteArrayOfLength(randomIntBetween(1, 24));
            byte[] value = randomByteArrayOfLength(randomIntBetween(1, 48));
            assertCleanFailureOrSuccess("iteration " + iteration, () -> walk(new Variant(metadata, value, 0)));
        }
    }

    /**
     * A container whose child offset points back at itself must be rejected rather than recursing until the stack
     * overflows.
     */
    public void testSelfReferentialOffsetDoesNotOverflowTheStack() {
        VariantBuilder builder = new VariantBuilder();
        builder.startArray();
        builder.appendLong(1);
        builder.endArray();
        Variant original = builder.finish();

        byte[] value = original.valueBytes().clone();
        // Layout is [header][count=1][offset0][offset1][element]. Point the first element back at the array header.
        int offsetZeroAt = 2;
        int valuesStart = 4;
        value[offsetZeroAt] = (byte) -valuesStart;

        try {
            walk(new Variant(original.metadataBytes(), value, 0));
        } catch (VariantFormatException expected) {
            return;
        }
        // Decoding something harmless is fine too; overflowing the stack is not, and that would surface as an Error.
    }

    private void assertCleanFailureOrSuccess(String message, Runnable action) {
        try {
            action.run();
        } catch (VariantFormatException expected) {
            // The contract: malformed input is reported, not crashed on.
        } catch (RuntimeException e) {
            throw new AssertionError(message + ": expected VariantFormatException but got " + e.getClass().getName(), e);
        }
    }

    /**
     * Touches every part of a value, so a decode error anywhere surfaces.
     */
    private static void walk(Variant variant) {
        switch (variant.type()) {
            case NULL:
                break;
            case BOOLEAN:
                variant.getBoolean();
                break;
            case LONG:
                variant.getLong();
                break;
            case FLOAT:
                variant.getFloat();
                break;
            case DOUBLE:
                variant.getDouble();
                break;
            case DECIMAL:
                variant.getDecimal();
                break;
            case STRING:
                variant.getString();
                break;
            case BINARY:
                variant.getBinary();
                break;
            case OBJECT: {
                int size = variant.objectSize();
                for (int i = 0; i < size; i++) {
                    variant.objectKeyAt(i);
                    walk(variant.objectValueAt(i));
                }
                variant.toJavaObject();
                break;
            }
            case ARRAY: {
                int size = variant.arraySize();
                for (int i = 0; i < size; i++) {
                    walk(variant.arrayGet(i));
                }
                variant.toJavaObject();
                break;
            }
            default:
                throw new AssertionError("unhandled type " + variant.type());
        }
    }

    private Variant encodeRandom() {
        VariantBuilder builder = new VariantBuilder();
        VariantJson.encodeObject(randomObject(0), builder);
        return builder.finish();
    }

    private Map<String, Object> randomObject(int depth) {
        int size = randomIntBetween(0, depth == 0 ? 12 : 4);
        Map<String, Object> map = new LinkedHashMap<>();
        // Keys must be unique within an object; the encoder rejects duplicates by design.
        while (map.size() < size) {
            map.put(randomKey(), randomValue(depth + 1));
        }
        return map;
    }

    private String randomKey() {
        switch (randomIntBetween(0, 4)) {
            case 0:
                return randomAlphaOfLengthBetween(1, 12);
            case 1:
                // A literal dotted key, the case the path rule has to disambiguate.
                return randomAlphaOfLengthBetween(1, 6) + "." + randomAlphaOfLengthBetween(1, 6);
            case 2:
                return "";
            case 3:
                return randomUnicodeOfCodepointLengthBetween(1, 8);
            default:
                return "key_" + randomIntBetween(0, 1000);
        }
    }

    private Object randomValue(int depth) {
        int limit = depth >= MAX_DEPTH ? 6 : 8;
        switch (randomIntBetween(0, limit)) {
            case 0:
                return null;
            case 1:
                return randomBoolean();
            case 2:
                return randomLong();
            case 3:
                return randomDouble();
            case 4:
                return randomAlphaOfLengthBetween(0, 100);
            case 5:
                // Straddles the 63-byte short-string boundary.
                return randomAlphaOfLength(randomBoolean() ? 63 : 64);
            case 6:
                return randomUnicodeOfCodepointLengthBetween(1, 20);
            case 7: {
                int size = randomIntBetween(0, 6);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(randomValue(depth + 1));
                }
                return list;
            }
            default:
                return randomObject(depth);
        }
    }

}
