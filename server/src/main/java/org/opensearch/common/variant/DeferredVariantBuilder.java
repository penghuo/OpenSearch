/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.common.annotation.InternalApi;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records a Variant token stream and emits it once its final dictionary order is known.
 *
 * <p>The regular {@link VariantBuilder} assigns dictionary ids while streaming. That is correct when its inline metadata
 * preserves insertion order, but a caller storing names in a sorted external column needs ids in name order. This builder
 * delays only the binary encoding: parsing still takes one pass, while the final replay writes container headers and field
 * ids once with the sorted dictionary already installed.
 *
 * @opensearch.internal
 */
@InternalApi
public final class DeferredVariantBuilder {

    private static final byte START_OBJECT = 0;
    private static final byte END_OBJECT = 1;
    private static final byte START_ARRAY = 2;
    private static final byte END_ARRAY = 3;
    private static final byte KEY = 4;
    private static final byte NULL = 5;
    private static final byte BOOLEAN = 6;
    private static final byte LONG = 7;
    private static final byte FLOAT = 8;
    private static final byte DOUBLE = 9;
    private static final byte BIG_INTEGER = 10;
    private static final byte BIG_DECIMAL = 11;
    private static final byte STRING = 12;
    private static final byte BINARY = 13;

    private byte[] eventTypes = new byte[32];
    private long[] eventNumbers = new long[32];
    private Object[] eventValues = new Object[32];
    private int eventCount;

    private final List<String> dictionaryKeys = new ArrayList<>();
    private final Map<String, Integer> dictionaryIds = new HashMap<>();
    private boolean unencodable;

    public void startObject() {
        appendEvent(START_OBJECT, 0, null);
    }

    public void endObject() {
        appendEvent(END_OBJECT, 0, null);
    }

    public void startArray() {
        appendEvent(START_ARRAY, 0, null);
    }

    public void endArray() {
        appendEvent(END_ARRAY, 0, null);
    }

    public void appendKey(String key) {
        Integer id = dictionaryIds.get(key);
        if (id == null) {
            id = dictionaryKeys.size();
            dictionaryIds.put(key, id);
            dictionaryKeys.add(key);
        }
        appendEvent(KEY, id, null);
    }

    public void appendNull() {
        appendEvent(NULL, 0, null);
    }

    public void appendBoolean(boolean value) {
        appendEvent(BOOLEAN, value ? 1 : 0, null);
    }

    public void appendLong(long value) {
        appendEvent(LONG, value, null);
    }

    public void appendFloat(float value) {
        appendEvent(FLOAT, Float.floatToRawIntBits(value), null);
    }

    public void appendDouble(double value) {
        appendEvent(DOUBLE, Double.doubleToRawLongBits(value), null);
    }

    public void appendBigInteger(BigInteger value) {
        appendEvent(BIG_INTEGER, 0, value);
    }

    public void appendBigDecimal(BigDecimal value) {
        appendEvent(BIG_DECIMAL, 0, value);
    }

    public void appendString(String value) {
        appendEvent(STRING, 0, value);
    }

    public void appendBinary(byte[] value) {
        appendEvent(BINARY, 0, Arrays.copyOf(value, value.length));
    }

    public void markUnencodable() {
        unencodable = true;
    }

    public boolean isUnencodable() {
        return unencodable;
    }

    public int dictionarySize() {
        return dictionaryKeys.size();
    }

    /**
     * Encodes the recorded value with field ids assigned in unsigned UTF-8 name order.
     *
     * @return the encoded value, or {@code null} when its object keys cannot be represented unambiguously
     */
    public EncodedValue finish() {
        if (unencodable) {
            return null;
        }

        int keyCount = dictionaryKeys.size();
        byte[][] keyBytesById = new byte[keyCount][];
        Integer[] idsByName = new Integer[keyCount];
        int[] finalIdById = new int[keyCount];
        for (int id = 0; id < keyCount; id++) {
            keyBytesById[id] = dictionaryKeys.get(id).getBytes(StandardCharsets.UTF_8);
            idsByName[id] = id;
        }
        Arrays.sort(idsByName, (left, right) -> Arrays.compareUnsigned(keyBytesById[left], keyBytesById[right]));

        byte[][] sortedKeyBytes = new byte[keyCount][];
        for (int rank = 0; rank < keyCount; rank++) {
            int id = idsByName[rank];
            byte[] keyBytes = keyBytesById[id];
            if (rank > 0 && Arrays.equals(sortedKeyBytes[rank - 1], keyBytes)) {
                return null;
            }
            sortedKeyBytes[rank] = keyBytes;
            finalIdById[id] = rank;
        }

        VariantBuilder builder = new VariantBuilder(Math.max(128, eventCount * 4));
        try {
            replay(builder, finalIdById, keyBytesById);
        } catch (VariantBuilder.DuplicateObjectKeyException e) {
            return null;
        }
        return new EncodedValue(sortedKeyBytes, builder.finishValueBytes());
    }

    private void replay(VariantBuilder builder, int[] finalIdById, byte[][] keyBytesById) {
        for (int i = 0; i < eventCount; i++) {
            switch (eventTypes[i]) {
                case START_OBJECT:
                    builder.startObject();
                    break;
                case END_OBJECT:
                    builder.endObject();
                    break;
                case START_ARRAY:
                    builder.startArray();
                    break;
                case END_ARRAY:
                    builder.endArray();
                    break;
                case KEY: {
                    int id = (int) eventNumbers[i];
                    builder.appendKey(finalIdById[id], keyBytesById[id]);
                    break;
                }
                case NULL:
                    builder.appendNull();
                    break;
                case BOOLEAN:
                    builder.appendBoolean(eventNumbers[i] != 0);
                    break;
                case LONG:
                    builder.appendLong(eventNumbers[i]);
                    break;
                case FLOAT:
                    builder.appendFloat(Float.intBitsToFloat((int) eventNumbers[i]));
                    break;
                case DOUBLE:
                    builder.appendDouble(Double.longBitsToDouble(eventNumbers[i]));
                    break;
                case BIG_INTEGER:
                    builder.appendBigInteger((BigInteger) eventValues[i]);
                    break;
                case BIG_DECIMAL:
                    builder.appendBigDecimal((BigDecimal) eventValues[i]);
                    break;
                case STRING:
                    builder.appendString((String) eventValues[i]);
                    break;
                case BINARY:
                    builder.appendBinary((byte[]) eventValues[i]);
                    break;
                default:
                    throw new IllegalStateException("unknown deferred Variant event " + eventTypes[i]);
            }
        }
    }

    private void appendEvent(byte type, long number, Object value) {
        if (eventCount == eventTypes.length) {
            int newLength = eventCount << 1;
            eventTypes = Arrays.copyOf(eventTypes, newLength);
            eventNumbers = Arrays.copyOf(eventNumbers, newLength);
            eventValues = Arrays.copyOf(eventValues, newLength);
        }
        eventTypes[eventCount] = type;
        eventNumbers[eventCount] = number;
        eventValues[eventCount] = value;
        eventCount++;
    }

    /**
     * The two values the flat_object mapper writes to separate doc-values columns.
     */
    public static final class EncodedValue {
        private final byte[][] sortedKeyBytes;
        private final byte[] valueBytes;

        private EncodedValue(byte[][] sortedKeyBytes, byte[] valueBytes) {
            this.sortedKeyBytes = sortedKeyBytes;
            this.valueBytes = valueBytes;
        }

        public byte[][] sortedKeyBytes() {
            return sortedKeyBytes;
        }

        public byte[] valueBytes() {
            return valueBytes;
        }
    }
}
