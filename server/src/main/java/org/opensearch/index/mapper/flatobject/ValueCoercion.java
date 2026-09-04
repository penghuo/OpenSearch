/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts a raw stored value to the type a caller asked for.
 *
 * <p>Shared by both value stores for the same reason as {@link PathResolver}: coercion has to be identical, or the same
 * path would answer differently depending on which store served it. Each accessor's job ends at producing the raw Java
 * value at a path; everything after that happens here.
 *
 * <p>Failures are reported through the {@link #FAILED} sentinel rather than by returning {@code null}, so that
 * "absent", "present and null" and "could not be read as the requested type" stay distinguishable. A caller counting
 * how many values it skipped cannot tell them apart otherwise.
 *
 * @opensearch.internal
 */
public final class ValueCoercion {

    /** Sentinel meaning the value exists but cannot be represented as the requested type. */
    public static final Object FAILED = new Object();

    private ValueCoercion() {}

    /**
     * Coerces {@code raw} to {@code type}.
     *
     * @param raw the value as stored; {@code null} for a stored null
     * @return the coerced value, {@code null} if {@code raw} was null, or {@link #FAILED} if coercion is not possible
     */
    public static Object coerce(Object raw, ValueType type) {
        if (raw == null) {
            return null;
        }
        switch (type) {
            case RAW:
                return raw;
            case LONG:
                return toLong(raw);
            case DOUBLE:
                return toDouble(raw);
            case STRING:
                return toStringValue(raw);
            case BOOLEAN:
                return toBoolean(raw);
            default:
                throw new IllegalArgumentException("unhandled value type [" + type + "]");
        }
    }

    private static Object toLong(Object raw) {
        if (raw instanceof Long value) {
            return value;
        }
        if (raw instanceof Integer || raw instanceof Short || raw instanceof Byte) {
            return ((Number) raw).longValue();
        }
        if (raw instanceof BigInteger big) {
            // Out of range is a failure rather than a silent wrap: a wrapped value would be indistinguishable from a
            // legitimate one and would corrupt an aggregation without any signal.
            try {
                return big.longValueExact();
            } catch (ArithmeticException e) {
                return FAILED;
            }
        }
        if (raw instanceof BigDecimal big) {
            try {
                return big.toBigInteger().longValueExact();
            } catch (ArithmeticException e) {
                return FAILED;
            }
        }
        if (raw instanceof Double || raw instanceof Float) {
            double value = ((Number) raw).doubleValue();
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return FAILED;
            }
            // >= rather than >: (double) Long.MAX_VALUE rounds up to exactly 2^63, so the strict comparison lets 2^63
            // through and the cast below saturates it to Long.MAX_VALUE -- reporting a value the document does not hold.
            if (value < Long.MIN_VALUE || value >= 0x1p63) {
                return FAILED;
            }
            // Truncation toward zero, matching a narrowing cast.
            return (long) value;
        }
        if (raw instanceof String text) {
            return parseLong(text);
        }
        // Booleans and containers have no meaningful numeric reading.
        return FAILED;
    }

    private static Object parseLong(String text) {
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return FAILED;
        }
        try {
            return Long.parseLong(trimmed);
        } catch (NumberFormatException e) {
            // Fall back to a floating-point reading so "200.7" behaves like the double 200.7 would.
            try {
                double value = Double.parseDouble(trimmed);
                // See toLong: the strict comparison against Long.MAX_VALUE lets exactly 2^63 through.
                if (Double.isNaN(value) || Double.isInfinite(value) || value < Long.MIN_VALUE || value >= 0x1p63) {
                    return FAILED;
                }
                return (long) value;
            } catch (NumberFormatException inner) {
                return FAILED;
            }
        }
    }

    private static Object toDouble(Object raw) {
        if (raw instanceof Double value) {
            return value;
        }
        if (raw instanceof Number number) {
            return number.doubleValue();
        }
        if (raw instanceof String text) {
            String trimmed = text.trim();
            if (trimmed.isEmpty()) {
                return FAILED;
            }
            try {
                return Double.parseDouble(trimmed);
            } catch (NumberFormatException e) {
                return FAILED;
            }
        }
        return FAILED;
    }

    private static Object toStringValue(Object raw) {
        if (raw instanceof String value) {
            return value;
        }
        if (raw instanceof Map || raw instanceof List) {
            // Stringifying a container would require agreeing on a canonical serialization; whole-value comparison goes
            // through the accessor's reconstruction path instead.
            return FAILED;
        }
        return String.valueOf(raw);
    }

    private static Object toBoolean(Object raw) {
        if (raw instanceof Boolean value) {
            return value;
        }
        if (raw instanceof String text) {
            String normalized = text.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(normalized)) {
                return Boolean.TRUE;
            }
            if ("false".equals(normalized)) {
                return Boolean.FALSE;
            }
        }
        return FAILED;
    }
}
