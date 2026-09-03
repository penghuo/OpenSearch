/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ValueCoercionTests extends OpenSearchTestCase {

    private static Object coerce(Object raw, ValueType type) {
        return ValueCoercion.coerce(raw, type);
    }

    public void testNullPassesThroughForEveryType() {
        for (ValueType type : ValueType.values()) {
            assertNull("null must stay null for " + type, coerce(null, type));
        }
    }

    public void testRawIsUntouched() {
        Object value = Map.of("a", 1);
        assertSame(value, coerce(value, ValueType.RAW));
    }

    // ---- LONG ----

    public void testLongFromIntegralTypes() {
        assertEquals(5L, coerce(5L, ValueType.LONG));
        assertEquals(5L, coerce(5, ValueType.LONG));
        assertEquals(5L, coerce((short) 5, ValueType.LONG));
        assertEquals(5L, coerce((byte) 5, ValueType.LONG));
    }

    public void testLongFromDoubleTruncatesTowardZero() {
        assertEquals(200L, coerce(200.7, ValueType.LONG));
        assertEquals(-200L, coerce(-200.7, ValueType.LONG));
        assertEquals(0L, coerce(-0.9, ValueType.LONG));
    }

    public void testLongFromString() {
        assertEquals(200L, coerce("200", ValueType.LONG));
        assertEquals(200L, coerce("  200  ", ValueType.LONG));
        assertEquals(7L, coerce("007", ValueType.LONG));
        assertEquals(-5L, coerce("-5", ValueType.LONG));
    }

    public void testLongFromNumericStringWithFraction() {
        // Falls back to a floating-point reading, so "200.7" behaves like the double.
        assertEquals(200L, coerce("200.7", ValueType.LONG));
        assertEquals(200L, coerce("2e2", ValueType.LONG));
    }

    public void testLongFromBigIntegerInRange() {
        assertEquals(Long.MAX_VALUE, coerce(BigInteger.valueOf(Long.MAX_VALUE), ValueType.LONG));
        assertEquals(Long.MIN_VALUE, coerce(BigInteger.valueOf(Long.MIN_VALUE), ValueType.LONG));
    }

    /**
     * Out of range must fail rather than wrap. A wrapped value is indistinguishable from a legitimate one and would
     * corrupt an aggregation with no signal that anything happened.
     */
    public void testLongFromBigIntegerOutOfRangeFails() {
        BigInteger tooBig = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        assertSame(ValueCoercion.FAILED, coerce(tooBig, ValueType.LONG));
        BigInteger tooSmall = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
        assertSame(ValueCoercion.FAILED, coerce(tooSmall, ValueType.LONG));
    }

    public void testLongFromBigDecimal() {
        assertEquals(200L, coerce(new BigDecimal("200.99"), ValueType.LONG));
    }

    public void testLongFromNonNumericFails() {
        assertSame(ValueCoercion.FAILED, coerce("OK", ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce("", ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(true, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(Map.of("a", 1), ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(List.of(1), ValueType.LONG));
    }

    public void testLongFromNonFiniteDoubleFails() {
        assertSame(ValueCoercion.FAILED, coerce(Double.NaN, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(Double.POSITIVE_INFINITY, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(Double.NEGATIVE_INFINITY, ValueType.LONG));
    }

    public void testLongFromOutOfRangeDoubleFails() {
        assertSame(ValueCoercion.FAILED, coerce(1e300, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(-1e300, ValueType.LONG));
    }

    // ---- DOUBLE ----

    public void testDoubleFromNumbers() {
        assertEquals(1.5, coerce(1.5, ValueType.DOUBLE));
        assertEquals(5.0, coerce(5L, ValueType.DOUBLE));
        assertEquals(5.0, coerce(5, ValueType.DOUBLE));
        assertEquals(1.5, (Double) coerce(1.5f, ValueType.DOUBLE), 1e-6);
    }

    public void testDoubleFromString() {
        assertEquals(200.0, coerce("200", ValueType.DOUBLE));
        assertEquals(200.0, coerce("2e2", ValueType.DOUBLE));
        assertEquals(200.5, coerce("200.5", ValueType.DOUBLE));
    }

    public void testNegativeZeroIsPreserved() {
        Double result = (Double) coerce(-0.0, ValueType.DOUBLE);
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(result));
    }

    public void testDoubleFromNonNumericFails() {
        assertSame(ValueCoercion.FAILED, coerce("OK", ValueType.DOUBLE));
        assertSame(ValueCoercion.FAILED, coerce(true, ValueType.DOUBLE));
        assertSame(ValueCoercion.FAILED, coerce(Map.of(), ValueType.DOUBLE));
    }

    // ---- STRING ----

    public void testStringFromScalars() {
        assertEquals("abc", coerce("abc", ValueType.STRING));
        assertEquals("200", coerce(200L, ValueType.STRING));
        assertEquals("200.5", coerce(200.5, ValueType.STRING));
        assertEquals("true", coerce(true, ValueType.STRING));
    }

    public void testStringFromContainerFails() {
        assertSame(ValueCoercion.FAILED, coerce(Map.of("a", 1), ValueType.STRING));
        assertSame(ValueCoercion.FAILED, coerce(List.of(1), ValueType.STRING));
    }

    // ---- BOOLEAN ----

    public void testBooleanFromBoolean() {
        assertEquals(Boolean.TRUE, coerce(true, ValueType.BOOLEAN));
        assertEquals(Boolean.FALSE, coerce(false, ValueType.BOOLEAN));
    }

    public void testBooleanFromString() {
        assertEquals(Boolean.TRUE, coerce("true", ValueType.BOOLEAN));
        assertEquals(Boolean.TRUE, coerce("TRUE", ValueType.BOOLEAN));
        assertEquals(Boolean.FALSE, coerce(" false ", ValueType.BOOLEAN));
    }

    public void testBooleanFromOtherFails() {
        assertSame(ValueCoercion.FAILED, coerce(1L, ValueType.BOOLEAN));
        assertSame(ValueCoercion.FAILED, coerce("yes", ValueType.BOOLEAN));
        assertSame(ValueCoercion.FAILED, coerce(Map.of(), ValueType.BOOLEAN));
    }

    /**
     * The point of the mixed-type case in C1.6: the same path holding a number in one document and a word in another must
     * produce one value and one counted exclusion, identically in both arms.
     */
    /**
     * A double at or above 2^63 has no long to become, so it must fail rather than saturate.
     *
     * <p>The obvious guard reads {@code value > Long.MAX_VALUE}, which never fires at the boundary: casting
     * {@code Long.MAX_VALUE} to a double rounds it up to exactly 2^63, so the comparison is false and the narrowing cast
     * then clamps to {@code Long.MAX_VALUE} -- reporting a number the document does not contain.
     */
    public void testADoubleAtTwoToTheSixtyThreeCannotBecomeALong() {
        assertSame(ValueCoercion.FAILED, coerce(0x1p63, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce("9223372036854775808", ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce(-0x1p64, ValueType.LONG));
        // Just inside the range still works, and is exact.
        assertEquals(Long.MAX_VALUE, coerce(Long.MAX_VALUE, ValueType.LONG));
        assertEquals(9223372036854774784L, coerce(9.223372036854774784E18, ValueType.LONG));
    }

    public void testMixedTypePathBehaviour() {
        assertEquals(200L, coerce(200L, ValueType.LONG));
        assertSame(ValueCoercion.FAILED, coerce("OK", ValueType.LONG));
        // Coerced to string instead, both are representable.
        assertEquals("200", coerce(200L, ValueType.STRING));
        assertEquals("OK", coerce("OK", ValueType.STRING));
    }
}
