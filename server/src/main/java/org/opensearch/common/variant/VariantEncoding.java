/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

/**
 * Constants for the Apache Parquet Variant binary encoding.
 *
 * <p>A Variant value is a pair of byte arrays: {@code metadata}, holding a dictionary of field-name strings, and
 * {@code value}, holding a type-tagged tree that refers to that dictionary by index.
 *
 * <p>Layouts, all little-endian:
 *
 * <pre>
 * metadata: &lt;header byte&gt; &lt;dictionary_size&gt; &lt;offset&gt;*(size+1) &lt;bytes&gt;
 *   header: version(bits 0-3) | sorted_strings(bit 4) | reserved(bit 5) | offset_size_minus_one(bits 6-7)
 *
 * value:    &lt;value_metadata byte&gt; &lt;value_data&gt;
 *   value_metadata: basic_type(bits 0-1) | value_header(bits 2-7)
 *
 * object value_data: &lt;num_elements&gt; &lt;field_id&gt;*n &lt;field_offset&gt;*(n+1) &lt;values&gt;
 *   value_header: field_offset_size_minus_one(0-1) | field_id_size_minus_one(2-3) | is_large(4) | reserved(5)
 *
 * array value_data:  &lt;num_elements&gt; &lt;field_offset&gt;*(n+1) &lt;values&gt;
 *   value_header: field_offset_size_minus_one(0-1) | is_large(2) | reserved(3-5)
 * </pre>
 *
 * <p>Note the asymmetry in {@code is_large}: bit 4 of the value header for objects, bit 2 for arrays. Objects carry a
 * {@code field_id_size} field in between and arrays do not. Getting this wrong produces a decoder that round-trips its
 * own output perfectly while being incompatible with every other implementation, which is why the tests include
 * hand-written golden byte arrays rather than relying on round-trip checks alone.
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Variant Binary Encoding</a>
 *
 * @opensearch.internal
 */
public final class VariantEncoding {

    private VariantEncoding() {}

    // ---- metadata header ----

    /** The only version this implementation writes or accepts. */
    public static final int VERSION = 1;
    public static final int VERSION_MASK = 0x0F;
    public static final int SORTED_STRINGS_SHIFT = 4;
    public static final int OFFSET_SIZE_SHIFT = 6;

    // ---- value metadata byte ----

    public static final int BASIC_TYPE_MASK = 0x03;
    public static final int VALUE_HEADER_SHIFT = 2;

    public static final int BASIC_PRIMITIVE = 0;
    public static final int BASIC_SHORT_STRING = 1;
    public static final int BASIC_OBJECT = 2;
    public static final int BASIC_ARRAY = 3;

    // ---- primitive type ids ----

    public static final int P_NULL = 0;
    public static final int P_TRUE = 1;
    public static final int P_FALSE = 2;
    public static final int P_INT8 = 3;
    public static final int P_INT16 = 4;
    public static final int P_INT32 = 5;
    public static final int P_INT64 = 6;
    public static final int P_DOUBLE = 7;
    public static final int P_DECIMAL4 = 8;
    public static final int P_DECIMAL8 = 9;
    public static final int P_DECIMAL16 = 10;
    public static final int P_DATE = 11;
    public static final int P_TIMESTAMP_TZ = 12;
    public static final int P_TIMESTAMP_NTZ = 13;
    public static final int P_FLOAT = 14;
    public static final int P_BINARY = 15;
    public static final int P_STRING = 16;
    public static final int P_TIME_NTZ = 17;
    public static final int P_TIMESTAMP_NANOS_TZ = 18;
    public static final int P_TIMESTAMP_NANOS_NTZ = 19;
    public static final int P_UUID = 20;

    /** Strings of this length or shorter are encoded with the length folded into the header byte. */
    public static final int MAX_SHORT_STRING_LEN = 63;

    // ---- object value header ----

    public static final int OBJ_FIELD_OFFSET_SIZE_SHIFT = 0;
    public static final int OBJ_FIELD_ID_SIZE_SHIFT = 2;
    public static final int OBJ_IS_LARGE_SHIFT = 4;

    // ---- array value header ----

    public static final int ARR_FIELD_OFFSET_SIZE_SHIFT = 0;
    public static final int ARR_IS_LARGE_SHIFT = 2;

    /** Above this many elements, {@code num_elements} must be written as four bytes rather than one. */
    public static final int MAX_SMALL_ELEMENT_COUNT = 255;

    /** Builds the {@code value_metadata} byte. */
    public static byte valueMetadata(int basicType, int valueHeader) {
        return (byte) ((basicType & BASIC_TYPE_MASK) | (valueHeader << VALUE_HEADER_SHIFT));
    }

    public static int basicType(byte valueMetadata) {
        return valueMetadata & BASIC_TYPE_MASK;
    }

    public static int valueHeader(byte valueMetadata) {
        return (valueMetadata & 0xFF) >>> VALUE_HEADER_SHIFT;
    }

    /**
     * The smallest number of bytes that can hold {@code value} as an unsigned integer, clamped to the 1..4 the format
     * allows.
     */
    public static int minUnsignedWidth(int value) {
        if (value <= 0xFF) {
            return 1;
        }
        if (value <= 0xFFFF) {
            return 2;
        }
        if (value <= 0xFFFFFF) {
            return 3;
        }
        return 4;
    }

    /** Writes {@code value} as an unsigned little-endian integer of {@code width} bytes. */
    public static void writeUnsigned(byte[] target, int offset, int value, int width) {
        for (int i = 0; i < width; i++) {
            target[offset + i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
    }

    /** Reads an unsigned little-endian integer of {@code width} bytes. */
    public static int readUnsigned(byte[] source, int offset, int width) {
        int value = 0;
        for (int i = 0; i < width; i++) {
            value |= (source[offset + i] & 0xFF) << (8 * i);
        }
        return value;
    }
}
