/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Audits the encoder against the Apache Parquet Variant specification.
 *
 * <p>The property under test is minimality: every container and scalar must use the narrowest encoding the spec permits.
 * A non-minimal encoder round-trips its own output perfectly, so no other test in the suite would notice, and a reader
 * written against the spec is entitled to assume the narrow forms exist.
 *
 * <p>The audit encodes through {@link VariantJson#encode}, which drives the same {@link VariantBuilder} calls the mapper's
 * own parse walk does, so the bytes counted here are the bytes the index holds.
 */
public class VariantEncodingAuditTests extends OpenSearchTestCase {

    /**
     * Documents chosen so that every narrow form the spec offers is actually exercised: integers at each width boundary,
     * strings either side of the 64-byte short-string limit, containers small enough for one-byte field ids and offsets and
     * large enough to need more, and nesting deep enough that an inner container's widths differ from its parent's.
     */
    private static final List<String> NARROWNESS_DOCUMENTS = List.of(
        // Integer widths, at and either side of each boundary.
        "{\"i8\":127,\"i8n\":-128,\"i16\":128,\"i16n\":-32768,\"i32\":32768,\"i32n\":-2147483648,\"i64\":2147483648}",
        // Zero, one, and the extremes.
        "{\"z\":0,\"o\":1,\"max\":9223372036854775807,\"min\":-9223372036854775808}",
        // Strings either side of the short-string limit.
        "{\"short\":\"a\",\"at63\":\"" + "x".repeat(63) + "\",\"at64\":\"" + "x".repeat(64) + "\"}",
        // Other scalars.
        "{\"t\":true,\"f\":false,\"n\":null,\"d\":1.5,\"neg\":-0.0}",
        // Nesting, so an inner container's widths are chosen independently of its parent's.
        "{\"a\":{\"b\":{\"c\":{\"d\":1}}},\"arr\":[[1,2],[3,[4,5]]]}",
        // Empty containers.
        "{\"eo\":{},\"ea\":[]}",
        // Enough members that field ids and offsets need more than one byte.
        wideObject(300),
        // And enough bytes that offsets need more than two.
        wideStrings(400, 200)
    );

    /** An object with {@code members} distinct keys, to push field-id and offset widths past one byte. */
    private static String wideObject(int members) {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < members; i++) {
            json.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
        }
        return json.append("}").toString();
    }

    /** An object whose values are long enough that the offset table needs a wider element. */
    private static String wideStrings(int members, int length) {
        String value = "y".repeat(length);
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < members; i++) {
            json.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":\"").append(value).append("\"");
        }
        return json.append("}").toString();
    }

    /**
     * Every value must use the narrowest form the spec offers: integers narrowed to int8/int16/int32/int64, strings under
     * 64 bytes folded into the header byte, and each container's field-id and field-offset widths the minimum that can
     * address it.
     *
     * <p>A non-minimal encoder would round-trip its own output perfectly, so nothing else in the suite would catch this.
     */
    public void testEveryValueUsesTheNarrowestSpecForm() {
        Tally tally = new Tally();
        for (String json : NARROWNESS_DOCUMENTS) {
            Variant variant = encode(json.getBytes(StandardCharsets.UTF_8));
            walk(variant.valueArray(), variant.offset(), tally);
        }

        assertEquals("integers wider than necessary: " + tally.nonMinimalIntegers, 0, tally.nonMinimalIntegers);
        assertEquals("short strings encoded in long form: " + tally.nonMinimalStrings, 0, tally.nonMinimalStrings);
        assertEquals("containers with a wider field-id width than needed: " + tally.nonMinimalFieldIds, 0, tally.nonMinimalFieldIds);
        assertEquals("containers with a wider offset width than needed: " + tally.nonMinimalOffsets, 0, tally.nonMinimalOffsets);
        assertEquals("element counts written in 4-byte form unnecessarily: " + tally.nonMinimalCounts, 0, tally.nonMinimalCounts);

        // Every narrow form the documents above were chosen to exercise must actually have been used, or the assertions
        // above would pass by never reaching the case they guard.
        for (String form : List.of("int8", "int16", "int32", "int64", "string (short form)", "string (long form)", "object", "array")) {
            assertTrue("no " + form + " was encoded, so its minimality was never tested: " + tally.forms, tally.forms.containsKey(form));
        }
    }

    /** Encodes one JSON object through the same builder calls the mapper's parse walk makes. */
    private static Variant encode(byte[] json) {
        VariantBuilder builder = new VariantBuilder(1024);
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            parser.nextToken();
            VariantJson.encode(parser, builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return builder.finish();
    }

    private static void walk(byte[] value, int pos, Tally tally) {
        int basic = VariantEncoding.basicType(value[pos]);
        int header = VariantEncoding.valueHeader(value[pos]);
        switch (basic) {
            case VariantEncoding.BASIC_OBJECT: {
                boolean isLarge = ((header >>> VariantEncoding.OBJ_IS_LARGE_SHIFT) & 1) != 0;
                int idSize = ((header >>> VariantEncoding.OBJ_FIELD_ID_SIZE_SHIFT) & 0x03) + 1;
                int offsetSize = ((header >>> VariantEncoding.OBJ_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;
                int countWidth = isLarge ? 4 : 1;
                int count = VariantEncoding.readUnsigned(value, pos + 1, countWidth);
                int idsStart = pos + 1 + countWidth;
                int offsetsStart = idsStart + count * idSize;
                int valuesStart = offsetsStart + (count + 1) * offsetSize;

                tally.form("object");
                tally.containerHeaderBytes += 1 + countWidth;
                tally.fieldIdBytes += (long) count * idSize;
                tally.fieldOffsetBytes += (long) (count + 1) * offsetSize;
                tally.objectMembersByIdWidth.merge(idSize, (long) count, Long::sum);

                int maxId = 0;
                for (int i = 0; i < count; i++) {
                    maxId = Math.max(maxId, VariantEncoding.readUnsigned(value, idsStart + i * idSize, idSize));
                }
                int dataSize = VariantEncoding.readUnsigned(value, offsetsStart + count * offsetSize, offsetSize);
                if (idSize != VariantEncoding.minUnsignedWidth(maxId)) {
                    tally.nonMinimalFieldIds++;
                }
                if (offsetSize != VariantEncoding.minUnsignedWidth(dataSize)) {
                    tally.nonMinimalOffsets++;
                }
                if (isLarge && count <= VariantEncoding.MAX_SMALL_ELEMENT_COUNT) {
                    tally.nonMinimalCounts++;
                }
                for (int i = 0; i < count; i++) {
                    walk(value, valuesStart + VariantEncoding.readUnsigned(value, offsetsStart + i * offsetSize, offsetSize), tally);
                }
                return;
            }
            case VariantEncoding.BASIC_ARRAY: {
                boolean isLarge = ((header >>> VariantEncoding.ARR_IS_LARGE_SHIFT) & 1) != 0;
                int offsetSize = ((header >>> VariantEncoding.ARR_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;
                int countWidth = isLarge ? 4 : 1;
                int count = VariantEncoding.readUnsigned(value, pos + 1, countWidth);
                int offsetsStart = pos + 1 + countWidth;
                int valuesStart = offsetsStart + (count + 1) * offsetSize;

                tally.form("array");
                tally.containerHeaderBytes += 1 + countWidth;
                tally.fieldOffsetBytes += (long) (count + 1) * offsetSize;

                int dataSize = VariantEncoding.readUnsigned(value, offsetsStart + count * offsetSize, offsetSize);
                if (offsetSize != VariantEncoding.minUnsignedWidth(dataSize)) {
                    tally.nonMinimalOffsets++;
                }
                if (isLarge && count <= VariantEncoding.MAX_SMALL_ELEMENT_COUNT) {
                    tally.nonMinimalCounts++;
                }
                for (int i = 0; i < count; i++) {
                    walk(value, valuesStart + VariantEncoding.readUnsigned(value, offsetsStart + i * offsetSize, offsetSize), tally);
                }
                return;
            }
            case VariantEncoding.BASIC_SHORT_STRING: {
                tally.form("string (short form)");
                tally.typeTagBytes += 1;
                tally.scalarPayloadBytes += header;
                return;
            }
            default:
                break;
        }

        tally.typeTagBytes += 1;
        switch (header) {
            case VariantEncoding.P_NULL:
                tally.form("null");
                return;
            case VariantEncoding.P_TRUE:
            case VariantEncoding.P_FALSE:
                tally.form("boolean");
                return;
            case VariantEncoding.P_INT8:
            case VariantEncoding.P_INT16:
            case VariantEncoding.P_INT32:
            case VariantEncoding.P_INT64: {
                int width = switch (header) {
                    case VariantEncoding.P_INT8 -> 1;
                    case VariantEncoding.P_INT16 -> 2;
                    case VariantEncoding.P_INT32 -> 4;
                    default -> 8;
                };
                tally.form("int" + (width * 8));
                tally.scalarPayloadBytes += width;
                long stored = readSigned(value, pos + 1, width);
                if (width > minimalIntegerWidth(stored)) {
                    tally.nonMinimalIntegers++;
                }
                return;
            }
            case VariantEncoding.P_FLOAT:
                tally.form("float");
                tally.scalarPayloadBytes += 4;
                return;
            case VariantEncoding.P_DOUBLE:
                tally.form("double");
                tally.scalarPayloadBytes += 8;
                return;
            case VariantEncoding.P_DECIMAL4:
                tally.form("decimal4");
                tally.scalarPayloadBytes += 5;
                return;
            case VariantEncoding.P_DECIMAL8:
                tally.form("decimal8");
                tally.scalarPayloadBytes += 9;
                return;
            case VariantEncoding.P_DECIMAL16:
                tally.form("decimal16");
                tally.scalarPayloadBytes += 17;
                return;
            case VariantEncoding.P_STRING: {
                int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
                tally.form("string (long form)");
                tally.containerHeaderBytes += 4;
                tally.scalarPayloadBytes += length;
                if (length <= VariantEncoding.MAX_SHORT_STRING_LEN) {
                    tally.nonMinimalStrings++;
                }
                return;
            }
            case VariantEncoding.P_BINARY: {
                int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
                tally.form("binary");
                tally.containerHeaderBytes += 4;
                tally.scalarPayloadBytes += length;
                return;
            }
            default:
                throw new AssertionError("unexpected primitive type id " + header);
        }
    }

    /**
     * What the walk found. The byte counters are kept because the walk has to compute the widths anyway to check them, and
     * they make a failure message say how far off the encoding was rather than only that it was.
     */
    /** Reads a signed little-endian integer of {@code width} bytes, as the encoding stores int8..int64. */
    private static long readSigned(byte[] value, int pos, int width) {
        long result = 0;
        for (int i = 0; i < width; i++) {
            result |= (value[pos + i] & 0xFFL) << (8 * i);
        }
        // Sign-extend from the top bit of the stored width.
        int shift = 64 - 8 * width;
        return (result << shift) >> shift;
    }

    /** The narrowest of the encoding's four integer widths that can hold {@code value}. */
    private static int minimalIntegerWidth(long value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return 1;
        }
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return 2;
        }
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return 4;
        }
        return 8;
    }

    private static final class Tally {
        long containerHeaderBytes;
        long fieldIdBytes;
        long fieldOffsetBytes;
        long typeTagBytes;
        long scalarPayloadBytes;

        long nonMinimalIntegers;
        long nonMinimalStrings;
        long nonMinimalFieldIds;
        long nonMinimalOffsets;
        long nonMinimalCounts;

        /** field-id width used -> number of object members encoded at that width. */
        final Map<Integer, Long> objectMembersByIdWidth = new TreeMap<>();
        final Map<String, Long> forms = new TreeMap<>();

        void form(String name) {
            forms.merge(name, 1L, Long::sum);
        }
    }
}
