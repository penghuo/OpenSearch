/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Bridges between {@link XContentParser} token streams, plain Java values, and Variant bytes.
 *
 * <p>The parser-driven path exists so a document can be encoded during the single walk the mapper already performs. An
 * easier implementation would read the subtree into a {@code Map} and encode that, but the extra materialisation would be
 * charged to the blob arm's write cost and would bias the indexing-throughput comparison against it.
 *
 * @opensearch.internal
 */
public final class VariantJson {

    private VariantJson() {}

    /**
     * Encodes the value the parser is currently positioned on, consuming exactly that value.
     *
     * <p>On entry {@code parser.currentToken()} must be the value's first token; on return the parser sits on the value's
     * last token, matching the convention the mapper's own walk uses.
     */
    public static void encode(XContentParser parser, VariantBuilder builder) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            throw new IllegalStateException("parser is not positioned on a value");
        }
        switch (token) {
            case START_OBJECT:
                builder.startObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                        throw new IllegalStateException("expected a field name but found " + parser.currentToken());
                    }
                    builder.appendKey(parser.currentName());
                    parser.nextToken();
                    encode(parser, builder);
                }
                builder.endObject();
                break;
            case START_ARRAY:
                builder.startArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    encode(parser, builder);
                }
                builder.endArray();
                break;
            case VALUE_STRING:
                builder.appendString(parser.text());
                break;
            case VALUE_NUMBER:
                encodeNumber(parser, builder);
                break;
            case VALUE_BOOLEAN:
                builder.appendBoolean(parser.booleanValue());
                break;
            case VALUE_NULL:
                builder.appendNull();
                break;
            case VALUE_EMBEDDED_OBJECT:
                builder.appendBinary(parser.binaryValue());
                break;
            default:
                throw new IllegalStateException("unexpected token " + token);
        }
    }

    /**
     * Picks the Variant type from the parser's own view of the number.
     *
     * <p>This is where the arms' type information comes from, and it is the same source for both: Solution A asks the
     * parser at read time, Solution B asks it at write time. Anything the parser cannot distinguish — and JSON text gives
     * it no way to tell {@code 200} meant to be a {@code long} from {@code 200} meant to be an {@code int} — is not
     * recoverable by either arm.
     */
    private static void encodeNumber(XContentParser parser, VariantBuilder builder) throws IOException {
        switch (parser.numberType()) {
            case INT:
            case LONG:
                builder.appendLong(parser.longValue());
                break;
            case FLOAT:
                builder.appendFloat(parser.floatValue());
                break;
            case DOUBLE:
                builder.appendDouble(parser.doubleValue());
                break;
            case BIG_INTEGER:
                builder.appendBigInteger(toBigInteger(parser.numberValue()));
                break;
            case BIG_DECIMAL:
                builder.appendDouble(parser.doubleValue());
                break;
            default:
                throw new IllegalStateException("unhandled number type " + parser.numberType());
        }
    }

    private static BigInteger toBigInteger(Number number) {
        if (number instanceof BigInteger big) {
            return big;
        }
        if (number instanceof BigDecimal decimal) {
            return decimal.toBigInteger();
        }
        return BigInteger.valueOf(number.longValue());
    }

    /**
     * Encodes a plain Java value, for callers that already hold a materialised tree such as tests.
     */
    public static void encodeObject(Object value, VariantBuilder builder) {
        if (value == null) {
            builder.appendNull();
        } else if (value instanceof Map<?, ?> map) {
            builder.startObject();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                builder.appendKey(String.valueOf(entry.getKey()));
                encodeObject(entry.getValue(), builder);
            }
            builder.endObject();
        } else if (value instanceof List<?> list) {
            builder.startArray();
            for (Object element : list) {
                encodeObject(element, builder);
            }
            builder.endArray();
        } else if (value instanceof String text) {
            builder.appendString(text);
        } else if (value instanceof Boolean bool) {
            builder.appendBoolean(bool);
        } else if (value instanceof BigInteger big) {
            builder.appendBigInteger(big);
        } else if (value instanceof Double || value instanceof BigDecimal) {
            builder.appendDouble(((Number) value).doubleValue());
        } else if (value instanceof Float number) {
            builder.appendFloat(number);
        } else if (value instanceof Number number) {
            builder.appendLong(number.longValue());
        } else if (value instanceof byte[] bytes) {
            builder.appendBinary(bytes);
        } else {
            throw new IllegalArgumentException("cannot encode " + value.getClass().getName() + " as a Variant value");
        }
    }

    /**
     * Reconstructs a Variant object as a map of plain Java values.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(Variant variant) {
        Object converted = variant.toJavaObject();
        if (converted instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("Variant value is a " + variant.type() + ", not an object");
    }
}
