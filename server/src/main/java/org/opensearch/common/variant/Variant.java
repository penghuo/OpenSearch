/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A decoded view over Variant bytes.
 *
 * <p>A {@code Variant} is a cursor, not a copy: it holds the shared {@code metadata} and {@code value} arrays plus the
 * offset of one value within them. {@link #objectGet} and {@link #arrayGet} return another cursor over the same arrays,
 * so reaching a subtree costs a binary search and an offset computation rather than a re-parse. That property is the
 * whole point of the blob arm, so it is preserved carefully here — nothing in this class copies the value bytes.
 *
 * <p>Every read is bounds-checked and raises {@link VariantFormatException} on malformed input.
 *
 * @opensearch.internal
 */
public final class Variant {

    private final VariantMetadata metadata;
    private final byte[] value;
    private final int pos;
    /** Exclusive end of this value's region within {@link #value}, so a shared buffer can be read without copying. */
    private final int valueEnd;
    /** First byte of the value region, retained so {@link #valueBytes()} can return exactly that region. */
    private final int valueBase;

    /**
     * Parses the metadata header and wraps the value. Prefer the {@link VariantMetadata} overload when reading many
     * values that share one dictionary, so the header is parsed once.
     */
    public Variant(byte[] metadata, byte[] value, int pos) {
        this(new VariantMetadata(metadata), value, pos);
    }

    public Variant(VariantMetadata metadata, byte[] value, int pos) {
        this(metadata, value, pos, pos, value.length);
    }

    /**
     * Reads a value in place from a region of a shared buffer.
     *
     * @param valueBase first byte of the value region
     * @param valueEnd  exclusive end of the value region
     */
    public Variant(VariantMetadata metadata, byte[] value, int pos, int valueBase, int valueEnd) {
        this.metadata = metadata;
        this.value = value;
        this.pos = pos;
        this.valueBase = valueBase;
        this.valueEnd = valueEnd;
        require(valueEnd <= value.length, "value region ends at " + valueEnd + " beyond buffer of " + value.length);
        require(pos >= valueBase && pos < valueEnd, "value offset " + pos + " outside region [" + valueBase + ", " + valueEnd + ")");
    }

    private Variant at(int newPos) {
        return new Variant(metadata, value, newPos, valueBase, valueEnd);
    }

    // ------------------------------------------------------------------ type

    public VariantType type() {
        int basic = VariantEncoding.basicType(value[pos]);
        switch (basic) {
            case VariantEncoding.BASIC_SHORT_STRING:
                return VariantType.STRING;
            case VariantEncoding.BASIC_OBJECT:
                return VariantType.OBJECT;
            case VariantEncoding.BASIC_ARRAY:
                return VariantType.ARRAY;
            case VariantEncoding.BASIC_PRIMITIVE:
                return primitiveType(VariantEncoding.valueHeader(value[pos]));
            default:
                throw new VariantFormatException("unknown basic type " + basic);
        }
    }

    /**
     * The raw primitive type id, or {@code -1} for a short string, object or array.
     *
     * <p>Exposed because the type-fidelity comparison is about exactly this: whether the stored width of an integer
     * survived, which the coarser {@link VariantType} deliberately hides.
     */
    public int primitiveTypeId() {
        return VariantEncoding.basicType(value[pos]) == VariantEncoding.BASIC_PRIMITIVE ? VariantEncoding.valueHeader(value[pos]) : -1;
    }

    private static VariantType primitiveType(int typeId) {
        switch (typeId) {
            case VariantEncoding.P_NULL:
                return VariantType.NULL;
            case VariantEncoding.P_TRUE:
            case VariantEncoding.P_FALSE:
                return VariantType.BOOLEAN;
            case VariantEncoding.P_INT8:
            case VariantEncoding.P_INT16:
            case VariantEncoding.P_INT32:
            case VariantEncoding.P_INT64:
                return VariantType.LONG;
            case VariantEncoding.P_FLOAT:
                return VariantType.FLOAT;
            case VariantEncoding.P_DOUBLE:
                return VariantType.DOUBLE;
            case VariantEncoding.P_DECIMAL4:
            case VariantEncoding.P_DECIMAL8:
            case VariantEncoding.P_DECIMAL16:
                return VariantType.DECIMAL;
            case VariantEncoding.P_STRING:
                return VariantType.STRING;
            case VariantEncoding.P_BINARY:
                return VariantType.BINARY;
            default:
                throw new VariantFormatException("unsupported primitive type id " + typeId);
        }
    }

    // ---------------------------------------------------------------- objects

    public int objectSize() {
        return objectLayout()[0];
    }

    /**
     * Looks up a key by binary search over the object's field ids.
     *
     * <p>The comparison is against the dictionary's UTF-8 bytes rather than a decoded {@code String}, so a lookup
     * allocates nothing per probe.
     *
     * @return a cursor over the member's value, or {@code null} if the key is absent
     */
    public Variant objectGet(String key) {
        int[] layout = objectLayout();
        int numElements = layout[0];
        int fieldIdSize = layout[1];
        int fieldOffsetSize = layout[2];
        int fieldIdsStart = layout[3];
        int fieldOffsetsStart = layout[4];
        int valuesStart = layout[5];

        byte[] probe = key.getBytes(StandardCharsets.UTF_8);
        int low = 0;
        int high = numElements - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int fieldId = VariantEncoding.readUnsigned(value, fieldIdsStart + mid * fieldIdSize, fieldIdSize);
            int comparison = metadata.compareKey(fieldId, probe);
            if (comparison < 0) {
                low = mid + 1;
            } else if (comparison > 0) {
                high = mid - 1;
            } else {
                int offset = VariantEncoding.readUnsigned(value, fieldOffsetsStart + mid * fieldOffsetSize, fieldOffsetSize);
                return at(valuesStart + offset);
            }
        }
        return null;
    }

    public String objectKeyAt(int index) {
        int[] layout = objectLayout();
        require(index >= 0 && index < layout[0], "object index " + index + " out of range");
        int fieldId = VariantEncoding.readUnsigned(value, layout[3] + index * layout[1], layout[1]);
        return metadata.key(fieldId);
    }

    public Variant objectValueAt(int index) {
        int[] layout = objectLayout();
        require(index >= 0 && index < layout[0], "object index " + index + " out of range");
        int offset = VariantEncoding.readUnsigned(value, layout[4] + index * layout[2], layout[2]);
        return at(layout[5] + offset);
    }

    /**
     * @return {numElements, fieldIdSize, fieldOffsetSize, fieldIdsStart, fieldOffsetsStart, valuesStart}
     */
    private int[] objectLayout() {
        requireBasicType(VariantEncoding.BASIC_OBJECT, "an object");
        int header = VariantEncoding.valueHeader(value[pos]);
        boolean isLarge = ((header >>> VariantEncoding.OBJ_IS_LARGE_SHIFT) & 0x01) != 0;
        int fieldIdSize = ((header >>> VariantEncoding.OBJ_FIELD_ID_SIZE_SHIFT) & 0x03) + 1;
        int fieldOffsetSize = ((header >>> VariantEncoding.OBJ_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;

        int at = pos + 1;
        int countWidth = isLarge ? 4 : 1;
        checkValueBounds(at, countWidth);
        // A 4-byte count read as an int can come back negative on corrupt input, so widen before doing any arithmetic.
        long numElements = VariantEncoding.readUnsigned(value, at, countWidth) & 0xFFFFFFFFL;
        at += countWidth;

        long fieldIdsStart = at;
        long fieldOffsetsStart = fieldIdsStart + numElements * fieldIdSize;
        long valuesStart = fieldOffsetsStart + (numElements + 1) * fieldOffsetSize;
        // Computed in long arithmetic so a bogus element count cannot overflow into a range that looks valid; the bound
        // also caps numElements at the blob length, which keeps a corrupt count from driving a huge allocation.
        require(valuesStart <= valueEnd, "object header claims " + numElements + " elements, which exceeds the value region");
        return new int[] {
            (int) numElements,
            fieldIdSize,
            fieldOffsetSize,
            (int) fieldIdsStart,
            (int) fieldOffsetsStart,
            (int) valuesStart };
    }

    // ----------------------------------------------------------------- arrays

    public int arraySize() {
        return arrayLayout()[0];
    }

    public Variant arrayGet(int index) {
        int[] layout = arrayLayout();
        require(index >= 0 && index < layout[0], "array index " + index + " out of range");
        int offset = VariantEncoding.readUnsigned(value, layout[2] + index * layout[1], layout[1]);
        return at(layout[3] + offset);
    }

    /**
     * @return {numElements, fieldOffsetSize, fieldOffsetsStart, valuesStart}
     */
    private int[] arrayLayout() {
        requireBasicType(VariantEncoding.BASIC_ARRAY, "an array");
        int header = VariantEncoding.valueHeader(value[pos]);
        boolean isLarge = ((header >>> VariantEncoding.ARR_IS_LARGE_SHIFT) & 0x01) != 0;
        int fieldOffsetSize = ((header >>> VariantEncoding.ARR_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;

        int at = pos + 1;
        int countWidth = isLarge ? 4 : 1;
        checkValueBounds(at, countWidth);
        long numElements = VariantEncoding.readUnsigned(value, at, countWidth) & 0xFFFFFFFFL;
        at += countWidth;

        long fieldOffsetsStart = at;
        long valuesStart = fieldOffsetsStart + (numElements + 1) * fieldOffsetSize;
        require(valuesStart <= valueEnd, "array header claims " + numElements + " elements, which exceeds the value region");
        return new int[] { (int) numElements, fieldOffsetSize, (int) fieldOffsetsStart, (int) valuesStart };
    }

    // ---------------------------------------------------------------- scalars

    public boolean isNull() {
        return type() == VariantType.NULL;
    }

    public boolean getBoolean() {
        int typeId = primitiveTypeId();
        if (typeId == VariantEncoding.P_TRUE) {
            return true;
        }
        if (typeId == VariantEncoding.P_FALSE) {
            return false;
        }
        throw new VariantFormatException("value is not a boolean");
    }

    public long getLong() {
        switch (primitiveTypeId()) {
            case VariantEncoding.P_INT8:
                return readSigned(pos + 1, 1);
            case VariantEncoding.P_INT16:
                return readSigned(pos + 1, 2);
            case VariantEncoding.P_INT32:
                return readSigned(pos + 1, 4);
            case VariantEncoding.P_INT64:
                return readSigned(pos + 1, 8);
            default:
                throw new VariantFormatException("value is not an integer");
        }
    }

    public double getDouble() {
        int typeId = primitiveTypeId();
        if (typeId == VariantEncoding.P_DOUBLE) {
            return Double.longBitsToDouble(readSigned(pos + 1, 8));
        }
        if (typeId == VariantEncoding.P_FLOAT) {
            return Float.intBitsToFloat((int) readSigned(pos + 1, 4));
        }
        throw new VariantFormatException("value is not a floating point number");
    }

    public float getFloat() {
        if (primitiveTypeId() != VariantEncoding.P_FLOAT) {
            throw new VariantFormatException("value is not a float");
        }
        return Float.intBitsToFloat((int) readSigned(pos + 1, 4));
    }

    public BigDecimal getDecimal() {
        int typeId = primitiveTypeId();
        int width;
        switch (typeId) {
            case VariantEncoding.P_DECIMAL4:
                width = 4;
                break;
            case VariantEncoding.P_DECIMAL8:
                width = 8;
                break;
            case VariantEncoding.P_DECIMAL16:
                width = 16;
                break;
            default:
                throw new VariantFormatException("value is not a decimal");
        }
        checkValueBounds(pos + 1, 1 + width);
        int scale = value[pos + 1] & 0xFF;
        // Variant decimals are little-endian; BigInteger wants big-endian two's complement.
        byte[] bigEndian = new byte[width];
        for (int i = 0; i < width; i++) {
            bigEndian[i] = value[pos + 2 + (width - 1 - i)];
        }
        return new BigDecimal(new BigInteger(bigEndian), scale);
    }

    public String getString() {
        int basic = VariantEncoding.basicType(value[pos]);
        if (basic == VariantEncoding.BASIC_SHORT_STRING) {
            int length = VariantEncoding.valueHeader(value[pos]);
            checkValueBounds(pos + 1, length);
            return new String(value, pos + 1, length, StandardCharsets.UTF_8);
        }
        if (primitiveTypeId() == VariantEncoding.P_STRING) {
            checkValueBounds(pos + 1, 4);
            int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
            require(length >= 0, "negative string length");
            checkValueBounds(pos + 5, length);
            return new String(value, pos + 5, length, StandardCharsets.UTF_8);
        }
        throw new VariantFormatException("value is not a string");
    }

    public byte[] getBinary() {
        if (primitiveTypeId() != VariantEncoding.P_BINARY) {
            throw new VariantFormatException("value is not binary");
        }
        checkValueBounds(pos + 1, 4);
        int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
        require(length >= 0, "negative binary length");
        checkValueBounds(pos + 5, length);
        return Arrays.copyOfRange(value, pos + 5, pos + 5 + length);
    }

    /**
     * Converts this value, and anything below it, to plain Java objects.
     *
     * <p>Integers become {@link Long} regardless of their stored width, and a scale-zero {@code decimal16} becomes a
     * {@link BigInteger}, so the result lines up with what an {@code XContentParser} produces for the same JSON. Without
     * that alignment the two arms would appear to disagree on every integer, and the comparison would be about boxing
     * rather than about storage.
     */
    public Object toJavaObject() {
        return toJavaObject(0);
    }

    /**
     * Guards against unbounded recursion. Corrupt offsets can point a container back at itself, which would otherwise
     * recurse until the stack overflows — an {@link Error} rather than a catchable exception, and a much worse failure
     * than a rejected value. Path lookups via {@link #objectGet} are naturally bounded by the path length and so are not
     * exposed to this; whole-value reconstruction is.
     */
    private static final int MAX_DEPTH = 1000;

    private Object toJavaObject(int depth) {
        if (depth > MAX_DEPTH) {
            throw new VariantFormatException("Variant nesting exceeds " + MAX_DEPTH + " levels, which suggests a cycle");
        }
        switch (type()) {
            case NULL:
                return null;
            case BOOLEAN:
                return getBoolean();
            case LONG:
                return getLong();
            case FLOAT:
                return getFloat();
            case DOUBLE:
                return getDouble();
            case DECIMAL: {
                BigDecimal decimal = getDecimal();
                return decimal.scale() == 0 ? decimal.toBigInteger() : decimal;
            }
            case STRING:
                return getString();
            case BINARY:
                return getBinary();
            case OBJECT: {
                int size = objectSize();
                Map<String, Object> map = new LinkedHashMap<>(Math.max(4, size * 2));
                for (int i = 0; i < size; i++) {
                    map.put(objectKeyAt(i), objectValueAt(i).toJavaObject(depth + 1));
                }
                return map;
            }
            case ARRAY: {
                int size = arraySize();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(arrayGet(i).toJavaObject(depth + 1));
                }
                return list;
            }
            default:
                throw new VariantFormatException("unhandled type " + type());
        }
    }

    // --------------------------------------------------------------- metadata

    public int dictionarySize() {
        return metadata.size();
    }

    public String dictionaryKey(int fieldId) {
        return metadata.key(fieldId);
    }

    public VariantMetadata metadata() {
        return metadata;
    }

    // ------------------------------------------------------------- re-encoding

    /**
     * Re-encodes this value with its dictionary supplied up front, so field ids follow {@code dictionary}'s order.
     *
     * <p>The general way to get field ids into a chosen order, and the way a Parquet writer does it: hand the encoder the
     * dictionary before anything is written, so every width is computed from the ids that will actually be stored.
     * {@link #relabelFieldIds} is the cheap alternative, but it can only patch ids into the width already written, which
     * bounds it to documents whose ids all fit that width. This has no such bound.
     *
     * <p>Only container framing is rebuilt. Scalars are copied byte for byte, so no value -- integer width, decimal scale,
     * string encoding -- can change in transit.
     *
     * @param dictionary the key names in the order field ids should refer to them; must contain every key this value uses
     * @return the re-encoded value bytes, paired with a dictionary in {@code dictionary}'s order
     */
    public byte[] reencodeWithDictionary(List<String> dictionary) {
        VariantBuilder builder = new VariantBuilder(valueEnd - valueBase);
        builder.presetDictionary(dictionary);
        copyInto(builder, pos, valueEnd - pos);
        return builder.finish().valueBytes();
    }

    /**
     * Copies the value at {@code at} into {@code builder}, rebuilding containers and copying scalars verbatim.
     */
    private void copyInto(VariantBuilder builder, int at, int length) {
        int basic = VariantEncoding.basicType(value[at]);
        if (basic == VariantEncoding.BASIC_OBJECT) {
            int[] layout = at(at).objectLayout();
            int numElements = layout[0];
            int fieldIdSize = layout[1];
            int fieldOffsetSize = layout[2];
            int fieldIdsStart = layout[3];
            int fieldOffsetsStart = layout[4];
            int valuesStart = layout[5];
            int[] extents = valueExtents(fieldOffsetsStart, fieldOffsetSize, numElements);
            builder.startObject();
            for (int i = 0; i < numElements; i++) {
                int fieldId = VariantEncoding.readUnsigned(value, fieldIdsStart + i * fieldIdSize, fieldIdSize);
                builder.appendKey(metadata.key(fieldId));
                int start = VariantEncoding.readUnsigned(value, fieldOffsetsStart + i * fieldOffsetSize, fieldOffsetSize);
                copyInto(builder, valuesStart + start, extentEnd(extents, start) - start);
            }
            builder.endObject();
        } else if (basic == VariantEncoding.BASIC_ARRAY) {
            int[] layout = at(at).arrayLayout();
            int numElements = layout[0];
            int fieldOffsetSize = layout[1];
            int fieldOffsetsStart = layout[2];
            int valuesStart = layout[3];
            int[] extents = valueExtents(fieldOffsetsStart, fieldOffsetSize, numElements);
            builder.startArray();
            for (int i = 0; i < numElements; i++) {
                int start = VariantEncoding.readUnsigned(value, fieldOffsetsStart + i * fieldOffsetSize, fieldOffsetSize);
                copyInto(builder, valuesStart + start, extentEnd(extents, start) - start);
            }
            builder.endArray();
        } else {
            require(at + length <= valueEnd, "scalar at " + at + " of " + length + " bytes runs past the value region");
            builder.appendRawValue(value, at, length);
        }
    }

    /**
     * A container's offset table, sorted, so that where one value <em>ends</em> can be found from where the next one starts.
     *
     * <p>Needed because an object's offset table is <b>not</b> monotonic. The format requires an object's entries to be
     * ordered by key <em>string</em>, while the values they point at sit wherever the writer put them -- for this encoder,
     * in the order the keys arrived. So entry {@code i + 1} is the next member <em>alphabetically</em>, not the next value
     * <em>physically</em>, and subtracting consecutive entries yields a nonsense length (often negative).
     *
     * <p>Sorting fixes it because the values tile the region contiguously: every offset is distinct, the table's trailing
     * entry is the region's total size, and so the next larger offset is exactly where the value at {@code start} ends.
     */
    private int[] valueExtents(int offsetsStart, int offsetSize, int numElements) {
        int[] offsets = new int[numElements + 1];
        for (int i = 0; i <= numElements; i++) {
            offsets[i] = VariantEncoding.readUnsigned(value, offsetsStart + i * offsetSize, offsetSize);
        }
        Arrays.sort(offsets);
        return offsets;
    }

    private int extentEnd(int[] extents, int start) {
        int index = Arrays.binarySearch(extents, start);
        require(index >= 0 && index + 1 < extents.length, "offset " + start + " is not an entry in the container's offset table");
        return extents[index + 1];
    }

    // ------------------------------------------------------------- relabelling

    /**
     * Rewrites every field id in this value and its descendants through {@code idMap}, in place.
     *
     * <p>The one write-side transform on this class, and it exists to let the key names live outside the value. A writer
     * that stores names in a separate sorted column wants field id {@code i} to mean the document's {@code i}-th smallest
     * name, because then a reader can resolve a name by indexing straight into the ordinal list that column hands it.
     * The encoder cannot assign ids that way while streaming -- a smaller name may still arrive -- so it assigns them in
     * insertion order and this remaps them afterwards.
     *
     * <p>Structure is untouched: only the field-id bytes change, at the width already written, so every offset in the
     * value stays valid. Members also stay in the order the encoder put them, which the spec requires to be by key
     * string; remapping consistently preserves that, and additionally leaves the ids ascending within each object.
     *
     * <p><b>This invalidates the metadata this value was paired with.</b> Field ids afterwards index whatever the caller's
     * map points at, not the dictionary {@link #metadata()} holds, so a caller that relabels must not read keys through
     * this cursor again.
     *
     * @param idMap new id for each old id, which must be a permutation for the result to be readable
     * @throws VariantFormatException if a field id falls outside {@code idMap}, or if its replacement does not fit the
     *                               width already written
     */
    public void relabelFieldIds(int[] idMap) {
        int basic = VariantEncoding.basicType(value[pos]);
        if (basic == VariantEncoding.BASIC_OBJECT) {
            int[] layout = objectLayout();
            int numElements = layout[0];
            int fieldIdSize = layout[1];
            int fieldOffsetSize = layout[2];
            int fieldIdsStart = layout[3];
            int fieldOffsetsStart = layout[4];
            int valuesStart = layout[5];
            for (int i = 0; i < numElements; i++) {
                int at = fieldIdsStart + i * fieldIdSize;
                int fieldId = VariantEncoding.readUnsigned(value, at, fieldIdSize);
                require(
                    fieldId >= 0 && fieldId < idMap.length,
                    "field id " + fieldId + " outside the " + idMap.length + " keys being relabelled"
                );
                int mapped = idMap[fieldId];
                // Widening a field id would move every byte after it, so the caller has to have chosen a map that fits.
                // Checked rather than assumed: silently truncating one id yields a value that this reader still parses and
                // that returns the wrong key, which is the failure mode hardest to notice.
                require(
                    fieldIdSize == 4 || (mapped >= 0 && mapped < (1 << (8 * fieldIdSize))),
                    "relabelled field id " + mapped + " does not fit the " + fieldIdSize + " byte(s) already written"
                );
                VariantEncoding.writeUnsigned(value, at, mapped, fieldIdSize);
            }
            for (int i = 0; i < numElements; i++) {
                int offset = VariantEncoding.readUnsigned(value, fieldOffsetsStart + i * fieldOffsetSize, fieldOffsetSize);
                at(valuesStart + offset).relabelFieldIds(idMap);
            }
        } else if (basic == VariantEncoding.BASIC_ARRAY) {
            int[] layout = arrayLayout();
            int numElements = layout[0];
            int fieldOffsetSize = layout[1];
            int fieldOffsetsStart = layout[2];
            int valuesStart = layout[3];
            for (int i = 0; i < numElements; i++) {
                int offset = VariantEncoding.readUnsigned(value, fieldOffsetsStart + i * fieldOffsetSize, fieldOffsetSize);
                at(valuesStart + offset).relabelFieldIds(idMap);
            }
        }
        // Primitives and short strings hold no field ids.
    }

    // ---------------------------------------------------------------- helpers

    /** The raw bytes of this value tree, for callers that need to persist it. */
    public byte[] valueBytes() {
        return valueBase == 0 && valueEnd == value.length ? value : Arrays.copyOfRange(value, valueBase, valueEnd);
    }

    /** Length of the value region, for callers writing it out without a copy. */
    public int valueLength() {
        return valueEnd - valueBase;
    }

    public byte[] valueArray() {
        return value;
    }

    public int valueOffset() {
        return valueBase;
    }

    public byte[] metadataBytes() {
        return metadata.bytes();
    }

    public int offset() {
        return pos;
    }

    private long readSigned(int offset, int width) {
        checkValueBounds(offset, width);
        long result = 0;
        for (int i = 0; i < width; i++) {
            result |= ((long) (value[offset + i] & 0xFF)) << (8 * i);
        }
        // Sign-extend from the top bit of the stored width.
        if (width < 8) {
            int shift = 64 - 8 * width;
            result = (result << shift) >> shift;
        }
        return result;
    }

    private void requireBasicType(int expected, String what) {
        if (VariantEncoding.basicType(value[pos]) != expected) {
            throw new VariantFormatException("value is not " + what);
        }
    }

    private void checkValueBounds(int offset, int length) {
        if (offset < valueBase || length < 0 || offset + length > valueEnd) {
            throw new VariantFormatException(
                "read of " + length + " bytes at " + offset + " exceeds value region [" + valueBase + ", " + valueEnd + ")"
            );
        }
    }

    private static void require(boolean condition, String message) {
        if (condition == false) {
            throw new VariantFormatException(message);
        }
    }
}
