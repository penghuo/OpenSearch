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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Streaming encoder for the Variant binary format.
 *
 * <p>Container children are written before their headers; closing a container inserts its header and sorts object members
 * by key. Dictionary ids use insertion order.
 *
 * <p>Not thread safe. One builder encodes one value.
 *
 * @opensearch.internal
 */
@InternalApi
public final class VariantBuilder {

    private byte[] buffer;
    private int pos;

    private final List<String> dictionaryKeys = new ArrayList<>();
    private final Map<String, Integer> dictionaryIds = new HashMap<>();

    private final List<Entry> entries = new ArrayList<>();
    private final List<Frame> frames = new ArrayList<>();

    public VariantBuilder() {
        this(128);
    }

    public VariantBuilder(int initialCapacity) {
        this.buffer = new byte[Math.max(16, initialCapacity)];
    }

    /**
     * Seeds the dictionary with keys that already have ids, so field ids refer to a dictionary shared beyond this one
     * value rather than to a per-value one.
     *
     * <p>Package-private because nothing in production seeds one. The mapper does not store a dictionary in the value at all:
     * a document's key names go to their own sorted column, and its field ids index that document's ordinals into it. This
     * exists so tests and format tooling can build a value whose ids refer to a dictionary they supply.
     *
     * @throws IllegalStateException if anything has already been appended, which would leave earlier ids pointing at the
     *                               wrong keys
     */
    void presetDictionary(List<String> keys) {
        if (dictionaryKeys.isEmpty() == false || pos != 0) {
            throw new IllegalStateException("the dictionary must be seeded before anything is appended");
        }
        for (String key : keys) {
            dictionaryId(key);
        }
    }

    private static final class Entry {
        final int fieldId;
        final byte[] keyBytes;
        final int offset;

        Entry(int fieldId, byte[] keyBytes, int offset) {
            this.fieldId = fieldId;
            this.keyBytes = keyBytes;
            this.offset = offset;
        }
    }

    static final class DuplicateObjectKeyException extends IllegalStateException {
        DuplicateObjectKeyException(String key) {
            super("duplicate object key [" + key + "]");
        }
    }

    private static final class Frame {
        final boolean isObject;
        final int valueStart;
        final int entryStart;

        Frame(boolean isObject, int valueStart, int entryStart) {
            this.isObject = isObject;
            this.valueStart = valueStart;
            this.entryStart = entryStart;
        }
    }

    public void startObject() {
        beforeValue();
        frames.add(new Frame(true, pos, entries.size()));
    }

    public void endObject() {
        Frame frame = popFrame(true);
        List<Entry> members = new ArrayList<>(entries.subList(frame.entryStart, entries.size()));
        entries.subList(frame.entryStart, entries.size()).clear();

        // The spec requires field ids ordered by key string, using unsigned byte ordering over UTF-8. Java's
        // String.compareTo orders by UTF-16 code unit, which differs for supplementary characters, so compare bytes.
        members.sort((a, b) -> compareUnsigned(a.keyBytes, b.keyBytes));
        for (int i = 1; i < members.size(); i++) {
            if (compareUnsigned(members.get(i - 1).keyBytes, members.get(i).keyBytes) == 0) {
                throw new DuplicateObjectKeyException(new String(members.get(i).keyBytes, StandardCharsets.UTF_8));
            }
        }

        int numElements = members.size();
        int dataSize = pos - frame.valueStart;
        boolean isLarge = numElements > VariantEncoding.MAX_SMALL_ELEMENT_COUNT;

        int maxFieldId = 0;
        for (Entry entry : members) {
            maxFieldId = Math.max(maxFieldId, entry.fieldId);
        }
        int fieldIdSize = VariantEncoding.minUnsignedWidth(maxFieldId);
        int fieldOffsetSize = VariantEncoding.minUnsignedWidth(dataSize);

        int headerSize = 1 + (isLarge ? 4 : 1) + numElements * fieldIdSize + (numElements + 1) * fieldOffsetSize;
        openGap(frame.valueStart, dataSize, headerSize);

        int at = frame.valueStart;
        int valueHeader = (isLarge ? 1 << VariantEncoding.OBJ_IS_LARGE_SHIFT : 0) | ((fieldIdSize - 1)
            << VariantEncoding.OBJ_FIELD_ID_SIZE_SHIFT) | ((fieldOffsetSize - 1) << VariantEncoding.OBJ_FIELD_OFFSET_SIZE_SHIFT);
        buffer[at++] = VariantEncoding.valueMetadata(VariantEncoding.BASIC_OBJECT, valueHeader);
        at = writeElementCount(at, numElements, isLarge);
        for (Entry entry : members) {
            VariantEncoding.writeUnsigned(buffer, at, entry.fieldId, fieldIdSize);
            at += fieldIdSize;
        }
        for (Entry entry : members) {
            VariantEncoding.writeUnsigned(buffer, at, entry.offset, fieldOffsetSize);
            at += fieldOffsetSize;
        }
        VariantEncoding.writeUnsigned(buffer, at, dataSize, fieldOffsetSize);

        pos += headerSize;
    }

    public void startArray() {
        beforeValue();
        frames.add(new Frame(false, pos, entries.size()));
    }

    public void endArray() {
        Frame frame = popFrame(false);
        List<Entry> elements = new ArrayList<>(entries.subList(frame.entryStart, entries.size()));
        entries.subList(frame.entryStart, entries.size()).clear();

        int numElements = elements.size();
        int dataSize = pos - frame.valueStart;
        boolean isLarge = numElements > VariantEncoding.MAX_SMALL_ELEMENT_COUNT;
        int fieldOffsetSize = VariantEncoding.minUnsignedWidth(dataSize);

        int headerSize = 1 + (isLarge ? 4 : 1) + (numElements + 1) * fieldOffsetSize;
        openGap(frame.valueStart, dataSize, headerSize);

        int at = frame.valueStart;
        int valueHeader = (isLarge ? 1 << VariantEncoding.ARR_IS_LARGE_SHIFT : 0) | ((fieldOffsetSize - 1)
            << VariantEncoding.ARR_FIELD_OFFSET_SIZE_SHIFT);
        buffer[at++] = VariantEncoding.valueMetadata(VariantEncoding.BASIC_ARRAY, valueHeader);
        at = writeElementCount(at, numElements, isLarge);
        for (Entry element : elements) {
            VariantEncoding.writeUnsigned(buffer, at, element.offset, fieldOffsetSize);
            at += fieldOffsetSize;
        }
        VariantEncoding.writeUnsigned(buffer, at, dataSize, fieldOffsetSize);

        pos += headerSize;
    }

    /**
     * Declares the key of the next object member. Must be followed immediately by exactly one value.
     */
    public void appendKey(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        appendKey(dictionaryId(key), keyBytes);
    }

    /**
     * Declares a key whose final dictionary id and UTF-8 bytes are already known.
     *
     * <p>Used by deferred encoding when names live outside the value, so rebuilding an inline dictionary would be wasted
     * work. The caller must ensure ids and bytes describe the same external dictionary.
     */
    void appendKey(int fieldId, byte[] keyBytes) {
        Frame frame = currentFrame();
        if (frame == null || frame.isObject == false) {
            throw new IllegalStateException("appendKey is only valid directly inside an object");
        }
        entries.add(new Entry(fieldId, keyBytes, pos - frame.valueStart));
    }

    public void appendNull() {
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_NULL);
    }

    public void appendBoolean(boolean value) {
        beforeValue();
        writePrimitiveHeader(value ? VariantEncoding.P_TRUE : VariantEncoding.P_FALSE);
    }

    /** Appends an integer using the smallest width that holds it. */
    public void appendLong(long value) {
        beforeValue();
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            writePrimitiveHeader(VariantEncoding.P_INT8);
            ensure(1);
            buffer[pos++] = (byte) value;
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            writePrimitiveHeader(VariantEncoding.P_INT16);
            writeLittleEndian(value, 2);
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            writePrimitiveHeader(VariantEncoding.P_INT32);
            writeLittleEndian(value, 4);
        } else {
            writePrimitiveHeader(VariantEncoding.P_INT64);
            writeLittleEndian(value, 8);
        }
    }

    public void appendDouble(double value) {
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_DOUBLE);
        writeLittleEndian(Double.doubleToRawLongBits(value), 8);
    }

    public void appendFloat(float value) {
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_FLOAT);
        writeLittleEndian(Float.floatToRawIntBits(value) & 0xFFFFFFFFL, 4);
    }

    /**
     * Set when a value was reached that the encoding cannot hold exactly.
     *
     * <p>A flag rather than an exception, because the caller has to keep walking: {@code flat_object} indexes the document's
     * terms whatever happens, so the walk must finish even once the blob is known to be unusable. What must not happen is
     * the blob being persisted as if it were the value -- a JSON number silently becoming a string, say -- so the writer
     * checks this and stores its unavailable sentinel instead.
     */
    private boolean unencodable;

    /**
     * Records that this value cannot be represented exactly, so whatever bytes are built must not be persisted as the value.
     */
    public void markUnencodable() {
        this.unencodable = true;
    }

    public boolean isUnencodable() {
        return unencodable;
    }

    /**
     * Most decimal digits any Variant decimal type can hold.
     *
     * <p>A bound on <em>digits</em>, not on bytes. A signed 16-byte integer reaches 39 decimal digits, which the encoding
     * does not allow, so a byte-width test would accept values a conforming reader must reject.
     */
    public static final int MAX_DECIMAL_PRECISION = 38;

    private static final BigInteger UNSCALED_LIMIT = BigInteger.TEN.pow(MAX_DECIMAL_PRECISION);

    /**
     * Appends an integer too large for {@code int64} as a scale-zero decimal, which is the widest exact integer the format
     * offers. Values needing more than {@link #MAX_DECIMAL_PRECISION} digits have no exact representation and are rejected
     * rather than silently rounded through a double.
     */
    public void appendBigInteger(BigInteger value) {
        if (value.bitLength() < 64) {
            appendLong(value.longValueExact());
            return;
        }
        if (canRepresentExactly(value) == false) {
            throw new IllegalArgumentException("integer needs more than " + MAX_DECIMAL_PRECISION + " decimal digits: " + value);
        }
        // bitLength >= 64 means a magnitude of at least 2^63, which is 19 digits, so only decimal16 can hold it.
        writeDecimal(value, 0, 16);
    }

    /**
     * Appends a decimal exactly, at the narrowest width that holds it.
     *
     * <p>Not routed through {@code appendDouble}. A double keeps 15--17 significant decimal digits, so a value such as a
     * 20-digit price is changed by the round trip and the change is invisible afterwards -- the column would hand back a
     * number the document does not contain. The encoding has exact decimal types precisely so that does not have to happen.
     *
     * <p><b>Width by precision, not by bytes.</b> The format bounds each width by digits: {@code decimal4} takes 9,
     * {@code decimal8} takes 18, {@code decimal16} takes {@value #MAX_DECIMAL_PRECISION}. Choosing by bit length instead
     * would put {@code 1000000000} -- ten digits, but only 30 bits -- into a {@code decimal4} that is not allowed to hold it.
     *
     * <p><b>Scale is canonicalised, the number is not.</b> {@code 1.2000} is stored as {@code 1.2} and {@code 1E+2} as
     * {@code 100}: trailing zeros carry no numeric information and dropping them is what lets both fit. So a read returns a
     * {@code BigDecimal} equal to the original under {@code compareTo}, though not necessarily under {@code equals}.
     *
     * @throws IllegalArgumentException if no exact representation exists, so a caller can choose a lossless fallback rather
     *                                  than have precision discarded here
     */
    public void appendBigDecimal(BigDecimal value) {
        BigDecimal exact = canonicalise(value);
        if (exact == null) {
            throw new IllegalArgumentException(
                "decimal cannot be represented exactly by the Variant encoding: scale " + value.scale() + ", precision " + value.precision()
            );
        }
        // Width by precision, and the same three bounds the decoder enforces.
        int precision = exact.precision();
        int width = precision <= 9 ? 4 : precision <= 18 ? 8 : 16;
        writeDecimal(exact.unscaledValue(), exact.scale(), width);
    }

    /**
     * Whether {@link #appendBigDecimal} would keep {@code value} exactly.
     *
     * <p>Offered separately so a caller can pick a lossless fallback before any bytes are written. It canonicalises by the
     * same rule the append does, so the two cannot disagree.
     */
    public static boolean canRepresentExactly(BigDecimal value) {
        return canonicalise(value) != null;
    }

    /** Whether {@link #appendBigInteger} would keep {@code value} exactly. */
    public static boolean canRepresentExactly(BigInteger value) {
        return value.abs().compareTo(UNSCALED_LIMIT) < 0;
    }

    /**
     * The number of decimal digits in an unscaled value, which is what every Variant decimal width is bounded by.
     *
     * <p>Shared with the decoder so a value this class refuses to write is also one the decoder refuses to read.
     */
    public static int decimalPrecisionOf(BigInteger unscaled) {
        return new BigDecimal(unscaled).precision();
    }

    /**
     * @return {@code value} in the form that will actually be encoded, or {@code null} if there is no exact one
     */
    private static BigDecimal canonicalise(BigDecimal value) {
        BigDecimal stripped = value.stripTrailingZeros();
        if (stripped.scale() < 0) {
            // A negative scale is an exponent, as in 1E+2. That is exactly 100 at scale 0, and setScale cannot fail here
            // because moving a negative scale up to zero only appends zeros.
            stripped = stripped.setScale(0);
        }
        if (stripped.scale() > MAX_DECIMAL_PRECISION) {
            return null;
        }
        if (stripped.precision() > MAX_DECIMAL_PRECISION) {
            return null;
        }
        return stripped;
    }

    /** Writes one decimal of the given byte width, little-endian and sign-extended. */
    private void writeDecimal(BigInteger unscaled, int scale, int width) {
        byte[] magnitude = unscaled.toByteArray();
        assert magnitude.length <= width : "precision was checked, so the unscaled value must fit " + width + " bytes";
        beforeValue();
        int typeId = width == 4 ? VariantEncoding.P_DECIMAL4 : width == 8 ? VariantEncoding.P_DECIMAL8 : VariantEncoding.P_DECIMAL16;
        writePrimitiveHeader(typeId);
        ensure(1 + width);
        buffer[pos++] = (byte) scale;
        // toByteArray is big-endian two's complement; Variant decimals are little-endian, so reverse and sign-extend.
        byte fill = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
        for (int i = 0; i < width; i++) {
            buffer[pos + i] = i < magnitude.length ? magnitude[magnitude.length - 1 - i] : fill;
        }
        pos += width;
    }

    public void appendString(String value) {
        beforeValue();
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        if (utf8.length <= VariantEncoding.MAX_SHORT_STRING_LEN) {
            ensure(1 + utf8.length);
            buffer[pos++] = VariantEncoding.valueMetadata(VariantEncoding.BASIC_SHORT_STRING, utf8.length);
        } else {
            writePrimitiveHeader(VariantEncoding.P_STRING);
            writeLittleEndian(utf8.length, 4);
            ensure(utf8.length);
        }
        System.arraycopy(utf8, 0, buffer, pos, utf8.length);
        pos += utf8.length;
    }

    /**
     * Appends an already-encoded value verbatim.
     *
     * <p>For copying a subtree from one value into another when only the framing needs to change. Going through the typed
     * {@code append*} methods instead would decode and re-encode every scalar, and the format has several exact-numeric
     * types whose width or scale a round trip through a Java value would not preserve. Copying the bytes cannot lose
     * anything.
     *
     * <p>The caller is responsible for {@code [offset, offset + length)} being exactly one well-formed value, which is why
     * this is package-private: the only caller reads those bounds out of an enclosing container's offset table.
     */
    void appendRawValue(byte[] source, int offset, int length) {
        beforeValue();
        ensure(length);
        System.arraycopy(source, offset, buffer, pos, length);
        pos += length;
    }

    public void appendBinary(byte[] value) {
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_BINARY);
        writeLittleEndian(value.length, 4);
        ensure(value.length);
        System.arraycopy(value, 0, buffer, pos, value.length);
        pos += value.length;
    }

    /**
     * Completes the value and returns it.
     *
     * @throws IllegalStateException if a container was left open
     */
    public Variant finish() {
        ensureComplete();
        return new Variant(buildMetadata(), Arrays.copyOf(buffer, pos), 0);
    }

    /**
     * Completes the value without constructing inline metadata.
     *
     * <p>Used when field ids already refer to a dictionary stored outside the Variant value.
     */
    byte[] finishValueBytes() {
        ensureComplete();
        return Arrays.copyOf(buffer, pos);
    }

    /**
     * This value's key names, in the order field ids refer to them.
     *
     * <p>Exposed so a caller can store the names somewhere other than inside the value. The order is load-bearing: entry
     * {@code i} is the name that field id {@code i} means, so a reader keeping the names elsewhere must preserve the
     * mapping from position to name.
     */
    public List<String> dictionaryKeys() {
        return Collections.unmodifiableList(dictionaryKeys);
    }

    private byte[] buildMetadata() {
        int count = dictionaryKeys.size();
        byte[][] keyBytes = new byte[count][];
        int totalBytes = 0;
        for (int i = 0; i < count; i++) {
            keyBytes[i] = dictionaryKeys.get(i).getBytes(StandardCharsets.UTF_8);
            totalBytes += keyBytes[i].length;
        }
        // dictionary_size is itself written with offset_size bytes, so the width must hold both it and the largest offset.
        int offsetSize = VariantEncoding.minUnsignedWidth(Math.max(totalBytes, count));

        byte[] metadata = new byte[1 + offsetSize + (count + 1) * offsetSize + totalBytes];
        int at = 0;
        metadata[at++] = (byte) (VariantEncoding.VERSION | ((offsetSize - 1) << VariantEncoding.OFFSET_SIZE_SHIFT));
        VariantEncoding.writeUnsigned(metadata, at, count, offsetSize);
        at += offsetSize;

        int running = 0;
        for (int i = 0; i < count; i++) {
            VariantEncoding.writeUnsigned(metadata, at, running, offsetSize);
            at += offsetSize;
            running += keyBytes[i].length;
        }
        VariantEncoding.writeUnsigned(metadata, at, running, offsetSize);
        at += offsetSize;

        for (int i = 0; i < count; i++) {
            System.arraycopy(keyBytes[i], 0, metadata, at, keyBytes[i].length);
            at += keyBytes[i].length;
        }
        return metadata;
    }

    private void ensureComplete() {
        if (frames.isEmpty() == false) {
            throw new IllegalStateException(frames.size() + " container(s) left open");
        }
    }

    private int dictionaryId(String key) {
        Integer existing = dictionaryIds.get(key);
        if (existing != null) {
            return existing;
        }
        int id = dictionaryKeys.size();
        dictionaryKeys.add(key);
        dictionaryIds.put(key, id);
        return id;
    }

    /**
     * Records an element offset when the enclosing container is an array. Object members get their offset from
     * {@link #appendKey}, which runs immediately before the value.
     */
    private void beforeValue() {
        Frame frame = currentFrame();
        if (frame != null && frame.isObject == false) {
            entries.add(new Entry(-1, null, pos - frame.valueStart));
        }
    }

    private Frame currentFrame() {
        return frames.isEmpty() ? null : frames.get(frames.size() - 1);
    }

    private Frame popFrame(boolean expectObject) {
        Frame frame = currentFrame();
        if (frame == null) {
            throw new IllegalStateException("no open container to close");
        }
        if (frame.isObject != expectObject) {
            throw new IllegalStateException("mismatched container: tried to close " + (expectObject ? "an object" : "an array"));
        }
        frames.remove(frames.size() - 1);
        return frame;
    }

    private int writeElementCount(int at, int numElements, boolean isLarge) {
        if (isLarge) {
            VariantEncoding.writeUnsigned(buffer, at, numElements, 4);
            return at + 4;
        }
        buffer[at] = (byte) numElements;
        return at + 1;
    }

    /**
     * Shifts {@code length} bytes at {@code start} forward by {@code gap}, growing the buffer if needed.
     */
    private void openGap(int start, int length, int gap) {
        ensure(gap);
        System.arraycopy(buffer, start, buffer, start + gap, length);
    }

    private void writePrimitiveHeader(int primitiveTypeId) {
        ensure(1);
        buffer[pos++] = VariantEncoding.valueMetadata(VariantEncoding.BASIC_PRIMITIVE, primitiveTypeId);
    }

    private void writeLittleEndian(long value, int width) {
        ensure(width);
        for (int i = 0; i < width; i++) {
            buffer[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
        pos += width;
    }

    private void ensure(int additional) {
        if (pos + additional > buffer.length) {
            int target = Math.max(buffer.length * 2, pos + additional);
            buffer = Arrays.copyOf(buffer, target);
        }
    }

    static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }
}
