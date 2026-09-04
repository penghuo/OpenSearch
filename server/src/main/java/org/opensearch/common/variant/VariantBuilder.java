/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

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
 * <p>The API mirrors a token stream so an encoder can be driven straight off an {@code XContentParser} without first
 * materialising the document as a tree. The mapper already walks every token to build the field's terms, so encoding
 * alongside that walk costs one pass; reading the subtree into a map first would cost two.
 *
 * <p><b>How objects get their header.</b> An object's header records its element count and the offset of every child, none
 * of which is known until the children have been written. The builder therefore writes children first, remembers where
 * each one started, and at {@link #endObject()} shifts them forward to open a gap for the header. Shifts are always at
 * the container's own start position, and every offset recorded so far refers to a position at or before that, so no
 * previously recorded offset is ever invalidated — including in the nested case.
 *
 * <p><b>Dictionary ordering.</b> {@code sorted_strings} is written as 0 and dictionary ids stay in insertion order.
 * Sorting the dictionary would mean rewriting every field id already emitted, for no read benefit: the spec requires each
 * object's field ids to be ordered by their <em>key strings</em> regardless, which is what makes the reader's binary
 * search work. That per-object ordering is applied at {@link #endObject()}.
 *
 * <p>Not thread safe. One builder encodes one value.
 *
 * @opensearch.internal
 */
public final class VariantBuilder {

    private byte[] buffer;
    private int pos;

    private final List<String> dictionaryKeys = new ArrayList<>();
    private final Map<String, Integer> dictionaryIds = new HashMap<>();

    /** Entries for every currently open container, in write order. Trimmed as containers close. */
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
     * <p>Package-private because nothing in production writes a shared dictionary: the blob column stores each document's
     * dictionary inline.
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

    // ------------------------------------------------------------ containers

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
                throw new IllegalStateException(
                    "duplicate object key [" + new String(members.get(i).keyBytes, StandardCharsets.UTF_8) + "]"
                );
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
        Frame frame = currentFrame();
        if (frame == null || frame.isObject == false) {
            throw new IllegalStateException("appendKey is only valid directly inside an object");
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        entries.add(new Entry(dictionaryId(key), keyBytes, pos - frame.valueStart));
    }

    // --------------------------------------------------------------- scalars

    public void appendNull() {
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_NULL);
    }

    public void appendBoolean(boolean value) {
        beforeValue();
        writePrimitiveHeader(value ? VariantEncoding.P_TRUE : VariantEncoding.P_FALSE);
    }

    /**
     * Appends an integer, narrowed to the smallest width that holds it.
     *
     * <p>Narrowing is deterministic on purpose. If the width depended on anything other than the value, two encodings of
     * the same document could differ, and a document's bytes would stop being a function of the document.
     */
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
     * Appends an integer too large for {@code int64} as a scale-zero decimal, which is the widest exact integer the
     * format offers. Values that do not fit in 16 bytes have no exact representation and are rejected rather than
     * silently rounded through a double.
     */
    public void appendBigInteger(BigInteger value) {
        if (value.bitLength() < 64) {
            appendLong(value.longValueExact());
            return;
        }
        byte[] magnitude = value.toByteArray();
        if (magnitude.length > 16) {
            throw new IllegalArgumentException("integer too large for Variant decimal16: " + value.bitLength() + " bits");
        }
        beforeValue();
        writePrimitiveHeader(VariantEncoding.P_DECIMAL16);
        ensure(1 + 16);
        buffer[pos++] = 0; // scale
        // toByteArray is big-endian two's complement; Variant decimals are little-endian, so reverse and sign-extend.
        byte fill = (byte) (value.signum() < 0 ? 0xFF : 0x00);
        for (int i = 0; i < 16; i++) {
            buffer[pos + i] = i < magnitude.length ? magnitude[magnitude.length - 1 - i] : fill;
        }
        pos += 16;
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

    // ---------------------------------------------------------------- finish

    /**
     * Completes the value and returns it.
     *
     * @throws IllegalStateException if a container was left open
     */
    public Variant finish() {
        if (frames.isEmpty() == false) {
            throw new IllegalStateException(frames.size() + " container(s) left open");
        }
        return new Variant(buildMetadata(), Arrays.copyOf(buffer, pos), 0);
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

    // --------------------------------------------------------------- helpers

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
