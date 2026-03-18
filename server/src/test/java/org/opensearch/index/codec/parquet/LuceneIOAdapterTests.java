/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for {@link LuceneOutputFile} and {@link LuceneInputFile}, the Parquet I/O adapters
 * that bridge Lucene's Directory-based I/O to Parquet's stream interfaces.
 *
 * These adapters are foundational to the Parquet DocValues format: all Parquet encoding/decoding
 * flows through them, so correctness here is critical for data integrity. The adapters must
 * faithfully translate between Lucene's IndexOutput/IndexInput and Parquet's
 * PositionOutputStream/SeekableInputStream without data loss or position tracking errors.
 */
public class LuceneIOAdapterTests extends OpenSearchTestCase {

    /**
     * Verifies that bytes written through LuceneOutputFile's PositionOutputStream
     * can be read back through LuceneInputFile's SeekableInputStream, confirming
     * the full round-trip through Lucene's Directory abstraction works correctly.
     */
    public void testRoundTrip() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = randomByteArrayOfLength(between(1, 8192));

            // Write via LuceneOutputFile
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "test.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            // Read via LuceneInputFile
            LuceneInputFile inputFile = new LuceneInputFile(dir, "test.bin", IOContext.DEFAULT);
            assertEquals(data.length, inputFile.getLength());

            try (SeekableInputStream in = inputFile.newStream()) {
                byte[] result = new byte[data.length];
                in.readFully(result);
                assertArrayEquals(data, result);
            }
        }
    }

    /**
     * Verifies that getPos() accurately tracks the write position as bytes are written.
     * Parquet encoders rely on position tracking to build page/column chunk offsets
     * in the file footer — incorrect positions would corrupt the Parquet file structure.
     */
    public void testOutputPositionTracking() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "pos.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                assertEquals(0, out.getPos());

                out.write(42);
                assertEquals(1, out.getPos());

                byte[] chunk = new byte[100];
                out.write(chunk);
                assertEquals(101, out.getPos());

                out.write(chunk, 10, 50);
                assertEquals(151, out.getPos());
            }
        }
    }

    /**
     * Verifies that seek() and getPos() work correctly on the input stream.
     * Parquet readers seek to specific offsets to read column chunks and pages —
     * broken seek would read wrong data from the file.
     */
    public void testInputSeekAndPosition() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[256];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }

            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "seek.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "seek.bin", IOContext.DEFAULT);
            try (SeekableInputStream in = inputFile.newStream()) {
                assertEquals(0, in.getPos());

                // Read first byte
                assertEquals(0, in.read());
                assertEquals(1, in.getPos());

                // Seek to middle
                in.seek(128);
                assertEquals(128, in.getPos());
                assertEquals(128, in.read());

                // Seek back to start
                in.seek(0);
                assertEquals(0, in.getPos());
                assertEquals(0, in.read());

                // Seek to end
                in.seek(256);
                assertEquals(256, in.getPos());
                assertEquals(-1, in.read()); // EOF
            }
        }
    }

    /**
     * Verifies that readFully(byte[]) throws EOFException when there aren't enough
     * bytes remaining. Parquet decoders use readFully and expect EOFException on
     * truncated files — silent short reads would cause data corruption.
     */
    public void testReadFullyThrowsOnEOF() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[10];
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "short.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "short.bin", IOContext.DEFAULT);
            try (SeekableInputStream in = inputFile.newStream()) {
                byte[] buf = new byte[20]; // more than available
                expectThrows(EOFException.class, () -> in.readFully(buf));
            }
        }
    }

    /**
     * Verifies ByteBuffer-based reads work correctly for both heap and direct buffers.
     * Parquet's column reader uses ByteBuffer reads for efficiency — both heap-backed
     * and direct ByteBuffers must work since the caller controls allocation.
     */
    public void testByteBufferRead() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }

            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "bb.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "bb.bin", IOContext.DEFAULT);

            // Test heap ByteBuffer
            try (SeekableInputStream in = inputFile.newStream()) {
                ByteBuffer heapBuf = ByteBuffer.allocate(50);
                int read = in.read(heapBuf);
                assertEquals(50, read);
                heapBuf.flip();
                for (int i = 0; i < 50; i++) {
                    assertEquals((byte) i, heapBuf.get());
                }
            }

            // Test direct ByteBuffer
            try (SeekableInputStream in = inputFile.newStream()) {
                ByteBuffer directBuf = ByteBuffer.allocateDirect(50);
                int read = in.read(directBuf);
                assertEquals(50, read);
                directBuf.flip();
                for (int i = 0; i < 50; i++) {
                    assertEquals((byte) i, directBuf.get());
                }
            }
        }
    }

    /**
     * Verifies readFully(ByteBuffer) fills the buffer completely and throws
     * EOFException if there aren't enough bytes. This is the primary read path
     * for Parquet page data — partial reads would corrupt decoded values.
     */
    public void testByteBufferReadFully() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }

            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "bbfull.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "bbfull.bin", IOContext.DEFAULT);

            // Successful readFully
            try (SeekableInputStream in = inputFile.newStream()) {
                ByteBuffer buf = ByteBuffer.allocate(100);
                in.readFully(buf);
                assertEquals(0, buf.remaining());
                buf.flip();
                for (int i = 0; i < 100; i++) {
                    assertEquals((byte) i, buf.get());
                }
            }

            // readFully past EOF
            try (SeekableInputStream in = inputFile.newStream()) {
                ByteBuffer buf = ByteBuffer.allocate(200);
                expectThrows(EOFException.class, () -> in.readFully(buf));
            }
        }
    }

    /**
     * Verifies that read() returns -1 at EOF and read(byte[], off, len) returns -1 at EOF.
     * Standard InputStream contract requires -1 at EOF — Parquet readers use this
     * to detect end of data.
     */
    public void testEOFBehavior() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = { 1, 2, 3 };
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "eof.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "eof.bin", IOContext.DEFAULT);
            try (SeekableInputStream in = inputFile.newStream()) {
                assertEquals(1, in.read());
                assertEquals(2, in.read());
                assertEquals(3, in.read());
                assertEquals(-1, in.read()); // EOF

                byte[] buf = new byte[10];
                assertEquals(-1, in.read(buf, 0, 10)); // EOF
            }
        }
    }

    /**
     * Verifies that multiple independent streams can be opened from the same InputFile.
     * Parquet readers may open multiple streams for parallel column reads —
     * each stream must maintain its own independent position.
     */
    public void testMultipleStreams() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }

            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "multi.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "multi.bin", IOContext.DEFAULT);
            try (SeekableInputStream s1 = inputFile.newStream(); SeekableInputStream s2 = inputFile.newStream()) {
                // Read from s1
                s1.seek(50);
                assertEquals(50, s1.read());

                // s2 should still be at position 0
                assertEquals(0, s2.getPos());
                assertEquals(0, s2.read());
            }
        }
    }

    /**
     * Verifies OutputFile metadata methods return correct values.
     * supportsBlockSize() must return false since Lucene directories don't expose block sizes.
     * getPath() must return the file name for Parquet metadata/error messages.
     */
    public void testOutputFileMetadata() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "meta.bin", IOContext.DEFAULT);
            assertFalse(outputFile.supportsBlockSize());
            assertEquals(0, outputFile.defaultBlockSize());
            assertEquals("meta.bin", outputFile.getPath());
        }
    }

    /**
     * Verifies that createOrOverwrite behaves the same as create, since Lucene's
     * Directory.createOutput always overwrites existing files.
     */
    public void testCreateOrOverwrite() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "overwrite.bin", IOContext.DEFAULT);

            // Write initial data
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(new byte[] { 1, 2, 3 });
            }

            // Overwrite with different data
            try (PositionOutputStream out = outputFile.createOrOverwrite(0)) {
                out.write(new byte[] { 4, 5 });
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "overwrite.bin", IOContext.DEFAULT);
            assertEquals(2, inputFile.getLength());
            try (SeekableInputStream in = inputFile.newStream()) {
                assertEquals(4, in.read());
                assertEquals(5, in.read());
                assertEquals(-1, in.read());
            }
        }
    }

    /**
     * Verifies partial reads via read(byte[], off, len) when fewer bytes remain
     * than requested. Parquet buffered readers may request more bytes than available
     * and rely on the return value to know how many were actually read.
     */
    public void testPartialRead() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            byte[] data = new byte[10];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i + 1);
            }

            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "partial.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                out.write(data);
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "partial.bin", IOContext.DEFAULT);
            try (SeekableInputStream in = inputFile.newStream()) {
                in.seek(5);
                byte[] buf = new byte[20];
                int read = in.read(buf, 0, 20);
                assertEquals(5, read); // only 5 bytes remaining
                assertEquals(6, buf[0]);
                assertEquals(10, buf[4]);
            }
        }
    }

    /**
     * Verifies that writing single bytes via write(int) works correctly.
     * This is the basic OutputStream contract that Parquet encoders may use
     * for writing individual bytes in headers and metadata.
     */
    public void testSingleByteWrite() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            LuceneOutputFile outputFile = new LuceneOutputFile(dir, "single.bin", IOContext.DEFAULT);
            try (PositionOutputStream out = outputFile.create(0)) {
                for (int i = 0; i < 256; i++) {
                    out.write(i);
                }
            }

            LuceneInputFile inputFile = new LuceneInputFile(dir, "single.bin", IOContext.DEFAULT);
            assertEquals(256, inputFile.getLength());
            try (SeekableInputStream in = inputFile.newStream()) {
                for (int i = 0; i < 256; i++) {
                    assertEquals(i, in.read());
                }
                assertEquals(-1, in.read());
            }
        }
    }
}
