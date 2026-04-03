/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.spark;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * A {@link SeekableInputStream} that reads from a window within a file.
 * All positions are shifted by {@code offset} so the Parquet content
 * appears to start at position 0.
 */
public class PdvdSeekableInputStream extends SeekableInputStream {

    private final RandomAccessFile raf;
    private final long offset;
    private final long length;

    public PdvdSeekableInputStream(Path path, long offset, long length) throws IOException {
        this.raf = new RandomAccessFile(path.toFile(), "r");
        this.offset = offset;
        this.length = length;
        this.raf.seek(offset);
    }

    @Override
    public long getPos() throws IOException {
        return raf.getFilePointer() - offset;
    }

    @Override
    public void seek(long newPos) throws IOException {
        if (newPos < 0 || newPos > length) {
            throw new IOException("Seek position " + newPos + " out of range [0, " + length + "]");
        }
        raf.seek(newPos + offset);
    }

    @Override
    public int read() throws IOException {
        if (getPos() >= length) {
            return -1;
        }
        return raf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long pos = getPos();
        if (pos >= length) {
            return -1;
        }
        int toRead = (int) Math.min(len, length - pos);
        return raf.read(b, off, toRead);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        int remaining = len;
        int off = start;
        while (remaining > 0) {
            int n = read(bytes, off, remaining);
            if (n < 0) {
                throw new EOFException("Reached end of stream with " + remaining + " bytes remaining");
            }
            off += n;
            remaining -= n;
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        byte[] tmp = new byte[buf.remaining()];
        int n = read(tmp, 0, tmp.length);
        if (n > 0) {
            buf.put(tmp, 0, n);
        }
        return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        byte[] tmp = new byte[buf.remaining()];
        readFully(tmp);
        buf.put(tmp);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
