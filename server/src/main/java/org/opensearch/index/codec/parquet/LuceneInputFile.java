/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Adapts a Lucene {@link Directory} to Parquet's {@link InputFile} interface,
 * keeping all I/O within Lucene's Directory abstraction so it works with
 * local filesystem, remote store, and any other Directory implementation.
 *
 * Each call to {@link #newStream()} opens a new {@link IndexInput} clone,
 * allowing multiple concurrent readers over the same file.
 *
 * @opensearch.experimental
 */
public class LuceneInputFile implements InputFile {

    private final Directory directory;
    private final String fileName;
    private final IOContext context;

    public LuceneInputFile(Directory directory, String fileName, IOContext context) {
        this.directory = directory;
        this.fileName = fileName;
        this.context = context;
    }

    @Override
    public long getLength() throws IOException {
        return directory.fileLength(fileName);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new IndexInputSeekableInputStream(directory.openInput(fileName, context));
    }

    /**
     * Bridges Lucene's {@link IndexInput} to Parquet's {@link SeekableInputStream}.
     */
    static class IndexInputSeekableInputStream extends SeekableInputStream {

        private final IndexInput input;

        IndexInputSeekableInputStream(IndexInput input) {
            this.input = input;
        }

        @Override
        public long getPos() throws IOException {
            return input.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException {
            input.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            if (input.getFilePointer() >= input.length()) {
                return -1;
            }
            return Byte.toUnsignedInt(input.readByte());
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            long remaining = input.length() - input.getFilePointer();
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            input.readBytes(b, off, toRead);
            return toRead;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int offset, int len) throws IOException {
            long remaining = input.length() - input.getFilePointer();
            if (remaining < len) {
                throw new EOFException("Reached end of stream with " + remaining + " bytes remaining, needed " + len);
            }
            input.readBytes(bytes, offset, len);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            long remaining = input.length() - input.getFilePointer();
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(buf.remaining(), remaining);
            if (toRead == 0) {
                return 0;
            }
            if (buf.hasArray()) {
                input.readBytes(buf.array(), buf.arrayOffset() + buf.position(), toRead);
                buf.position(buf.position() + toRead);
            } else {
                byte[] tmp = new byte[toRead];
                input.readBytes(tmp, 0, toRead);
                buf.put(tmp);
            }
            return toRead;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int needed = buf.remaining();
            long remaining = input.length() - input.getFilePointer();
            if (remaining < needed) {
                throw new EOFException("Reached end of stream with " + remaining + " bytes remaining, needed " + needed);
            }
            if (buf.hasArray()) {
                input.readBytes(buf.array(), buf.arrayOffset() + buf.position(), needed);
                buf.position(buf.position() + needed);
            } else {
                byte[] tmp = new byte[needed];
                input.readBytes(tmp, 0, needed);
                buf.put(tmp);
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
