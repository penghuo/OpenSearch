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
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

/**
 * Adapts a Lucene {@link Directory} to Parquet's {@link OutputFile} interface,
 * keeping all I/O within Lucene's Directory abstraction so it works with
 * local filesystem, remote store, and any other Directory implementation.
 *
 * @opensearch.experimental
 */
public class LuceneOutputFile implements OutputFile {

    private final Directory directory;
    private final String fileName;
    private final IOContext context;

    public LuceneOutputFile(Directory directory, String fileName, IOContext context) {
        this.directory = directory;
        this.fileName = fileName;
        this.context = context;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new IndexOutputPositionOutputStream(directory.createOutput(fileName, context));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        try {
            directory.deleteFile(fileName);
        } catch (FileNotFoundException | NoSuchFileException ignored) {
            // file doesn't exist yet, that's fine
        }
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return fileName;
    }

    /**
     * Bridges Lucene's {@link IndexOutput} to Parquet's {@link PositionOutputStream}.
     */
    static class IndexOutputPositionOutputStream extends PositionOutputStream {

        private final IndexOutput output;

        IndexOutputPositionOutputStream(IndexOutput output) {
            this.output = output;
        }

        @Override
        public long getPos() throws IOException {
            return output.getFilePointer();
        }

        @Override
        public void write(int b) throws IOException {
            output.writeByte((byte) b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            output.writeBytes(b, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            output.writeBytes(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            // IndexOutput doesn't have flush — data is written through
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
