/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import com.github.luben.zstd.Zstd;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * ZSTD-based compression mode for Lucene stored fields.
 *
 * @opensearch.internal
 */
public class ZstdCompressionMode extends CompressionMode {

    private static final int NUM_SUB_BLOCKS = 10;
    private final int level;

    public ZstdCompressionMode(int level) {
        this.level = level;
    }

    @Override
    public Compressor newCompressor() {
        return new ZstdCompressor(level);
    }

    @Override
    public Decompressor newDecompressor() {
        return new ZstdDecompressor();
    }

    private static class ZstdCompressor extends Compressor {
        private final int level;

        ZstdCompressor(int level) {
            this.level = level;
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int len = (int) buffersInput.length();
            byte[] src = new byte[len];
            buffersInput.readBytes(src, 0, len);

            // Write sub-block size so decompressor knows block boundaries
            final int blockLen = (len + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLen);

            byte[] dst = new byte[(int) Zstd.compressBound(blockLen)];
            for (int start = 0; start < len; start += blockLen) {
                int end = Math.min(start + blockLen, len);
                int compressedSize = (int) Zstd.compressByteArray(dst, 0, dst.length, src, start, end - start, level);
                out.writeVInt(compressedSize);
                out.writeBytes(dst, 0, compressedSize);
            }
        }

        @Override
        public void close() {}
    }

    private static class ZstdDecompressor extends Decompressor {

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            if (length == 0) {
                bytes.length = 0;
                return;
            }

            final int blockLen = in.readVInt();
            bytes.bytes = ArrayUtil.grow(bytes.bytes, length);
            bytes.offset = 0;
            bytes.length = length;

            int dstOff = 0;
            for (int start = 0; start < originalLength; start += blockLen) {
                int blockOrigLen = Math.min(blockLen, originalLength - start);
                int compressedSize = in.readVInt();
                int blockEnd = start + blockOrigLen;

                // Skip blocks entirely before the requested range
                if (blockEnd <= offset) {
                    in.skipBytes(compressedSize);
                    continue;
                }

                // Stop if we've read enough
                if (start >= offset + length) {
                    in.skipBytes(compressedSize);
                    continue;
                }

                // Decompress this block
                byte[] compressed = new byte[compressedSize];
                in.readBytes(compressed, 0, compressedSize);
                byte[] decompressed = new byte[blockOrigLen];
                Zstd.decompressByteArray(decompressed, 0, blockOrigLen, compressed, 0, compressedSize);

                // Copy the relevant portion
                int copyStart = Math.max(0, offset - start);
                int copyEnd = Math.min(blockOrigLen, offset + length - start);
                System.arraycopy(decompressed, copyStart, bytes.bytes, dstOff, copyEnd - copyStart);
                dstOff += copyEnd - copyStart;
            }
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }
}
