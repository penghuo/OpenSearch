/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.spark;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

/**
 * Parquet {@link InputFile} that reads OpenSearch .pdvd files directly.
 * <p>
 * A .pdvd file wraps standard Parquet content inside Lucene CodecUtil framing:
 * <pre>
 *   [CodecUtil header N bytes] [PAR1 ...parquet... PAR1] [CodecUtil footer 16 bytes]
 * </pre>
 * This class finds the PAR1 magic, then presents the Parquet content to readers
 * with all positions shifted so PAR1 appears at offset 0.
 * <p>
 * No temp files. No byte copying. Reads directly from the .pdvd file.
 */
public class PdvdInputFile implements InputFile {

    private static final byte[] PAR1_MAGIC = { 'P', 'A', 'R', '1' };
    private static final int CODEC_UTIL_FOOTER_SIZE = 16;
    private static final int SCAN_LIMIT = 200;

    private final Path path;
    private final long parquetStart;
    private final long parquetLength;

    public PdvdInputFile(Path path) throws IOException {
        this.path = path;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long fileSize = raf.length();

            // Scan for PAR1 magic in the first SCAN_LIMIT bytes
            int scanBytes = (int) Math.min(SCAN_LIMIT, fileSize);
            byte[] header = new byte[scanBytes];
            raf.readFully(header);

            int par1Offset = findMagic(header);
            if (par1Offset < 0) {
                throw new IOException("No PAR1 magic found in first " + SCAN_LIMIT + " bytes of " + path);
            }

            this.parquetStart = par1Offset;
            this.parquetLength = fileSize - par1Offset - CODEC_UTIL_FOOTER_SIZE;

            if (parquetLength < 12) {
                throw new IOException("Parquet content too short (" + parquetLength + " bytes) in " + path);
            }
        }
    }

    @Override
    public long getLength() {
        return parquetLength;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new PdvdSeekableInputStream(path, parquetStart, parquetLength);
    }

    private static int findMagic(byte[] data) {
        for (int i = 0; i <= data.length - 4; i++) {
            if (data[i] == PAR1_MAGIC[0] && data[i + 1] == PAR1_MAGIC[1]
                && data[i + 2] == PAR1_MAGIC[2] && data[i + 3] == PAR1_MAGIC[3]) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return "PdvdInputFile{" + path + ", parquetStart=" + parquetStart + ", parquetLength=" + parquetLength + "}";
    }
}
