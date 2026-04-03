/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;

/**
 * Reads a Parquet footer from the end of a file via Lucene's {@link IndexInput}
 * and extracts metadata. The Parquet file layout is:
 * {@code PAR1 | column data | footer (Thrift) | footer length (4 bytes LE) | PAR1}
 *
 * @opensearch.experimental
 */
public class ParquetFooterReader {

    private final IndexInput input;

    /**
     * Creates a new footer reader.
     *
     * @param input the Lucene IndexInput positioned at the start of a Parquet file
     */
    public ParquetFooterReader(IndexInput input) {
        this.input = input;
    }

    /**
     * Reads the Parquet footer from the end of the file.
     * <p>
     * The file must end with: {@code footer bytes | 4-byte footer length (LE) | PAR1}.
     *
     * @return the deserialized {@link FileMetaData}
     * @throws IOException if an I/O error occurs or the file is corrupt
     */
    public FileMetaData readFooter() throws IOException {
        long fileLen = input.length();
        if (fileLen < 12) {
            throw new CorruptIndexException("File too short for Parquet format", input.toString());
        }

        // Read trailing PAR1 magic (last 4 bytes)
        byte[] trailingMagic = new byte[4];
        input.seek(fileLen - 4);
        input.readBytes(trailingMagic, 0, 4);
        if (!Arrays.equals(trailingMagic, ParquetFooterWriter.PARQUET_MAGIC)) {
            throw new CorruptIndexException("Missing trailing PAR1 magic", input.toString());
        }

        // Read footer length (4 bytes little-endian before trailing magic)
        byte[] footerLenBytes = new byte[4];
        input.seek(fileLen - 8);
        input.readBytes(footerLenBytes, 0, 4);
        int footerLen = ByteBuffer.wrap(footerLenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

        if (footerLen <= 0 || footerLen > fileLen - 12) {
            throw new CorruptIndexException("Invalid footer length: " + footerLen, input.toString());
        }

        // Seek to footer start, read bytes, deserialize
        long footerStart = fileLen - 8 - footerLen;
        input.seek(footerStart);
        byte[] footerBytes = new byte[footerLen];
        input.readBytes(footerBytes, 0, footerLen);

        return Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
    }

    /**
     * Validates that the file starts with the PAR1 magic bytes.
     *
     * @throws IOException if an I/O error occurs or the magic is missing
     */
    public void validateLeadingMagic() throws IOException {
        byte[] leadingMagic = new byte[4];
        input.seek(0);
        input.readBytes(leadingMagic, 0, 4);
        if (!Arrays.equals(leadingMagic, ParquetFooterWriter.PARQUET_MAGIC)) {
            throw new CorruptIndexException("Missing leading PAR1 magic", input.toString());
        }
    }

    /**
     * Extracts key-value metadata from a {@link FileMetaData} as a map.
     *
     * @param meta the file metadata
     * @return a map of key-value pairs
     */
    public static Map<String, String> extractKvMetadata(FileMetaData meta) {
        return ParquetFooterWriter.kvToMap(meta.getKey_value_metadata());
    }
}
