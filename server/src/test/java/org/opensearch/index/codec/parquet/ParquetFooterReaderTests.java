/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Type;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Map;

/**
 * Tests for {@link ParquetFooterReader}.
 *
 * @opensearch.experimental
 */
public class ParquetFooterReaderTests extends OpenSearchTestCase {

    /**
     * Writes a fake .pdvd file with PAR1 header, dummy data, serialized footer,
     * footer length, and trailing PAR1. Then reads it back with ParquetFooterReader
     * and verifies the metadata round-trips correctly.
     */
    public void testReadFooterRoundTrip() throws Exception {
        // Build footer using ParquetFooterWriter
        ParquetFooterWriter.ColumnChunkInfo col = new ParquetFooterWriter.ColumnChunkInfo(
            "price",
            "NUMERIC",
            FieldRepetitionType.OPTIONAL,
            Type.INT64,
            4L,
            4L,
            -1L,
            1,
            false,
            100L,
            200L,
            1024L,
            2048L,
            500L,
            Collections.singletonList(org.apache.parquet.format.Encoding.PLAIN)
        );

        ParquetFooterWriter writer = new ParquetFooterWriter(
            "1.0",
            "seg_abc123",
            "",
            1000,
            500,
            Collections.singletonList(col)
        );

        FileMetaData fmd = writer.buildFileMetaData();
        byte[] footerBytes = ParquetFooterWriter.serializeFooter(fmd);

        // Write fake .pdvd file: PAR1 + 200 bytes dummy + footer + footerLen(LE) + PAR1
        Directory dir = newDirectory();
        try (IndexOutput out = dir.createOutput("test.pdvd", IOContext.DEFAULT)) {
            out.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);

            byte[] dummy = new byte[200];
            out.writeBytes(dummy, 0, dummy.length);

            out.writeBytes(footerBytes, 0, footerBytes.length);

            byte[] footerLenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array();
            out.writeBytes(footerLenBytes, 0, 4);

            out.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);
        }

        // Read back and verify
        try (IndexInput in = dir.openInput("test.pdvd", IOContext.DEFAULT)) {
            ParquetFooterReader reader = new ParquetFooterReader(in);

            reader.validateLeadingMagic();

            FileMetaData readFmd = reader.readFooter();

            assertEquals(500, readFmd.getNum_rows());
            assertEquals(1, readFmd.getRow_groups().size());
            assertEquals(1, readFmd.getRow_groups().get(0).getColumns().size());

            Map<String, String> kvMap = ParquetFooterReader.extractKvMetadata(readFmd);
            assertEquals("1.0", kvMap.get("lucene.codec.version"));
            assertEquals("seg_abc123", kvMap.get("lucene.segment.id"));
            assertEquals("", kvMap.get("lucene.segment.suffix"));
            assertEquals("1000", kvMap.get("lucene.maxDoc"));
            assertEquals("NUMERIC", kvMap.get("field.price.dvTypeName"));
            assertEquals("OPTIONAL", kvMap.get("field.price.repetition"));
            assertEquals("false", kvMap.get("field.price.hasDictionary"));
        }

        dir.close();
    }

    /**
     * Writes a file that does not start with PAR1, verifies that
     * validateLeadingMagic throws CorruptIndexException.
     */
    public void testInvalidMagicThrows() throws Exception {
        Directory dir = newDirectory();

        // Write a file with invalid leading magic but valid trailing structure
        ParquetFooterWriter.ColumnChunkInfo col = new ParquetFooterWriter.ColumnChunkInfo(
            "f",
            "NUMERIC",
            FieldRepetitionType.OPTIONAL,
            Type.INT64,
            4L, 4L, -1L, 1, false, 0L, 0L,
            10L, 20L, 1L,
            Collections.singletonList(org.apache.parquet.format.Encoding.PLAIN)
        );

        ParquetFooterWriter writer = new ParquetFooterWriter("1.0", "seg1", "", 1, 1, Collections.singletonList(col));
        byte[] footerBytes = ParquetFooterWriter.serializeFooter(writer.buildFileMetaData());

        try (IndexOutput out = dir.createOutput("bad.pdvd", IOContext.DEFAULT)) {
            // Write NOPE instead of PAR1
            out.writeBytes(new byte[] { 'N', 'O', 'P', 'E' }, 0, 4);
            byte[] dummy = new byte[200];
            out.writeBytes(dummy, 0, dummy.length);
            out.writeBytes(footerBytes, 0, footerBytes.length);
            byte[] footerLenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array();
            out.writeBytes(footerLenBytes, 0, 4);
            out.writeBytes(ParquetFooterWriter.PARQUET_MAGIC, 0, 4);
        }

        try (IndexInput in = dir.openInput("bad.pdvd", IOContext.DEFAULT)) {
            ParquetFooterReader reader = new ParquetFooterReader(in);
            // readFooter should succeed (trailing magic is valid)
            reader.readFooter();
            // But leading magic validation should fail
            expectThrows(CorruptIndexException.class, reader::validateLeadingMagic);
        }

        dir.close();
    }
}
