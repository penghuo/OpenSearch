/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ParquetFooterWriter}.
 *
 * @opensearch.experimental
 */
public class ParquetFooterWriterTests extends OpenSearchTestCase {

    /**
     * Builds a footer for a single NUMERIC field and verifies the schema has
     * 2 elements (root + field), KV metadata contains all expected keys, and
     * serialization round-trips correctly.
     */
    public void testBuildFooterSingleNumericField() throws Exception {
        ParquetFooterWriter.ColumnChunkInfo col = new ParquetFooterWriter.ColumnChunkInfo(
            "price",
            "NUMERIC",
            FieldRepetitionType.OPTIONAL,
            Type.INT64,
            4L,         // fileOffset (after PAR1 magic)
            4L,         // dataPageOffset
            -1L,        // dictPageOffset (no dictionary)
            1,          // pageCount
            false,      // hasDictionary
            100L,       // docIdOffset
            200L,       // packedValuesOffset
            1024L,      // totalCompressedSize
            2048L,      // totalUncompressedSize
            500L,       // numValues
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

        // Schema: root + 1 field = 2 elements
        List<SchemaElement> schema = fmd.getSchema();
        assertEquals(2, schema.size());
        assertEquals("schema", schema.get(0).getName());
        assertEquals("price", schema.get(1).getName());
        assertEquals(Type.INT64, schema.get(1).getType());
        assertEquals(FieldRepetitionType.OPTIONAL, schema.get(1).getRepetition_type());

        // RowGroup
        assertEquals(1, fmd.getRow_groups().size());
        assertEquals(1, fmd.getRow_groups().get(0).getColumns().size());
        assertEquals(500, fmd.getNum_rows());

        // KV metadata
        Map<String, String> kvMap = ParquetFooterWriter.kvToMap(fmd.getKey_value_metadata());
        assertEquals("1.0", kvMap.get("lucene.codec.version"));
        assertEquals("seg_abc123", kvMap.get("lucene.segment.id"));
        assertEquals("", kvMap.get("lucene.segment.suffix"));
        assertEquals("1000", kvMap.get("lucene.maxDoc"));
        assertEquals("NUMERIC", kvMap.get("field.price.dvTypeName"));
        assertEquals("OPTIONAL", kvMap.get("field.price.repetition"));
        assertEquals("1", kvMap.get("field.price.pageCount"));
        assertEquals("false", kvMap.get("field.price.hasDictionary"));
        assertEquals("100", kvMap.get("field.price.docIdOffset"));
        assertEquals("200", kvMap.get("field.price.packedValuesOffset"));

        // Serialization round-trip
        byte[] bytes = ParquetFooterWriter.serializeFooter(fmd);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        FileMetaData roundTripped = Util.readFileMetaData(new ByteArrayInputStream(bytes));
        assertEquals(fmd.getNum_rows(), roundTripped.getNum_rows());
        assertEquals(fmd.getSchema().size(), roundTripped.getSchema().size());
        assertEquals(fmd.getKey_value_metadata().size(), roundTripped.getKey_value_metadata().size());
    }

    /**
     * Builds a footer for 3 fields (NUMERIC, SORTED, SORTED_SET) and verifies
     * the schema has 4 elements (root + 3 fields) and all KV metadata is present.
     */
    public void testBuildFooterMultipleFields() throws Exception {
        List<ParquetFooterWriter.ColumnChunkInfo> columns = new ArrayList<>();

        // NUMERIC field
        columns.add(new ParquetFooterWriter.ColumnChunkInfo(
            "price",
            "NUMERIC",
            FieldRepetitionType.OPTIONAL,
            Type.INT64,
            4L, 4L, -1L,
            1, false, 100L, 200L,
            512L, 1024L, 100L,
            Collections.singletonList(org.apache.parquet.format.Encoding.PLAIN)
        ));

        // SORTED field
        columns.add(new ParquetFooterWriter.ColumnChunkInfo(
            "status",
            "SORTED",
            FieldRepetitionType.OPTIONAL,
            Type.BYTE_ARRAY,
            520L, 600L, 520L,
            2, true, 300L, 400L,
            768L, 1536L, 200L,
            Arrays.asList(org.apache.parquet.format.Encoding.PLAIN, org.apache.parquet.format.Encoding.PLAIN_DICTIONARY)
        ));

        // SORTED_SET field
        columns.add(new ParquetFooterWriter.ColumnChunkInfo(
            "tags",
            "SORTED_SET",
            FieldRepetitionType.REPEATED,
            Type.BYTE_ARRAY,
            1300L, 1400L, 1300L,
            3, true, 500L, 600L,
            1024L, 2048L, 350L,
            Arrays.asList(org.apache.parquet.format.Encoding.PLAIN, org.apache.parquet.format.Encoding.PLAIN_DICTIONARY)
        ));

        ParquetFooterWriter writer = new ParquetFooterWriter(
            "2.0",
            "seg_xyz789",
            "_0",
            5000,
            1000,
            columns
        );

        FileMetaData fmd = writer.buildFileMetaData();

        // Schema: root + 3 fields = 4 elements
        List<SchemaElement> schema = fmd.getSchema();
        assertEquals(4, schema.size());
        assertEquals("schema", schema.get(0).getName());
        assertEquals(3, schema.get(0).getNum_children());
        assertEquals("price", schema.get(1).getName());
        assertEquals("status", schema.get(2).getName());
        assertEquals("tags", schema.get(3).getName());

        // Verify types
        assertEquals(Type.INT64, schema.get(1).getType());
        assertEquals(Type.BYTE_ARRAY, schema.get(2).getType());
        assertEquals(Type.BYTE_ARRAY, schema.get(3).getType());

        // Verify repetition
        assertEquals(FieldRepetitionType.OPTIONAL, schema.get(1).getRepetition_type());
        assertEquals(FieldRepetitionType.OPTIONAL, schema.get(2).getRepetition_type());
        assertEquals(FieldRepetitionType.REPEATED, schema.get(3).getRepetition_type());

        // RowGroup has 3 columns
        assertEquals(1, fmd.getRow_groups().size());
        assertEquals(3, fmd.getRow_groups().get(0).getColumns().size());

        // KV metadata for all fields
        Map<String, String> kvMap = ParquetFooterWriter.kvToMap(fmd.getKey_value_metadata());
        assertEquals("2.0", kvMap.get("lucene.codec.version"));
        assertEquals("seg_xyz789", kvMap.get("lucene.segment.id"));
        assertEquals("_0", kvMap.get("lucene.segment.suffix"));
        assertEquals("5000", kvMap.get("lucene.maxDoc"));

        // Verify per-field metadata exists for all 3 fields
        for (String fieldName : new String[] { "price", "status", "tags" }) {
            String prefix = "field." + fieldName + ".";
            assertNotNull("Missing dvTypeName for " + fieldName, kvMap.get(prefix + "dvTypeName"));
            assertNotNull("Missing repetition for " + fieldName, kvMap.get(prefix + "repetition"));
            assertNotNull("Missing pageCount for " + fieldName, kvMap.get(prefix + "pageCount"));
            assertNotNull("Missing hasDictionary for " + fieldName, kvMap.get(prefix + "hasDictionary"));
            assertNotNull("Missing docIdOffset for " + fieldName, kvMap.get(prefix + "docIdOffset"));
            assertNotNull("Missing packedValuesOffset for " + fieldName, kvMap.get(prefix + "packedValuesOffset"));
        }

        // Verify specific values for SORTED field
        assertEquals("SORTED", kvMap.get("field.status.dvTypeName"));
        assertEquals("true", kvMap.get("field.status.hasDictionary"));

        // Verify specific values for SORTED_SET field
        assertEquals("SORTED_SET", kvMap.get("field.tags.dvTypeName"));
        assertEquals("REPEATED", kvMap.get("field.tags.repetition"));

        // Serialization works
        byte[] bytes = ParquetFooterWriter.serializeFooter(fmd);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }
}
