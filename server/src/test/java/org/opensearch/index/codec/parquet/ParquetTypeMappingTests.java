/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link ParquetTypeMapping}.
 *
 * <p>ParquetTypeMapping is the foundation of the Parquet DocValues codec: every
 * writer column and reader column is created from these mappings. If a mapping
 * produces the wrong Parquet type or repetition level, data will be silently
 * corrupted or unreadable. These tests lock down the contract between Lucene
 * DocValues semantics and Parquet physical types.</p>
 *
 * @opensearch.experimental
 */
public class ParquetTypeMappingTests extends OpenSearchTestCase {

    /**
     * NUMERIC doc values store exactly one long per document.
     * The Parquet representation must be REQUIRED INT64 — REQUIRED because
     * every document has a value, INT64 because Lucene numerics are longs.
     */
    public void testNumericTypeIsRequiredInt64() {
        PrimitiveType type = ParquetTypeMapping.numericType("price");
        assertEquals(PrimitiveTypeName.INT64, type.getPrimitiveTypeName());
        assertEquals(Repetition.REQUIRED, type.getRepetition());
        assertEquals("price", type.getName());
    }

    /**
     * SORTED_NUMERIC doc values store zero or more longs per document.
     * The Parquet representation must be REPEATED INT64 — REPEATED because
     * a document can have multiple values (e.g., multi-valued numeric fields).
     */
    public void testSortedNumericTypeIsRepeatedInt64() {
        PrimitiveType type = ParquetTypeMapping.sortedNumericType("timestamps");
        assertEquals(PrimitiveTypeName.INT64, type.getPrimitiveTypeName());
        assertEquals(Repetition.REPEATED, type.getRepetition());
        assertEquals("timestamps", type.getName());
    }

    /**
     * BINARY doc values store exactly one byte array per document.
     * The Parquet representation must be REQUIRED BINARY.
     */
    public void testBinaryTypeIsRequiredBinary() {
        PrimitiveType type = ParquetTypeMapping.binaryType("payload");
        assertEquals(PrimitiveTypeName.BINARY, type.getPrimitiveTypeName());
        assertEquals(Repetition.REQUIRED, type.getRepetition());
        assertEquals("payload", type.getName());
    }

    /**
     * SORTED doc values store one dictionary-encoded byte array per document.
     * The Parquet representation must be REQUIRED BINARY — dictionary encoding
     * is handled at the page level, not the schema level.
     */
    public void testSortedTypeIsRequiredBinary() {
        PrimitiveType type = ParquetTypeMapping.sortedType("status");
        assertEquals(PrimitiveTypeName.BINARY, type.getPrimitiveTypeName());
        assertEquals(Repetition.REQUIRED, type.getRepetition());
        assertEquals("status", type.getName());
    }

    /**
     * SORTED_SET doc values store zero or more dictionary-encoded byte arrays
     * per document. The Parquet representation must be REPEATED BINARY.
     */
    public void testSortedSetTypeIsRepeatedBinary() {
        PrimitiveType type = ParquetTypeMapping.sortedSetType("tags");
        assertEquals(PrimitiveTypeName.BINARY, type.getPrimitiveTypeName());
        assertEquals(Repetition.REPEATED, type.getRepetition());
        assertEquals("tags", type.getName());
    }

    /**
     * messageType wraps a column into a root MessageType. The writer and reader
     * both need a MessageType to construct ColumnWriteStore / ColumnReadStore.
     * Verify the schema contains exactly one column with the right path.
     */
    public void testMessageTypeWrapsColumn() {
        PrimitiveType col = ParquetTypeMapping.numericType("count");
        MessageType schema = ParquetTypeMapping.messageType("test_schema", col);
        assertEquals("test_schema", schema.getName());
        assertEquals(1, schema.getFieldCount());
        assertEquals(1, schema.getColumns().size());
        assertEquals("count", schema.getColumns().get(0).getPath()[0]);
    }

    /**
     * columnDescriptor extracts the ColumnDescriptor from a MessageType.
     * ColumnDescriptor carries the path, type, and max rep/def levels that
     * the Parquet column API requires for reading and writing.
     */
    public void testColumnDescriptorFromSchema() {
        PrimitiveType col = ParquetTypeMapping.sortedNumericType("values");
        MessageType schema = ParquetTypeMapping.messageType("schema", col);
        ColumnDescriptor desc = ParquetTypeMapping.columnDescriptor(schema);
        assertEquals(PrimitiveTypeName.INT64, desc.getPrimitiveType().getPrimitiveTypeName());
        assertArrayEquals(new String[] { "values" }, desc.getPath());
        assertEquals(1, desc.getMaxRepetitionLevel());
        assertEquals(1, desc.getMaxDefinitionLevel());
    }

    /**
     * Convenience overload: columnDescriptor(schemaName, columnType) should
     * produce the same result as building a MessageType first.
     */
    public void testColumnDescriptorConvenience() {
        PrimitiveType col = ParquetTypeMapping.binaryType("data");
        ColumnDescriptor fromSchema = ParquetTypeMapping.columnDescriptor(ParquetTypeMapping.messageType("s", col));
        ColumnDescriptor direct = ParquetTypeMapping.columnDescriptor("s", col);
        assertEquals(fromSchema.getPrimitiveType().getPrimitiveTypeName(), direct.getPrimitiveType().getPrimitiveTypeName());
        assertArrayEquals(fromSchema.getPath(), direct.getPath());
        assertEquals(fromSchema.getMaxRepetitionLevel(), direct.getMaxRepetitionLevel());
        assertEquals(fromSchema.getMaxDefinitionLevel(), direct.getMaxDefinitionLevel());
    }

    /**
     * REQUIRED fields must have maxRepetitionLevel=0 and maxDefinitionLevel=0.
     * These levels control how Parquet encodes null/repeated values — getting
     * them wrong causes data corruption in the column pages.
     */
    public void testRequiredFieldLevels() {
        PrimitiveType required = ParquetTypeMapping.numericType("x");
        assertEquals(0, ParquetTypeMapping.maxRepetitionLevel(required));
        assertEquals(0, ParquetTypeMapping.maxDefinitionLevel(required));
    }

    /**
     * REPEATED fields must have maxRepetitionLevel=1 and maxDefinitionLevel=1.
     * repetitionLevel=1 signals a new value in the same record; definitionLevel=1
     * signals the value is present.
     */
    public void testRepeatedFieldLevels() {
        PrimitiveType repeated = ParquetTypeMapping.sortedNumericType("y");
        assertEquals(1, ParquetTypeMapping.maxRepetitionLevel(repeated));
        assertEquals(1, ParquetTypeMapping.maxDefinitionLevel(repeated));
    }

    /**
     * The field name flows through to the Parquet schema unchanged. This matters
     * because the reader uses field names to locate columns in the Parquet metadata.
     */
    public void testFieldNamePreserved() {
        String name = "my_field_123";
        assertEquals(name, ParquetTypeMapping.numericType(name).getName());
        assertEquals(name, ParquetTypeMapping.sortedNumericType(name).getName());
        assertEquals(name, ParquetTypeMapping.binaryType(name).getName());
        assertEquals(name, ParquetTypeMapping.sortedType(name).getName());
        assertEquals(name, ParquetTypeMapping.sortedSetType(name).getName());
    }
}
