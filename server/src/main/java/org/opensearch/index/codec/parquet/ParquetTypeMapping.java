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
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Maps Lucene DocValues types to Parquet primitive types and schema elements.
 *
 * <p>Lucene stores five kinds of per-document values. Each maps to a Parquet
 * primitive with a specific repetition level:</p>
 * <ul>
 *   <li>{@code NUMERIC} → required {@code INT64}</li>
 *   <li>{@code SORTED_NUMERIC} → repeated {@code INT64}</li>
 *   <li>{@code BINARY} → required {@code BINARY}</li>
 *   <li>{@code SORTED} → required {@code BINARY} (dictionary-encoded)</li>
 *   <li>{@code SORTED_SET} → repeated {@code BINARY} (dictionary-encoded)</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public final class ParquetTypeMapping {

    private ParquetTypeMapping() {}

    /** Parquet type for a single long value per document (NUMERIC doc values). */
    public static PrimitiveType numericType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
    }

    /** Parquet type for multiple long values per document (SORTED_NUMERIC doc values). */
    public static PrimitiveType sortedNumericType(String fieldName) {
        return Types.repeated(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
    }

    /** Parquet type for a single byte array per document (BINARY doc values). */
    public static PrimitiveType binaryType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
    }

    /** Parquet type for a dictionary-encoded byte array per document (SORTED doc values). */
    public static PrimitiveType sortedType(String fieldName) {
        return Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
    }

    /** Parquet type for multiple dictionary-encoded byte arrays per document (SORTED_SET doc values). */
    public static PrimitiveType sortedSetType(String fieldName) {
        return Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
    }

    /** Wraps a single column type into a Parquet {@link MessageType} (root schema). */
    public static MessageType messageType(String schemaName, PrimitiveType columnType) {
        return new MessageType(schemaName, columnType);
    }

    /** Returns the {@link ColumnDescriptor} for the single column in a one-field message schema. */
    public static ColumnDescriptor columnDescriptor(MessageType schema) {
        return schema.getColumns().get(0);
    }

    /**
     * Convenience: builds a {@link MessageType} and extracts its {@link ColumnDescriptor}
     * for a single-field schema.
     */
    public static ColumnDescriptor columnDescriptor(String schemaName, PrimitiveType columnType) {
        return columnDescriptor(messageType(schemaName, columnType));
    }

    /**
     * Returns the max repetition level for the given type.
     * REQUIRED fields have 0; REPEATED fields have 1.
     */
    public static int maxRepetitionLevel(PrimitiveType type) {
        return type.getRepetition() == Type.Repetition.REPEATED ? 1 : 0;
    }

    /**
     * Returns the max definition level for the given type.
     * REQUIRED fields have 0; OPTIONAL fields have 1; REPEATED fields have 1.
     */
    public static int maxDefinitionLevel(PrimitiveType type) {
        return type.getRepetition() == Type.Repetition.REQUIRED ? 0 : 1;
    }
}
