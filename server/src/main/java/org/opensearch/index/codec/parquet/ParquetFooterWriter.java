/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a standard Parquet {@link FileMetaData} Thrift struct from accumulated
 * column metadata. Lucene-specific metadata (codec version, segment ID, per-field
 * offsets) is stored in Parquet's {@code key_value_metadata}.
 *
 * @opensearch.experimental
 */
public class ParquetFooterWriter {

    /** Standard Parquet magic bytes: {@code PAR1}. */
    public static final byte[] PARQUET_MAGIC = new byte[] { 'P', 'A', 'R', '1' };

    private final String codecVersion;
    private final String segmentId;
    private final String segmentSuffix;
    private final int maxDoc;
    private final long totalRows;
    private final List<ColumnChunkInfo> columns;

    /**
     * Metadata for a single column chunk written to the Parquet file.
     */
    public static class ColumnChunkInfo {
        public final String fieldName;
        public final String dvTypeName;
        public final FieldRepetitionType repetition;
        public final Type parquetType;
        public final long fileOffset;
        public final long dataPageOffset;
        public final long dictPageOffset;
        public final int pageCount;
        public final boolean hasDictionary;
        public final long docIdOffset;
        public final long packedValuesOffset;
        public final long totalCompressedSize;
        public final long totalUncompressedSize;
        public final long numValues;
        public final List<org.apache.parquet.format.Encoding> encodings;

        public ColumnChunkInfo(
            String fieldName,
            String dvTypeName,
            FieldRepetitionType repetition,
            Type parquetType,
            long fileOffset,
            long dataPageOffset,
            long dictPageOffset,
            int pageCount,
            boolean hasDictionary,
            long docIdOffset,
            long packedValuesOffset,
            long totalCompressedSize,
            long totalUncompressedSize,
            long numValues,
            List<org.apache.parquet.format.Encoding> encodings
        ) {
            this.fieldName = fieldName;
            this.dvTypeName = dvTypeName;
            this.repetition = repetition;
            this.parquetType = parquetType;
            this.fileOffset = fileOffset;
            this.dataPageOffset = dataPageOffset;
            this.dictPageOffset = dictPageOffset;
            this.pageCount = pageCount;
            this.hasDictionary = hasDictionary;
            this.docIdOffset = docIdOffset;
            this.packedValuesOffset = packedValuesOffset;
            this.totalCompressedSize = totalCompressedSize;
            this.totalUncompressedSize = totalUncompressedSize;
            this.numValues = numValues;
            this.encodings = encodings;
        }
    }

    /**
     * Creates a new footer writer.
     *
     * @param codecVersion  the codec version string
     * @param segmentId     the Lucene segment ID
     * @param segmentSuffix the segment suffix
     * @param maxDoc        the maximum document number
     * @param totalRows     the total number of rows
     * @param columns       the column chunk metadata
     */
    public ParquetFooterWriter(
        String codecVersion,
        String segmentId,
        String segmentSuffix,
        int maxDoc,
        long totalRows,
        List<ColumnChunkInfo> columns
    ) {
        this.codecVersion = codecVersion;
        this.segmentId = segmentId;
        this.segmentSuffix = segmentSuffix;
        this.maxDoc = maxDoc;
        this.totalRows = totalRows;
        this.columns = columns;
    }

    /**
     * Builds the complete {@link FileMetaData} with schema, row group, and KV metadata.
     */
    public FileMetaData buildFileMetaData() {
        // Build schema
        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(columns.size());
        schema.add(root);

        for (ColumnChunkInfo col : columns) {
            SchemaElement elem = new SchemaElement(col.fieldName);
            elem.setType(col.parquetType);
            elem.setRepetition_type(col.repetition);
            schema.add(elem);
        }

        // Build ColumnChunks for the RowGroup
        List<ColumnChunk> columnChunks = new ArrayList<>();
        long totalByteSize = 0;

        for (ColumnChunkInfo col : columns) {
            ColumnMetaData colMeta = new ColumnMetaData(
                col.parquetType,
                col.encodings,
                Collections.singletonList(col.fieldName),
                CompressionCodec.ZSTD,
                col.numValues,
                col.totalUncompressedSize,
                col.totalCompressedSize,
                col.dataPageOffset
            );
            if (col.dictPageOffset >= 0) {
                colMeta.setDictionary_page_offset(col.dictPageOffset);
            }

            ColumnChunk cc = new ColumnChunk(col.fileOffset);
            cc.setMeta_data(colMeta);
            columnChunks.add(cc);

            totalByteSize += col.totalCompressedSize;
        }

        // Build RowGroup
        RowGroup rowGroup = new RowGroup(columnChunks, totalByteSize, totalRows);

        // Build KV metadata
        List<KeyValue> kvMetadata = new ArrayList<>();
        kvMetadata.add(new KeyValue("lucene.codec.version").setValue(codecVersion));
        kvMetadata.add(new KeyValue("lucene.segment.id").setValue(segmentId));
        kvMetadata.add(new KeyValue("lucene.segment.suffix").setValue(segmentSuffix));
        kvMetadata.add(new KeyValue("lucene.maxDoc").setValue(String.valueOf(maxDoc)));

        for (ColumnChunkInfo col : columns) {
            String prefix = "field." + col.fieldName + ".";
            kvMetadata.add(new KeyValue(prefix + "dvTypeName").setValue(col.dvTypeName));
            kvMetadata.add(new KeyValue(prefix + "repetition").setValue(col.repetition.name()));
            kvMetadata.add(new KeyValue(prefix + "pageCount").setValue(String.valueOf(col.pageCount)));
            kvMetadata.add(new KeyValue(prefix + "hasDictionary").setValue(String.valueOf(col.hasDictionary)));
            kvMetadata.add(new KeyValue(prefix + "docIdOffset").setValue(String.valueOf(col.docIdOffset)));
            kvMetadata.add(new KeyValue(prefix + "packedValuesOffset").setValue(String.valueOf(col.packedValuesOffset)));
        }

        // Build FileMetaData
        FileMetaData fileMetaData = new FileMetaData(1, schema, totalRows, Collections.singletonList(rowGroup));
        fileMetaData.setCreated_by("OpenSearch Parquet DocValues");
        fileMetaData.setKey_value_metadata(kvMetadata);

        return fileMetaData;
    }

    /**
     * Serializes the given {@link FileMetaData} to bytes using Parquet Thrift serialization.
     */
    public static byte[] serializeFooter(FileMetaData fileMetaData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Util.writeFileMetaData(fileMetaData, baos);
        return baos.toByteArray();
    }

    /**
     * Converts a list of Parquet {@link KeyValue} entries to a {@link Map}.
     */
    public static Map<String, String> kvToMap(List<KeyValue> kvList) {
        Map<String, String> map = new HashMap<>();
        if (kvList != null) {
            for (KeyValue kv : kvList) {
                map.put(kv.getKey(), kv.getValue());
            }
        }
        return map;
    }
}
