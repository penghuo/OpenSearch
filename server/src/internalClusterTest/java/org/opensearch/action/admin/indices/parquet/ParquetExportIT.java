/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.parquet;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration test for the _parquet_export REST API.
 *
 * Validates that the export endpoint produces standalone Parquet files with valid
 * footers that external tools (Spark, Trino, DuckDB) can read. This is critical
 * because the internal Parquet doc_values format (.pdvd/.pdvm) is NOT a standard
 * Parquet file — the export bridges the gap by writing proper Parquet files with
 * Thrift-encoded FileMetaData footers.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ParquetExportIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PARQUET_DOC_VALUES, true).build();
    }

    /**
     * Tests that the export endpoint writes a valid Parquet file with correct schema
     * and row count. This is the core correctness test — if the file has a valid
     * Parquet footer with the right schema and row count, external tools can read it.
     */
    public void testExportProducesValidParquetFile() throws Exception {
        String indexName = "test-export";
        createParquetIndex(indexName);

        // Index documents with numeric and keyword fields
        int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("status", i, "category", "cat_" + (i % 3)).get();
        }
        flushAndRefresh(indexName);

        // Export to a temp directory
        Path exportDir = createTempDir("parquet-export");
        ParquetExportResponse response = client().execute(
            ParquetExportAction.INSTANCE,
            new ParquetExportRequest(indexName).path(exportDir.toString())
        ).actionGet();

        // Verify response
        assertEquals(0, response.getFailedShards());
        assertTrue("Expected at least one file exported", response.getFileCount() > 0);
        assertTrue("Expected bytes written > 0", response.getTotalBytes() > 0);

        // Verify the Parquet file exists and has valid footer
        Path parquetFile = exportDir.resolve(indexName).resolve("shard_0.parquet");
        assertTrue("Parquet file should exist", Files.exists(parquetFile));

        FileMetaData footer = readParquetFooter(parquetFile);
        assertEquals("Row count should match indexed docs", numDocs, footer.getNum_rows());
        assertEquals("Should have one row group", 1, footer.getRow_groups().size());

        // Schema: root + 2 fields (status, category)
        // Root element has num_children, field elements have type
        assertTrue("Schema should have root + field elements", footer.getSchema().size() >= 3);
        assertEquals("Root schema element should be 'schema'", "schema", footer.getSchema().get(0).getName());
    }

    /**
     * Tests that the export correctly handles multiple field types (numeric, keyword/sorted).
     * Verifies that the Parquet schema maps Lucene doc_values types to the correct
     * Parquet primitive types (INT64 for numeric, BYTE_ARRAY for keyword).
     */
    public void testExportFieldTypes() throws Exception {
        String indexName = "test-export-types";
        createParquetIndex(indexName);

        client().prepareIndex(indexName).setId("1").setSource("status", 42, "category", "alpha").get();
        flushAndRefresh(indexName);

        Path exportDir = createTempDir("parquet-export-types");
        client().execute(ParquetExportAction.INSTANCE, new ParquetExportRequest(indexName).path(exportDir.toString())).actionGet();

        Path parquetFile = exportDir.resolve(indexName).resolve("shard_0.parquet");
        FileMetaData footer = readParquetFooter(parquetFile);

        // Find the status field (should be INT64) and category field (should be BYTE_ARRAY)
        boolean foundNumeric = false;
        boolean foundBinary = false;
        for (int i = 1; i < footer.getSchema().size(); i++) {
            var elem = footer.getSchema().get(i);
            if ("status".equals(elem.getName())) {
                assertEquals(org.apache.parquet.format.Type.INT64, elem.getType());
                foundNumeric = true;
            } else if ("category".equals(elem.getName())) {
                assertEquals(org.apache.parquet.format.Type.BYTE_ARRAY, elem.getType());
                foundBinary = true;
            }
        }
        assertTrue("Should find numeric field 'status'", foundNumeric);
        assertTrue("Should find binary field 'category'", foundBinary);
    }

    /**
     * Tests that the export endpoint returns an error when the required 'path'
     * parameter is missing. This validates input validation in the request.
     */
    public void testExportRequiresPath() {
        String indexName = "test-export-no-path";
        createParquetIndex(indexName);

        ParquetExportRequest request = new ParquetExportRequest(indexName);
        // path is null — should fail validation
        expectThrows(
            org.opensearch.action.ActionRequestValidationException.class,
            () -> client().execute(ParquetExportAction.INSTANCE, request).actionGet()
        );
    }

    private void createParquetIndex(String indexName) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.codec.doc_values.format", "parquet")
                )
                .setMapping("status", "type=long", "category", "type=keyword")
                .get()
        );
    }

    /**
     * Reads and validates the Parquet footer from a file.
     * Parquet file format: PAR1 + data + footer + 4-byte footer length + PAR1
     */
    private FileMetaData readParquetFooter(Path file) throws IOException {
        byte[] fileBytes = Files.readAllBytes(file);

        // Verify magic bytes at start and end
        assertEquals('P', fileBytes[0]);
        assertEquals('A', fileBytes[1]);
        assertEquals('R', fileBytes[2]);
        assertEquals('1', fileBytes[3]);
        assertEquals('P', fileBytes[fileBytes.length - 4]);
        assertEquals('A', fileBytes[fileBytes.length - 3]);
        assertEquals('R', fileBytes[fileBytes.length - 2]);
        assertEquals('1', fileBytes[fileBytes.length - 1]);

        // Read footer length (4 bytes before trailing magic, little-endian)
        int footerLen = ByteBuffer.wrap(fileBytes, fileBytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        assertTrue("Footer length should be positive", footerLen > 0);
        assertTrue("Footer length should be less than file size", footerLen < fileBytes.length);

        // Read footer
        int footerStart = fileBytes.length - 8 - footerLen;
        byte[] footerBytes = new byte[footerLen];
        System.arraycopy(fileBytes, footerStart, footerBytes, 0, footerLen);

        return Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
    }
}
