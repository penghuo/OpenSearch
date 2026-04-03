/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.format.FileMetaData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;

/**
 * Creates a .pdvd file and validates it can be parsed as valid Parquet by standard tools.
 * Also exports the file to /tmp/opensearch-parquet/ for external testing with spark-sql.
 */
public class ParquetSparkCompatTests extends OpenSearchTestCase {

    public void testCreatePdvdAndValidateParquetFooter() throws Exception {
        // Create index with Parquet doc values
        Path tmpDir = createTempDir("parquet-spark-compat");
        Directory dir = FSDirectory.open(tmpDir);

        IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
        conf.setCodec(new org.apache.lucene.codecs.lucene104.Lucene104Codec() {
            @Override
            public org.apache.lucene.codecs.DocValuesFormat getDocValuesFormatForField(String field) {
                return new ParquetDocValuesFormat();
            }
        });
        conf.setUseCompoundFile(false); // Force non-compound to get .pdvd files

        IndexWriter writer = new IndexWriter(dir, conf);

        // Index 100 documents with various field types
        String[] countries = {"US", "CA", "UK", "DE", "JP"};
        String[] paths = {"/index.html", "/api/users", "/images/logo.png", "/about", "/search"};

        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField("timestamp", 1718000000000L + i * 60000L));
            doc.add(new SortedNumericDocValuesField("status", 200 + (i % 5) * 100));
            doc.add(new SortedNumericDocValuesField("size", 100 + i * 10));
            doc.add(new SortedSetDocValuesField("country", new BytesRef(countries[i % 5])));
            doc.add(new SortedSetDocValuesField("request", new BytesRef(paths[i % 5])));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // Find the .pdvd file
        String pdvdFile = null;
        for (String file : dir.listAll()) {
            if (file.endsWith(".pdvd")) {
                pdvdFile = file;
                break;
            }
        }
        assertNotNull("No .pdvd file found", pdvdFile);

        // Validate: file starts with CodecUtil header, contains PAR1, ends with CodecUtil footer
        IndexInput input = dir.openInput(pdvdFile, IOContext.DEFAULT);
        long fileLen = input.length();
        assertTrue("File too small", fileLen > 100);

        // Find PAR1 magic after CodecUtil header
        byte[] buf = new byte[100];
        input.readBytes(buf, 0, Math.min(100, (int) fileLen));
        int par1Offset = -1;
        for (int i = 0; i <= buf.length - 4; i++) {
            if (buf[i] == 'P' && buf[i + 1] == 'A' && buf[i + 2] == 'R' && buf[i + 3] == '1') {
                par1Offset = i;
                break;
            }
        }
        assertTrue("No PAR1 magic found", par1Offset > 0);

        // Extract Parquet content (between PAR1 and CodecUtil footer)
        int codecFooterLen = 16;
        long parquetStart = par1Offset;
        long parquetEnd = fileLen - codecFooterLen;

        // Read trailing PAR1
        input.seek(parquetEnd - 4);
        byte[] trailingMagic = new byte[4];
        input.readBytes(trailingMagic, 0, 4);
        assertArrayEquals("Missing trailing PAR1", new byte[]{'P', 'A', 'R', '1'}, trailingMagic);

        // Read footer length
        input.seek(parquetEnd - 8);
        byte[] footerLenBytes = new byte[4];
        input.readBytes(footerLenBytes, 0, 4);
        int footerLen = ByteBuffer.wrap(footerLenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        assertTrue("Invalid footer length", footerLen > 0 && footerLen < parquetEnd);

        // Read and parse Parquet footer
        long footerStart = parquetEnd - 8 - footerLen;
        input.seek(footerStart);
        byte[] footerBytes = new byte[footerLen];
        input.readBytes(footerBytes, 0, footerLen);
        FileMetaData meta = org.apache.parquet.format.Util.readFileMetaData(
            new java.io.ByteArrayInputStream(footerBytes)
        );

        // Validate footer
        assertEquals(100, meta.getNum_rows());
        assertEquals(1, meta.getRow_groups().size());
        int numCols = meta.getRow_groups().get(0).getColumns().size();
        System.out.println("Columns in footer: " + numCols);
        for (int i = 0; i < numCols; i++) {
            System.out.println("  " + meta.getRow_groups().get(0).getColumns().get(i).getMeta_data().getPath_in_schema());
        }
        assertTrue("Expected at least 1 column, got " + numCols, numCols >= 1);

        // Validate KV metadata
        Map<String, String> kv = ParquetFooterWriter.kvToMap(meta.getKey_value_metadata());
        assertEquals("7", kv.get("lucene.codec.version"));
        assertNotNull(kv.get("lucene.segment.id"));

        // Extract Parquet content to test temp dir
        input.seek(parquetStart);
        int parquetLen = (int) (parquetEnd - parquetStart);
        byte[] parquetContent = new byte[parquetLen];
        input.readBytes(parquetContent, 0, parquetLen);

        Path outputFile = tmpDir.resolve("mock_v7.parquet");
        Files.write(outputFile, parquetContent);

        input.close();

        System.out.println("=== Parquet Spark Compat Test ===");
        System.out.println("PAR1 offset: " + par1Offset);
        System.out.println("Parquet content: " + parquetLen + " bytes");
        System.out.println("Footer length: " + footerLen + " bytes");
        System.out.println("Rows: " + meta.getNum_rows());
        System.out.println("Columns: " + meta.getRow_groups().get(0).getColumns().size());
        System.out.println("KV keys: " + kv.keySet());
        System.out.println("Exported to: " + outputFile);
        System.out.println("PDVD_FILE=" + tmpDir.resolve(pdvdFile));
        System.out.println("PARQUET_FILE=" + outputFile);
        System.out.println("Test with: spark-sql -e \"SELECT * FROM parquet.`" + outputFile + "` LIMIT 5;\"");

        // Verify Lucene can still read the data correctly
        DirectoryReader reader = DirectoryReader.open(dir);
        LeafReader leaf = reader.leaves().get(0).reader();
        assertEquals(100, leaf.maxDoc());
        assertNotNull(leaf.getSortedNumericDocValues("timestamp"));
        assertNotNull(leaf.getSortedNumericDocValues("status"));
        assertNotNull(leaf.getSortedSetDocValues("country"));
        reader.close();

        // Print paths so the test runner can find them
        System.out.println("PDVD_PATH=" + tmpDir.resolve(pdvdFile));
        System.out.println("PARQUET_PATH=" + outputFile);
        System.out.println("DATA_PAGE_OFFSET=" + meta.getRow_groups().get(0).getColumns().get(0).getMeta_data().getData_page_offset());

        dir.close();
    }
}
