/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SourceValueAccessorTests extends OpenSearchTestCase {

    private static final String DOC = "{"
        + "\"@timestamp\":1755720000000,"
        + "\"body\":\"hello\","
        + "\"attributes\":{"
        + "\"status\":200,"
        + "\"ratio\":0.25,"
        + "\"level\":\"info\","
        + "\"ok\":true,"
        + "\"nothing\":null,"
        + "\"tags\":[\"a\",\"b\"],"
        + "\"nested\":{\"deep\":{\"value\":42}},"
        + "\"k8s.namespace\":\"ns-01\""
        + "}}";

    /**
     * Builds a single-segment index whose documents carry only a {@code _source} stored field.
     */
    private Directory indexWithSource(String... sources) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String source : sources) {
                Document doc = new Document();
                doc.add(new StoredField(SourceFieldMapper.NAME, new BytesRef(source.getBytes(StandardCharsets.UTF_8))));
                writer.addDocument(doc);
            }
            // One segment, so document ids follow insertion order and the assertions below can address them directly.
            writer.forceMerge(1);
        }
        return dir;
    }

    private SourceValueAccessor accessorFor(DirectoryReader reader) throws Exception {
        assertEquals("expected a single segment", 1, reader.leaves().size());
        LeafReaderContext context = reader.leaves().get(0);
        SourceValueAccessor accessor = new SourceValueAccessor("attributes");
        accessor.setNextReader(context);
        return accessor;
    }

    public void testReadsEveryValueType() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);

            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            assertEquals(0.25, accessor.get(0, "ratio", ValueType.DOUBLE));
            assertEquals("info", accessor.get(0, "level", ValueType.STRING));
            assertEquals(Boolean.TRUE, accessor.get(0, "ok", ValueType.BOOLEAN));
            assertNull(accessor.get(0, "nothing", ValueType.STRING));
            assertEquals(List.of("a", "b"), accessor.get(0, "tags", ValueType.RAW));
            assertEquals(42L, accessor.get(0, "nested.deep.value", ValueType.LONG));
            assertEquals("ns-01", accessor.get(0, "k8s.namespace", ValueType.STRING));

            assertEquals("no value should have failed coercion", 0L, accessor.coercionFailures());
            assertTrue(accessor.valueStoreAvailable());
            assertEquals("source", accessor.storeName());
        }
    }

    public void testCrossTypeCoercion() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            // The number read as a string, and the double read as a long.
            assertEquals("200", accessor.get(0, "status", ValueType.STRING));
            assertEquals(0L, accessor.get(0, "ratio", ValueType.LONG));
            assertEquals(200.0, accessor.get(0, "status", ValueType.DOUBLE));
        }
    }

    public void testCoercionFailureIsCounted() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertNull("a word is not a number", accessor.get(0, "level", ValueType.LONG));
            assertEquals(1L, accessor.coercionFailures());
            // An absent path is not a coercion failure.
            assertNull(accessor.get(0, "no_such_key", ValueType.LONG));
            assertEquals(1L, accessor.coercionFailures());
            // Nor is a stored null.
            assertNull(accessor.get(0, "nothing", ValueType.LONG));
            assertEquals(1L, accessor.coercionFailures());
        }
    }

    public void testMissingPathsReturnNull() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertNull(accessor.get(0, "absent", ValueType.STRING));
            assertNull(accessor.get(0, "nested.absent", ValueType.STRING));
            assertNull(accessor.get(0, "status.absent", ValueType.STRING));
        }
    }

    public void testGetAllReconstructsWholeValue() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            Map<String, Object> all = accessor.getAll(0);
            assertEquals(8, all.size());
            assertEquals("info", all.get("level"));
            assertEquals("ns-01", all.get("k8s.namespace"));
            assertTrue(all.containsKey("nothing"));
            assertNull(all.get("nothing"));
            assertEquals(Map.of("deep", Map.of("value", 42)), all.get("nested"));
        }
    }

    public void testDocumentWithoutTheFieldYieldsNothing() throws Exception {
        String other = "{\"body\":\"no attributes here\"}";
        try (Directory dir = indexWithSource(DOC, other); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            assertNull(accessor.get(1, "status", ValueType.LONG));
            assertTrue(accessor.getAll(1).isEmpty());
        }
    }

    public void testMultipleDocumentsInBothDirections() throws Exception {
        String second = "{\"attributes\":{\"status\":404}}";
        String third = "{\"attributes\":{\"status\":500}}";
        try (Directory dir = indexWithSource(DOC, second, third); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            // Forwards.
            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            assertEquals(404L, accessor.get(1, "status", ValueType.LONG));
            assertEquals(500L, accessor.get(2, "status", ValueType.LONG));
            // Backwards: stored fields are randomly addressable, unlike a doc-values iterator.
            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            assertEquals(500L, accessor.get(2, "status", ValueType.LONG));
            assertEquals(404L, accessor.get(1, "status", ValueType.LONG));
        }
    }

    /**
     * The per-document cache must not leak across documents; that would be a correctness bug masquerading as speed.
     */
    public void testCacheIsScopedToOneDocument() throws Exception {
        String second = "{\"attributes\":{\"status\":404}}";
        try (Directory dir = indexWithSource(DOC, second); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            assertEquals(404L, accessor.get(1, "status", ValueType.LONG));
            assertEquals(200L, accessor.get(0, "status", ValueType.LONG));
            // The second document has no `level`, so a stale cache would wrongly return "info" here.
            assertNull(accessor.get(1, "level", ValueType.STRING));
        }
    }

    /**
     * Test C3.1: with {@code _source} disabled there is no value to read, and this arm reports as much rather than
     * throwing.
     */
    public void testSourceDisabled() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = newIndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                Document doc = new Document();
                doc.add(new StringField("other", "value", org.apache.lucene.document.Field.Store.NO));
                writer.addDocument(doc);
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SourceValueAccessor accessor = accessorFor(reader);
                assertFalse("no _source field exists in this index", accessor.valueStoreAvailable());
                assertNull(accessor.get(0, "status", ValueType.LONG));
                assertTrue(accessor.getAll(0).isEmpty());
            }
        }
    }
}
