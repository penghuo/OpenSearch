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

            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertEquals(0.25, accessor.get(0, FlatObjectPath.compile("ratio")));
            assertEquals("info", accessor.get(0, FlatObjectPath.compile("level")));
            assertEquals(Boolean.TRUE, accessor.get(0, FlatObjectPath.compile("ok")));
            assertNull(accessor.get(0, FlatObjectPath.compile("nothing")));
            assertEquals(List.of("a", "b"), accessor.get(0, FlatObjectPath.compile("tags")));
            assertEquals(42, accessor.get(0, FlatObjectPath.compile("nested.deep.value")));
            assertEquals("ns-01", accessor.get(0, FlatObjectPath.compile("['k8s.namespace']")));

            assertTrue(accessor.valueStoreAvailable());
            assertEquals("source", accessor.storeName());
        }
    }

    /**
     * A value comes back with its own JSON type; nothing is coerced towards a requested one, because a schemaless path has
     * no requested one.
     */
    public void testValuesKeepTheirOwnTypes() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertEquals(0.25, accessor.get(0, FlatObjectPath.compile("ratio")));
            assertEquals("info", accessor.get(0, FlatObjectPath.compile("level")));
            assertEquals(Boolean.TRUE, accessor.get(0, FlatObjectPath.compile("ok")));
        }
    }

    public void testMissingPathsAreMissingRatherThanNull() throws Exception {
        try (Directory dir = indexWithSource(DOC); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertSame(PathResolver.MISSING, accessor.get(0, FlatObjectPath.compile("absent")));
            assertSame(PathResolver.MISSING, accessor.get(0, FlatObjectPath.compile("nested.absent")));
            assertSame(PathResolver.MISSING, accessor.get(0, FlatObjectPath.compile("status.absent")));
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
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertSame(PathResolver.MISSING, accessor.get(1, FlatObjectPath.compile("status")));
            assertTrue(accessor.getAll(1).isEmpty());
        }
    }

    public void testMultipleDocumentsInBothDirections() throws Exception {
        String second = "{\"attributes\":{\"status\":404}}";
        String third = "{\"attributes\":{\"status\":500}}";
        try (Directory dir = indexWithSource(DOC, second, third); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            // Forwards.
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertEquals(404, accessor.get(1, FlatObjectPath.compile("status")));
            assertEquals(500, accessor.get(2, FlatObjectPath.compile("status")));
            // Backwards: stored fields are randomly addressable, unlike a doc-values iterator.
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertEquals(500, accessor.get(2, FlatObjectPath.compile("status")));
            assertEquals(404, accessor.get(1, FlatObjectPath.compile("status")));
        }
    }

    /**
     * The per-document cache must not leak across documents; that would be a correctness bug masquerading as speed.
     */
    public void testCacheIsScopedToOneDocument() throws Exception {
        String second = "{\"attributes\":{\"status\":404}}";
        try (Directory dir = indexWithSource(DOC, second); DirectoryReader reader = DirectoryReader.open(dir)) {
            SourceValueAccessor accessor = accessorFor(reader);
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            assertEquals(404, accessor.get(1, FlatObjectPath.compile("status")));
            assertEquals(200, accessor.get(0, FlatObjectPath.compile("status")));
            // The second document has no `level`, so a stale cache would wrongly return "info" here.
            assertSame(PathResolver.MISSING, accessor.get(1, FlatObjectPath.compile("level")));
        }
    }

    /**
     * With {@code _source} disabled there is no value to read, and the accessor reports as much rather than throwing.
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
                assertSame(PathResolver.MISSING, accessor.get(0, FlatObjectPath.compile("status")));
                assertTrue(accessor.getAll(0).isEmpty());
            }
        }
    }
}
