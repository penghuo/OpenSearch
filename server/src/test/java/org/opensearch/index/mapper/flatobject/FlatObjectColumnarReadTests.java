/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The properties the columnar read path rests on, each of which fails silently rather than loudly if it breaks.
 *
 * <p>Kept apart from {@code AccessorEquivalenceTests} because these need what that class deliberately removes: several
 * segments. Its helper force-merges to one and asserts a single leaf, which is right for comparing two readers over
 * identical documents and wrong for everything here.
 */
public class FlatObjectColumnarReadTests extends MapperServiceTestCase {

    private static final String FIELD = "attributes";

    private MapperService service(Version version) throws IOException {
        return createMapperService(version, mapping(b -> b.startObject(FIELD).field("type", "flat_object").endObject()));
    }

    /**
     * Indexes each batch into its own segment and does not merge.
     *
     * <p>Segment count is the variable under test: ordinals are assigned per segment, so the same key name has a different
     * ordinal in each one, and any code that treats an ordinal as global breaks only once there is more than one segment.
     */
    private Directory indexBatches(MapperService mapperService, List<List<String>> batches) throws IOException {
        Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
            for (List<String> batch : batches) {
                for (String source : batch) {
                    ParsedDocument parsed = mapperService.documentMapper().parse(source(source));
                    writer.addDocument(parsed.rootDoc());
                }
                writer.flush();
                writer.commit();
            }
        }
        return dir;
    }

    private static String doc(long status, String extra) {
        return "{\"" + FIELD + "\":{" + extra + "\"status\":" + status + "}}";
    }

    private IndexNumericFieldData fielddata(MapperService mapperService, String path) {
        MappedFieldType keyed = mapperService.fieldType(FIELD + "." + path);
        return (IndexNumericFieldData) keyed.fielddataBuilder("test", () -> null).build(null, null);
    }

    /**
     * A sort must order every document, across segments, in both directions.
     *
     * <p>This is the only test that would catch {@code sortRequiresCustomComparator()} regressing to false. When it does,
     * {@code IndexNumericFieldData.sortField} builds a raw {@code SortedNumericSortField} over a Lucene field named
     * {@code attributes.status}, which does not exist -- and Lucene answers an absent field with an empty iterator rather
     * than an error, so every document sorts as missing and the result comes back in document order. Asserting only the
     * top hit would not notice; asserting the whole ordering does.
     */
    public void testSortOrdersEveryDocumentAcrossSegments() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        // Deliberately not in document order, and split so no segment is internally sorted either. Each batch also carries a
        // different extra key, so the segments have genuinely different name dictionaries.
        List<List<String>> batches = List.of(
            List.of(doc(300, "\"a\":1,"), doc(100, "\"a\":1,")),
            List.of(doc(500, "\"b\":2,"), doc(200, "\"b\":2,")),
            List.of(doc(400, "\"c\":3,"))
        );
        try (Directory dir = indexBatches(mapperService, batches); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertTrue("the point of this test is several segments", reader.leaves().size() > 1);
            IndexSearcher searcher = new IndexSearcher(reader);
            IndexNumericFieldData fielddata = fielddata(mapperService, "status");

            SortField ascending = fielddata.sortField(null, MultiValueMode.MIN, null, false);
            TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(ascending));
            assertEquals(5L, docs.totalHits.value());
            assertEquals(List.of(100.0, 200.0, 300.0, 400.0, 500.0), sortValues(docs));

            SortField descending = fielddata.sortField(null, MultiValueMode.MAX, null, true);
            docs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(descending));
            assertEquals(List.of(500.0, 400.0, 300.0, 200.0, 100.0), sortValues(docs));
        }
    }

    private static List<Double> sortValues(TopFieldDocs docs) {
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < docs.scoreDocs.length; i++) {
            values.add(((Number) ((FieldDoc) docs.scoreDocs[i]).fields[0]).doubleValue());
        }
        return values;
    }

    /**
     * Merging reassigns every ordinal, so the values a path yields must be identical before and after.
     *
     * <p>The read path turns a key name into a field id by finding that name's segment ordinal and taking its position in
     * the document's ordinal list. A merge renumbers the ordinals -- as an order-preserving remap of the union of the
     * segments' dictionaries -- so positions survive and values must not move. If they did, a background merge would
     * silently change query results.
     */
    public void testValuesSurviveAMerge() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        // Segments with disjoint extra keys, so the merged dictionary interleaves them and every ordinal shifts.
        List<List<String>> batches = List.of(
            List.of(doc(11, "\"zebra\":1,"), doc(22, "\"apple\":1,")),
            List.of(doc(33, "\"mango\":2,")),
            List.of(doc(44, "\"banana\":3,\"cherry\":4,"))
        );
        List<Double> before;
        try (Directory dir = indexBatches(mapperService, batches); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertTrue(reader.leaves().size() > 1);
            before = allValues(mapperService, reader);
            assertEquals(List.of(11.0, 22.0, 33.0, 44.0), before);
        }

        try (Directory dir = indexBatches(mapperService, batches)) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force merge should leave one segment", 1, reader.leaves().size());
                assertEquals("a merge must not change what a path yields", before, allValues(mapperService, reader));
            }
        }
    }

    private List<Double> allValues(MapperService mapperService, DirectoryReader reader) throws IOException {
        IndexNumericFieldData fielddata = fielddata(mapperService, "status");
        List<Double> values = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            var view = fielddata.load(leaf).getDoubleValues();
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                if (view.advanceExact(doc)) {
                    for (int i = 0; i < view.docValueCount(); i++) {
                        values.add(view.nextValue());
                    }
                }
            }
        }
        values.sort(null);
        return values;
    }

    /**
     * A document's ordinals must come back ascending and deduplicated.
     *
     * <p>Everything rests on this and nothing else pins it. The read path binary-searches the drained ordinal buffer, which
     * is undefined on unsorted input -- so a violation would not fail, it would return a field id belonging to a different
     * key. It is a Lucene contract, which is exactly why it is worth asserting rather than assuming.
     */
    public void testDocumentOrdinalsAreAscendingAndDeduplicated() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<List<String>> batches = List.of(
            // Keys deliberately out of order in the document, and one key repeated at two depths so that a per-document
            // dictionary entry is shared rather than duplicated in the name column.
            List.of("{\"attributes\":{\"zebra\":1,\"apple\":2,\"value\":3,\"nested\":{\"value\":4},\"mango\":5}}"),
            List.of("{\"attributes\":{\"only\":1}}", "{\"attributes\":{}}")
        );
        try (Directory dir = indexBatches(mapperService, batches); DirectoryReader reader = DirectoryReader.open(dir)) {
            int documentsChecked = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                SortedSetDocValues names = DocValues.getSortedSet(leaf.reader(), FIELD + "._blobnames");
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    if (names.advanceExact(doc) == false) {
                        continue;
                    }
                    documentsChecked++;
                    long previous = -1;
                    for (int i = 0; i < names.docValueCount(); i++) {
                        long ord = names.nextOrd();
                        assertTrue("ordinals must ascend, got " + ord + " after " + previous, ord > previous);
                        previous = ord;
                    }
                }
            }
            assertEquals("both documents with keys should have been checked", 2, documentsChecked);
        }
    }

    /**
     * An index created before the column existed must refuse the aggregation rather than answer it emptily.
     *
     * <p>The values are in {@code _source} on such an index, so serving no values would be a wrong number rather than an
     * absent one -- which is what the ordinary convention for a missing doc-values column would do.
     *
     * <p>The gate is in {@code fielddataBuilder} rather than {@code isAggregatable()}, because the aggregation framework
     * never consults the latter: it calls {@code getForField} first, so whatever the builder throws is what the user sees.
     */
    public void testOlderIndicesRefuseTheAggregation() throws IOException {
        MapperService older = service(Version.V_3_5_0);
        MappedFieldType keyed = older.fieldType(FIELD + ".status");
        assertNotNull(keyed);
        assertFalse("a path with no column behind it is not aggregatable", keyed.isAggregatable());

        IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> keyed.fielddataBuilder("older-index", () -> null)
        );
        assertTrue(refused.getMessage(), refused.getMessage().contains("Cannot aggregate or sort on [attributes.status]"));
        assertTrue("the message should say what to do", refused.getMessage().contains("Reindex"));

        MapperService current = service(Version.CURRENT);
        MappedFieldType currentKeyed = current.fieldType(FIELD + ".status");
        assertTrue("a current index can aggregate the same path", currentKeyed.isAggregatable());
        assertNotNull(currentKeyed.fielddataBuilder("current-index", () -> null));
    }

    /**
     * A segment whose documents have the field but no column must refuse, not answer emptily.
     *
     * <p>The version gate is only a proxy for "the column exists". This is the case where the proxy fails: an index created
     * at a version that should have the column, written by a build where it was optional. It is not hypothetical -- a
     * prototype-built index did exactly this, and the aggregation returned a confident 0.0 over a million documents.
     */
    public void testASegmentWithDocumentsButNoColumnIsRefused() throws IOException {
        MapperService current = service(Version.CURRENT);
        // Build the index with a mapper that does not write the columns, then read it with one that expects them -- which is
        // precisely the shape of a prototype-era index opened by a current node.
        MapperService writerWithoutColumns = service(Version.V_3_5_0);
        try (
            Directory dir = indexBatches(writerWithoutColumns, List.of(List.of(doc(1, ""))));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertNotNull("the field's terms are present", leaf.reader().getFieldInfos().fieldInfo(FIELD));
            assertNull("but the column is not", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));

            // Refused when the value view is opened, before any document is touched.
            var leafData = fielddata(current, "status").load(leaf);
            IllegalStateException refused = expectThrows(IllegalStateException.class, leafData::getLongValues);
            assertTrue(refused.getMessage(), refused.getMessage().contains("no [" + FIELD + "._blob] column"));
            assertTrue("the message should say what to do", refused.getMessage().contains("Reindex"));
        }
    }

    /** The parent field stays un-aggregatable, whatever the version, because there is no single value to aggregate. */
    public void testTheParentFieldIsNeverAggregatable() throws IOException {
        for (Version version : List.of(Version.V_3_5_0, Version.CURRENT)) {
            MapperService mapperService = service(version);
            assertFalse("the parent of " + version, mapperService.fieldType(FIELD).isAggregatable());
        }
    }

    /** An older index keeps writing exactly what it wrote before: no columns, so nothing new to reject or read. */
    public void testOlderIndicesWriteNoColumns() throws IOException {
        MapperService older = service(Version.V_3_5_0);
        try (Directory dir = indexBatches(older, List.of(List.of(doc(1, "")))); DirectoryReader reader = DirectoryReader.open(dir)) {
            var infos = reader.leaves().get(0).reader().getFieldInfos();
            assertNull("no value column on a pre-gate index", infos.fieldInfo(FIELD + "._blob"));
            assertNull("no name column on a pre-gate index", infos.fieldInfo(FIELD + "._blobnames"));
            assertNotNull("but the field's own terms are unchanged", infos.fieldInfo(FIELD));
        }
    }

    /** And a current index writes both, and only those two. */
    public void testCurrentIndicesWriteBothColumns() throws IOException {
        MapperService current = service(Version.CURRENT);
        try (Directory dir = indexBatches(current, List.of(List.of(doc(1, "")))); DirectoryReader reader = DirectoryReader.open(dir)) {
            var infos = reader.leaves().get(0).reader().getFieldInfos();
            assertNotNull(infos.fieldInfo(FIELD + "._blob"));
            assertNotNull(infos.fieldInfo(FIELD + "._blobnames"));
            assertNull("ordering the field ids makes a third column unnecessary", infos.fieldInfo(FIELD + "._blobmeta"));
        }
    }

    /** A path absent from one segment but present in another must not disturb the segment that has it. */
    public void testAPathPresentInOnlySomeSegments() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<List<String>> batches = List.of(
            List.of("{\"attributes\":{\"status\":7}}"),
            List.of("{\"attributes\":{\"other\":1}}"),
            List.of("{\"attributes\":{\"status\":9}}")
        );
        try (Directory dir = indexBatches(mapperService, batches); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(List.of(7.0, 9.0), allValues(mapperService, reader));

            // The segment without the key serves nothing, and does so from the term-dictionary miss rather than by decoding
            // its documents.
            IndexNumericFieldData fielddata = fielddata(mapperService, "status");
            int emptyLeaves = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                SortedNumericDocValues values = fielddata.load(leaf).getLongValues();
                boolean any = false;
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    any |= values.advanceExact(doc);
                }
                if (any == false) {
                    emptyLeaves++;
                }
            }
            assertEquals("exactly the segment without the key", 1, emptyLeaves);
        }
    }
}
