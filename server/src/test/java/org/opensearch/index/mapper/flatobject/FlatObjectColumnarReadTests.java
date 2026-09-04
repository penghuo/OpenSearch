/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
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
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
     * <p>The version gate is only a proxy for "the column exists". Where the two disagree, Lucene answers an absent
     * doc-values field with an empty iterator rather than an error, so the aggregation returns a confident 0.0 over
     * documents that do have values -- which no other test here would catch.
     */
    public void testASegmentWithDocumentsButNoColumnIsRefused() throws IOException {
        MapperService current = service(Version.CURRENT);
        // Build the index with a mapper that does not write the columns, then read it with one that expects them.
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

    /**
     * Two keys that differ as text but not as UTF-8 must be refused, not silently misread.
     *
     * <p>The encoder's dictionary is keyed by String while the name column stores UTF-8 and Lucene deduplicates a
     * document's entries by those bytes. An unpaired surrogate encodes to the same byte as a literal question mark, so such
     * a document would write fewer ordinals than it has field ids -- and field id i would stop meaning ordinal i, returning
     * another key's value for every key above the collision.
     */
    public void testKeysThatCollideInUtf8CannotReachTheEncoder() throws IOException {
        // Separate objects, so a per-object duplicate-key check would not catch it first.
        String json = "{\"attributes\":{\"a\":1,\"o1\":{\"\\ud800\":1},\"o2\":{\"?\":2}}}";
        // The XContent parser refuses a lone surrogate in a property name, so the encoder never sees the collision -- and,
        // what matters here, it refuses it identically whether or not the index writes columns. The guard in the write path
        // stays as defence for any caller that does not come through a parser.
        for (Version version : List.of(Version.V_3_5_0, Version.CURRENT)) {
            MapperService mapperService = service(version);
            MapperParsingException refused = expectThrows(
                MapperParsingException.class,
                () -> mapperService.documentMapper().parse(source(json))
            );
            assertTrue(stackTraceOf(refused), stackTraceOf(refused).contains("Broken surrogate pair"));
        }
    }

    /**
     * A top-level array of objects must be accepted, because {@code flat_object} has always accepted one.
     *
     * <p>It flattens every element into the same term set, so a document like this is ordinary for the field. One
     * doc-values column per document cannot hold an element at a time, which is why the whole array is taken and encoded as
     * a Variant array -- and why the read path descends it.
     */
    public void testATopLevelArrayOfObjectsIsIndexedAndReadable() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":[{\"status\":7,\"host\":\"a\"},{\"code\":9}]}";

        // Byte-for-byte the terms an index with no columns writes.
        assertEquals(
            termsOf(service(Version.V_3_5_0).documentMapper().parse(source(json))),
            termsOf(mapperService.documentMapper().parse(source(json)))
        );

        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            // A key from either element resolves, which is only true if the reader descends the array.
            SortedNumericDocValues status = fielddata(mapperService, "status").load(leaf).getLongValues();
            assertTrue(status.advanceExact(0));
            assertEquals(7L, status.nextValue());
            SortedNumericDocValues code = fielddata(mapperService, "code").load(leaf).getLongValues();
            assertTrue(code.advanceExact(0));
            assertEquals(9L, code.nextValue());
        }
    }

    /** A key repeated across the elements is multi-valued, which is what the field's terms hold for it too. */
    public void testAKeyRepeatedAcrossArrayElementsIsMultiValued() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":[{\"status\":7},{\"status\":3}]}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            SortedNumericDocValues status = fielddata(mapperService, "status").load(reader.leaves().get(0)).getLongValues();
            assertTrue(status.advanceExact(0));
            assertEquals(2, status.docValueCount());
            // Ascending, because MultiValueMode.MIN takes the first value.
            assertEquals(3L, status.nextValue());
            assertEquals(7L, status.nextValue());
        }
    }

    /**
     * An array of objects <em>inside</em> the value is descended too, and this is the case that was silently wrong.
     *
     * <p>The write path has always encoded it faithfully; the read path used to stop at the array and report the path
     * absent, while a search for the same path matched. Two stores disagreeing with no error is the worst outcome
     * available, so it is pinned here.
     */
    public void testAnArrayOfObjectsInsideTheValueIsReadable() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{\"spans\":[{\"ms\":40},{\"ms\":10}]}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            SortedNumericDocValues ms = fielddata(mapperService, "spans.ms").load(reader.leaves().get(0)).getLongValues();
            assertTrue("the terms hold spans.ms, so the column must too", ms.advanceExact(0));
            assertEquals(2, ms.docValueCount());
            assertEquals(10L, ms.nextValue());
            assertEquals(40L, ms.nextValue());
        }
    }

    /** doc['attributes'] is typed as a map, so a top-level array reads as the union its terms already present. */
    public void testATopLevelArrayReadsAsTheUnionOfItsElements() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":[{\"status\":7,\"host\":\"a\"},{\"status\":3,\"code\":9}]}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            var parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            var scriptValues = parent.load(reader.leaves().get(0)).getScriptValues();
            scriptValues.setNextDocId(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> value = (Map<String, Object>) scriptValues.get(0);

            assertEquals(3, value.size());
            assertEquals("a", value.get("host"));
            assertEquals(9L, value.get("code"));
            // Repeated across elements: the first occurrence, which is what a path read returns as well.
            assertEquals(7L, value.get("status"));
            assertEquals(Map.of("status", 7L, "host", "a", "code", 9L), Map.copyOf(value));
        }
    }

    /** An empty array indexes nothing, exactly as it did when the array was split into elements and there were none. */
    public void testAnEmptyTopLevelArrayIndexesNoTerms() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":[]}";
        assertEquals(
            termsOf(service(Version.V_3_5_0).documentMapper().parse(source(json))),
            termsOf(mapperService.documentMapper().parse(source(json)))
        );
        assertEquals(List.of(), termsOf(mapperService.documentMapper().parse(source(json))));
    }

    /** A bare scalar in the array has no key, so it has no term to go in and is refused -- unchanged in both directions. */
    public void testAScalarInATopLevelArrayIsRefusedByBothVersions() throws IOException {
        String json = "{\"attributes\":[{\"a\":1},\"loose\"]}";
        for (Version version : List.of(Version.V_3_5_0, Version.CURRENT)) {
            MapperService mapperService = service(version);
            expectThrows(Exception.class, () -> mapperService.documentMapper().parse(source(json)));
        }
    }

    /**
     * A key named after one of the columns needs no special handling.
     *
     * <p>{@code attributes._blob} as a Lucene field is the column; as a key it is a name inside the value tree and a term in
     * the field's own terms. Two namespaces, so a document using the name indexes and reads like any other.
     */
    public void testAKeyNamedLikeAColumnIsReadable() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{\"_blob\":5,\"_blobnames\":6}}";
        assertEquals(
            termsOf(service(Version.V_3_5_0).documentMapper().parse(source(json))),
            termsOf(mapperService.documentMapper().parse(source(json)))
        );
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            SortedNumericDocValues blob = fielddata(mapperService, "_blob").load(leaf).getLongValues();
            assertTrue(blob.advanceExact(0));
            assertEquals(5L, blob.nextValue());
            SortedNumericDocValues names = fielddata(mapperService, "_blobnames").load(leaf).getLongValues();
            assertTrue(names.advanceExact(0));
            assertEquals(6L, names.nextValue());
        }
    }

    /**
     * More keys than the name column will take must not cost the document either.
     *
     * <p>The bound is a guard on how long a segment's per-document name lists can get, not a limit of the encoding. Passing
     * it means the blob is dropped for that document, which is the same trade as every other unencodable case: the terms
     * are unchanged, so the document indexes and searches exactly as it does on an index with no columns.
     */
    public void testMoreKeysThanTheColumnTakesIsIndexedWithoutABlob() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        StringBuilder json = new StringBuilder("{\"attributes\":{");
        // One past FlatObjectFieldMapper.MAX_KEYS_PER_DOCUMENT, which is package-private in the mapper's own package.
        for (int i = 0; i <= 0xFFFF; i++) {
            json.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
        }
        json.append("}}");
        String source = json.toString();

        assertEquals(
            termsOf(service(Version.V_3_5_0).documentMapper().parse(source(source))),
            termsOf(mapperService.documentMapper().parse(source(source)))
        );

        try (Directory dir = indexBatches(mapperService, List.of(List.of(source))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            // The column is present -- so the segment is not mistaken for a broken one -- and holds nothing readable.
            assertNotNull(leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));
            assertFalse(fielddata(mapperService, "k0").load(leaf).getLongValues().advanceExact(0));
        }
    }

    /**
     * An array whose elements repeat a key writes the same distinct terms, but not the same number of them.
     *
     * <p>The one place the two routes differ. Element-at-a-time calls the mapper once per element, so the field's
     * path-name entries -- {@code attributes.host}, {@code attributes.status} -- are written once per element; taking the
     * array whole writes them once for the document. Nothing matches differently: that field is indexed {@code DOCS}-only
     * so no frequency is recorded, and its doc values are {@code SORTED_SET}, which deduplicates within a document. The
     * new route simply writes fewer duplicate postings.
     *
     * <p>Pinned because "the same distinct terms" is the property that actually holds. Asserting the stronger one passes
     * or fails depending on whether the test's array happens to repeat a key across elements.
     */
    public void testAnArrayRepeatingAKeyWritesTheSameDistinctTerms() throws IOException {
        String json = "{\"attributes\":[{\"host\":\"a\",\"status\":200},{\"host\":\"b\",\"status\":500}]}";
        List<String> before = termsOf(service(Version.V_3_5_0).documentMapper().parse(source(json)));
        List<String> after = termsOf(service(Version.CURRENT).documentMapper().parse(source(json)));

        assertEquals("the same distinct terms", new HashSet<>(before), new HashSet<>(after));
        // Two path names, each written once per element rather than once per document, as a term and as a doc value.
        assertEquals(before.size() - 4, after.size());
        for (String term : after) {
            assertTrue(term + " must already be written today", before.contains(term));
        }
    }

    /**
     * The field's terms for a document, as an index with no columns would write them.
     *
     * <p>Comparing these between a pre-gate and a current index is the test for "nothing a document could contain is newly
     * refused, and nothing it indexes changes": everything the blob adds lives in other Lucene fields.
     *
     * <p>Returned in order, so a caller can compare the list or its distinct set. The two differ in exactly one case, which
     * {@code testAnArrayRepeatingAKeyWritesTheSameDistinctTerms} pins.
     */
    private static List<String> termsOf(ParsedDocument parsed) {
        List<String> out = new ArrayList<>();
        for (IndexableField field : parsed.rootDoc()) {
            if (field.name().equals(FIELD) == false
                && field.name().equals(FIELD + "._value") == false
                && field.name().equals(FIELD + "._valueAndPath") == false) {
                continue;
            }
            BytesRef bytes = field.binaryValue();
            out.add(
                field.name() + '|' + field.getClass().getSimpleName() + '|' + (bytes == null ? field.stringValue() : bytes.utf8ToString())
            );
        }
        Collections.sort(out);
        return out;
    }

    /**
     * An integer too wide for the Variant encoding must not cost the document.
     *
     * <p>Variant's widest integer is decimal16, so past 128 bits there is nothing to store it in. Refusing the document
     * would be a regression: {@code flat_object} accepts it today and keeps it as a term. So the blob keeps the text, which
     * is what the terms hold anyway, and the two stores agree.
     */
    public void testAnIntegerTooWideForTheEncodingIsKeptAsText() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String wide = "9".repeat(60);
        String json = "{\"attributes\":{\"huge\":" + wide + ",\"status\":1}}";
        // Indexes rather than failing.
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            SortedBinaryDocValues values = fielddata(mapperService, "huge").load(leaf).getBytesValues();
            assertTrue("the value must be readable", values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(wide, values.nextValue().utf8ToString());

            // And the rest of the document is unaffected.
            SortedNumericDocValues status = fielddata(mapperService, "status").load(leaf).getLongValues();
            assertTrue(status.advanceExact(0));
            assertEquals(1L, status.nextValue());
        }
    }

    /**
     * A value read from doc[] must not outlive the document it came from.
     *
     * <p>The map is a view over the reader's live cursor, which is how {@code ScriptDocValues} is meant to be used. If a
     * caller ever holds one across a document boundary it would read a different document's value, so the view says so
     * instead.
     */
    public void testAValueCannotBeReadAfterItsDocumentIsPastanother() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<List<String>> batches = List.of(List.of(doc(11, ""), doc(22, "")));
        try (Directory dir = indexBatches(mapperService, batches); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            var parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            var scriptValues = parent.load(leaf).getScriptValues();

            scriptValues.setNextDocId(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) scriptValues.get(0);
            assertEquals(11L, first.get("status"));

            scriptValues.setNextDocId(1);
            IllegalStateException stale = expectThrows(IllegalStateException.class, () -> first.get("status"));
            assertTrue(stale.getMessage(), stale.getMessage().contains("already moved past"));
        }
    }

    /** A segment carrying the superseded metadata column must be refused by both readers, not read as if ordered. */
    public void testASegmentWithTheSupersededMetaColumnIsRefused() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
                ParsedDocument parsed = mapperService.documentMapper().parse(source(doc(1, "")));
                parsed.rootDoc().add(new BinaryDocValuesField(FIELD + "._blobmeta", new BytesRef(new byte[] { 1, 1, 0 })));
                writer.addDocument(parsed.rootDoc());
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);

                var keyed = fielddata(mapperService, "status").load(leaf);
                IllegalStateException perPath = expectThrows(IllegalStateException.class, keyed::getLongValues);
                assertTrue(perPath.getMessage(), perPath.getMessage().contains("_blobmeta"));

                var parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
                IllegalStateException wholeObject = expectThrows(IllegalStateException.class, () -> parent.load(leaf).getScriptValues());
                assertTrue(wholeObject.getMessage(), wholeObject.getMessage().contains("_blobmeta"));
            }
        }
    }

    private static String stackTraceOf(Throwable throwable) {
        StringBuilder text = new StringBuilder();
        for (Throwable at = throwable; at != null && at != at.getCause(); at = at.getCause()) {
            text.append(at).append('\n');
        }
        return text.toString();
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
