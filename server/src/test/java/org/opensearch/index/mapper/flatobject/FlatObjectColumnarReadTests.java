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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.variant.Variant;
import org.opensearch.common.variant.VariantBuilder;
import org.opensearch.common.variant.VariantMetadata;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The properties the columnar read path rests on, each of which fails silently rather than loudly if it breaks.
 *
 * <p>Kept apart from {@link AccessorEquivalenceTests} because these need what that class deliberately removes: several
 * segments, and documents written by a mapper of a different version.
 */
public class FlatObjectColumnarReadTests extends MapperServiceTestCase {

    private static final String FIELD = "attributes";

    private MapperService service(Version version) throws IOException {
        return service(version, true);
    }

    private MapperService service(Version version, boolean variantDocValues) throws IOException {
        Settings settings = Settings.builder()
            .put("index.version.created", version)
            .put(FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getKey(), variantDocValues)
            .build();
        return createMapperService(settings, mapping(b -> b.startObject(FIELD).field("type", "flat_object").endObject()));
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
                    writer.addDocument(mapperService.documentMapper().parse(source(source)).rootDoc());
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

    private static IndexFieldData<?> fielddata(MapperService mapperService, String path) {
        MappedFieldType keyed = mapperService.fieldType(FIELD + "." + path);
        assertNotNull("no field type for [" + path + "]", keyed);
        return keyed.fielddataBuilder("test", () -> null).build(null, null);
    }

    private static List<Object> fetch(MapperService mapperService, LeafReaderContext leaf, String path, int docId) throws IOException {
        MappedFieldType keyed = mapperService.fieldType(FIELD + "." + path);
        assertNotNull("no field type for [" + path + "]", keyed);
        var fetcher = fielddata(mapperService, path).load(leaf).getLeafValueFetcher(keyed.docValueFormat(null, null));
        if (fetcher.advanceExact(docId) == false) {
            return List.of();
        }
        List<Object> fetched = new ArrayList<>(fetcher.docValueCount());
        for (int i = 0, count = fetcher.docValueCount(); i < count; i++) {
            fetched.add(fetcher.nextValue());
        }
        return fetched;
    }

    private static Object read(MapperService mapperService, LeafReaderContext leaf, String path, int docId) throws IOException {
        ScriptDocValues<?> values = fielddata(mapperService, path).load(leaf).getScriptValues();
        values.setNextDocId(docId);
        return values.isEmpty() ? PathResolver.MISSING : values.get(0);
    }

    private static List<Object> readAll(MapperService mapperService, DirectoryReader reader, String path) throws IOException {
        List<Object> out = new ArrayList<>();
        IndexFieldData<?> fielddata = fielddata(mapperService, path);
        for (LeafReaderContext leaf : reader.leaves()) {
            ScriptDocValues<?> values = fielddata.load(leaf).getScriptValues();
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                values.setNextDocId(doc);
                if (values.isEmpty() == false) {
                    out.add(values.get(0));
                }
            }
        }
        return out;
    }

    public void testScriptDocValuesSizeIsPresenceNotCardinality() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<String> docs = List.of(
            "{\"attributes\":{\"other\":1}}",
            "{\"attributes\":{\"events\":[]}}",
            "{\"attributes\":{\"events\":[{\"status\":1},{\"status\":2}]}}"
        );
        try (Directory dir = indexBatches(mapperService, List.of(docs)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            ScriptDocValues<?> values = fielddata(mapperService, "events{}.status").load(leaf).getScriptValues();

            values.setNextDocId(0);
            assertEquals("no events at all is absent", 0, values.size());
            assertTrue(values.isEmpty());
            expectThrows(IndexOutOfBoundsException.class, () -> values.get(0));

            values.setNextDocId(1);
            assertEquals("an empty array is present", 1, values.size());
            assertEquals("and its wildcard result is an empty list", List.of(), values.get(0));

            values.setNextDocId(2);
            assertEquals("a populated array is still one value", 1, values.size());
            assertEquals(List.of(1L, 2L), values.get(0));
        }
    }

    /**
     * A whole-object value must stay readable after the reader has moved on.
     *
     * <p>The fetch phase does not consume a script field where it produces it: it can hold hit 1's value, advance the doc
     * values to hit 2, and serialise hit 1 afterwards. A value that reads live reader state -- the reused ordinal buffer, or
     * doc-values bytes the codec may have overwritten -- would then either fail or, worse, return hit 2's data under hit 1's
     * id. So a returned value has to be independent of the reader once handed over.
     */
    public void testAWholeObjectValueSurvivesTheReaderMovingOn() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<String> docs = List.of(
            "{\"attributes\":{\"region\":\"us-east-1\",\"status\":1}}",
            "{\"attributes\":{\"region\":\"eu-west-1\",\"status\":2}}"
        );
        try (Directory dir = indexBatches(mapperService, List.of(docs)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            IndexFieldData<?> parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            ScriptDocValues<?> values = parent.load(leaf).getScriptValues();

            // Take every document's value first, the way a fetch phase collects hits, then read them all afterwards.
            List<Object> held = new ArrayList<>();
            for (int docId = 0; docId < docs.size(); docId++) {
                values.setNextDocId(docId);
                assertEquals("doc " + docId + " must have a value", 1, values.size());
                held.add(values.get(0));
            }

            Map<?, ?> first = (Map<?, ?>) held.get(0);
            assertEquals("us-east-1", first.get("region"));
            assertEquals(1L, first.get("status"));
            assertEquals("and enumerating it still works", Set.of("region", "status"), first.keySet());

            Map<?, ?> second = (Map<?, ?>) held.get(1);
            assertEquals("eu-west-1", second.get("region"));
            assertEquals(2L, second.get("status"));
        }
    }

    /**
     * A whole-object value must outlive the reader itself, not merely the document.
     *
     * <p>PPL may hold a row past the search context, and the response is serialised after the context closes. A value still
     * resolving key names through the segment's term dictionary would be reading closed Lucene state by then -- which is why
     * the whole object is decoded before it is handed over rather than viewed.
     */
    public void testAWholeObjectValueSurvivesTheReaderBeingClosed() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{\"region\":\"us-east-1\",\"nested\":{\"deep\":[1,2]},\"status\":200}}";
        Object held;
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json)))) {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexFieldData<?> parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
                ScriptDocValues<?> values = parent.load(reader.leaves().get(0)).getScriptValues();
                values.setNextDocId(0);
                held = values.get(0);
            }
            Map<?, ?> map = (Map<?, ?>) held;
            assertEquals("us-east-1", map.get("region"));
            assertEquals(200L, map.get("status"));
            assertEquals(Set.of("region", "nested", "status"), map.keySet());
            assertEquals(3, map.size());
            Map<?, ?> nested = (Map<?, ?>) map.get("nested");
            assertEquals(List.of(1L, 2L), nested.get("deep"));
            assertEquals("{nested={deep=[1, 2]}, region=us-east-1, status=200}", map.toString());
        }
    }

    /** The whole matrix of spath reads on the design document, through the real fielddata. */
    public void testEverySelectorFormReadsThroughFielddata() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{"
            + "\"region\":\"us-east-1\","
            + "\"sub\":{\"center\":1},"
            + "\"events\":[{\"status\":1},{\"status\":2}],"
            + "\"matrix\":[[{\"value\":1}],[{\"value\":2}]],"
            + "\"cluster.name\":\"c1\","
            + "\"events{}\":[{\"status\":99}]"
            + "}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);

            assertEquals("us-east-1", read(mapperService, leaf, "region", 0));
            assertEquals(Map.of("center", 1L), read(mapperService, leaf, "sub", 0));
            assertEquals(List.of(Map.of("status", 1L), Map.of("status", 2L)), read(mapperService, leaf, "events", 0));
            assertEquals(List.of(1L, 2L), read(mapperService, leaf, "events{}.status", 0));
            assertEquals(1L, read(mapperService, leaf, "events{0}.status", 0));
            assertEquals(2L, read(mapperService, leaf, "events{1}.status", 0));
            assertEquals(List.of(1L, 2L), read(mapperService, leaf, "matrix{}{}.value", 0));

            // No implicit traversal.
            assertSame(PathResolver.MISSING, read(mapperService, leaf, "events.status", 0));
            // A quoted literal key, and the same string unquoted meaning nesting.
            assertEquals("c1", read(mapperService, leaf, "['cluster.name']", 0));
            assertSame(PathResolver.MISSING, read(mapperService, leaf, "cluster.name", 0));
            // A literal key that looks like a selector.
            assertEquals(List.of(99L), read(mapperService, leaf, "['events{}']{}.status", 0));
        }
    }

    /**
     * A literal dotted key and the equivalent nested path remain distinct in Variant even though legacy flat_object terms
     * collapse them onto one multi-valued query path.
     */
    public void testDottedKeyCollisionKeepsStructuralPathsDistinct() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{\"cluster.name\":\"c1\",\"cluster\":{\"name\":\"c2\"}}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);

            assertEquals("c1", read(mapperService, leaf, "['cluster.name']", 0));
            assertEquals("c2", read(mapperService, leaf, "cluster.name", 0));
            assertEquals("docvalue_fields uses the same structural path", List.of("c1"), fetch(mapperService, leaf, "['cluster.name']", 0));
            assertEquals(List.of("c2"), fetch(mapperService, leaf, "cluster.name", 0));
        }
    }

    /** Variant-backed {@code docvalue_fields} returns one typed logical value rather than scanning flattened strings. */
    public void testDocValueFetcherReadsTypedVariantValues() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<String> docs = List.of(
            "{\"attributes\":{"
                + "\"region\":\"us-east-1\","
                + "\"sub\":{\"center\":1},"
                + "\"events\":[{\"status\":1},{\"status\":2}],"
                + "\"flag\":true,"
                + "\"ratio\":0.25,"
                + "\"nothing\":null"
                + "}}",
            "{\"attributes\":{\"other\":1}}"
        );
        try (Directory dir = indexBatches(mapperService, List.of(docs)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);

            assertEquals(List.of("us-east-1"), fetch(mapperService, leaf, "region", 0));
            assertEquals(List.of(1L), fetch(mapperService, leaf, "sub.center", 0));
            assertEquals(List.of(Map.of("center", 1L)), fetch(mapperService, leaf, "sub", 0));
            assertEquals(List.of(List.of(1L, 2L)), fetch(mapperService, leaf, "events{}.status", 0));
            assertEquals(List.of(Boolean.TRUE), fetch(mapperService, leaf, "flag", 0));

            List<Object> ratio = fetch(mapperService, leaf, "ratio", 0);
            assertEquals(1, ratio.size());
            assertEquals(0.25, ((Number) ratio.get(0)).doubleValue(), 0.0);

            List<Object> explicitNull = fetch(mapperService, leaf, "nothing", 0);
            assertEquals(1, explicitNull.size());
            assertNull(explicitNull.get(0));

            assertEquals(List.of(), fetch(mapperService, leaf, "missing", 0));
            assertEquals(List.of(), fetch(mapperService, leaf, "region", 1));
        }
    }

    /** Every JSON type comes back as itself, including the two exact numeric ones. */
    public void testValuesKeepTheirJsonTypes() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{"
            + "\"i\":200,"
            + "\"d\":1.5,"
            + "\"b\":true,"
            + "\"s\":\"text\","
            + "\"o\":{\"k\":1},"
            + "\"a\":[1,2],"
            + "\"big\":92233720368547758070"
            + "}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertEquals(Long.class, read(mapperService, leaf, "i", 0).getClass());
            assertEquals(200L, read(mapperService, leaf, "i", 0));
            assertEquals(1.5, ((Number) read(mapperService, leaf, "d", 0)).doubleValue(), 0.0);
            assertEquals(Boolean.TRUE, read(mapperService, leaf, "b", 0));
            assertEquals("text", read(mapperService, leaf, "s", 0));
            assertEquals(Map.of("k", 1L), read(mapperService, leaf, "o", 0));
            assertEquals(List.of(1L, 2L), read(mapperService, leaf, "a", 0));
            // Past int64 the value is kept exactly, as a scale-zero decimal.
            assertEquals(new java.math.BigInteger("92233720368547758070"), read(mapperService, leaf, "big", 0));
        }
    }

    /**
     * An aggregation and a sort are both refused, and the aggregation keeps the message this field has always produced.
     *
     * <p>The mechanism is deliberate. {@code getValuesSourceType()} reports a type no aggregation registers for, so
     * {@code ValuesSourceRegistry} refuses with <em>"is not supported for aggregation"</em> -- the released wording, and the
     * same for every aggregation. Throwing from the values-source type instead would replace that with something new.
     * Nothing can reach values through the type either, because the leaf's bytes view refuses.
     */
    public void testAggregationAndSortAreRefused() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        try (
            Directory dir = indexBatches(mapperService, List.of(List.of(doc(1, ""))));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);

            for (String path : List.of("status", "events{}.status")) {
                IndexFieldData<?> keyed = fielddata(mapperService, path);
                assertEquals("flat_object", keyed.getValuesSourceType().typeName());
                assertFalse(
                    "no aggregation may register for this type",
                    keyed.getValuesSourceType() == CoreValuesSourceType.BYTES || keyed.getValuesSourceType() == CoreValuesSourceType.NUMERIC
                );
                expectThrows(IllegalArgumentException.class, () -> keyed.sortField(null, MultiValueMode.MIN, null, false));
                expectThrows(IllegalArgumentException.class, () -> keyed.load(leaf).getBytesValues());
            }

            IndexFieldData<?> parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            assertEquals("flat_object", parent.getValuesSourceType().typeName());
            expectThrows(IllegalArgumentException.class, () -> parent.sortField(null, MultiValueMode.MIN, null, false));
            expectThrows(IllegalArgumentException.class, () -> parent.load(leaf).getBytesValues());
        }
    }

    /** And {@code _field_caps} must not advertise what the fielddata refuses. */
    public void testNoPathIsAggregatable() throws IOException {
        for (Version version : List.of(Version.V_3_5_0, Version.CURRENT)) {
            MapperService mapperService = service(version);
            assertFalse("a legacy index too", service(version, false).fieldType(FIELD).isAggregatable());
            assertFalse("the parent on " + version, mapperService.fieldType(FIELD).isAggregatable());
            assertFalse("a keyed path on " + version, mapperService.fieldType(FIELD + ".status").isAggregatable());
            assertFalse("a selector path on " + version, mapperService.fieldType(FIELD + ".events{}.status").isAggregatable());
        }
    }

    /**
     * An ordinary query keeps working on a path that contains braces, because a brace is a legal character in a key.
     *
     * <p>The same string means different things in the two contexts, and that is intended: a query reads it as a literal
     * term path through the field's terms, while a doc-value read parses it as spath. Rejecting braces in a query would
     * break a document {@code flat_object} has always accepted.
     */
    public void testAQueryOnAPathWithBracesIsUnaffected() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        MappedFieldType keyed = mapperService.fieldType(FIELD + ".events{}");
        assertNotNull(keyed);
        // Builds a term query rather than throwing, and names the legacy column.
        String query = keyed.termQuery("1", null).toString();
        assertTrue(query, query.contains(FIELD + "._valueAndPath"));
        assertTrue(query, query.contains("events{}"));
    }

    /**
     * Without the opt-in, the field is exactly what it was before the column existed.
     *
     * <p>Not "refuses", not "returns nothing": the same ordinals fielddata over the field's own column, so
     * {@code doc['attributes']} yields that field's sorted deduplicated strings and a keyed path yields its own. An index the
     * user did not opt in must not notice that the feature exists.
     */
    public void testWithoutTheOptInEverythingIsLegacy() throws IOException {
        MapperService legacy = service(Version.CURRENT, false);
        try (
            Directory dir = indexBatches(legacy, List.of(List.of(doc(7, "\"level\":\"info\","))));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertNull("no column is written", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));
            assertNull(leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blobnames"));

            // The parent hands a script the legacy string values, as a ScriptDocValues.Strings over attributes._value.
            IndexFieldData<?> parent = legacy.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            assertTrue(
                "the parent must keep its ordinals fielddata, was " + parent.getClass().getSimpleName(),
                parent instanceof SortedSetOrdinalsIndexFieldData
            );
            ScriptDocValues<?> whole = parent.load(leaf).getScriptValues();
            whole.setNextDocId(0);
            // Prefixed with the field name, sorted, deduplicated: precisely what attributes._value has always held.
            assertEquals("the legacy sorted deduplicated strings", List.of("attributes.7", "attributes.info"), List.copyOf(whole));

            // And a keyed path keeps its own, over attributes._valueAndPath.
            IndexFieldData<?> keyed = fielddata(legacy, "status");
            assertTrue(keyed instanceof SortedSetOrdinalsIndexFieldData);
            ScriptDocValues<?> keyedValues = keyed.load(leaf).getScriptValues();
            keyedValues.setNextDocId(0);
            assertEquals(List.of("attributes.attributes.level=info", "attributes.attributes.status=7"), List.copyOf(keyedValues));

            // docvalue_fields on the keyed path is the string it always was.
            var fetcher = keyed.load(leaf).getLeafValueFetcher(legacy.fieldType(FIELD + ".status").docValueFormat(null, null));
            assertTrue(fetcher.advanceExact(0));
            List<Object> fetched = new ArrayList<>();
            for (int i = 0, count = fetcher.docValueCount(); i < count; i++) {
                fetched.add(fetcher.nextValue());
            }
            assertTrue("the path's own value survives the prefix filter, was " + fetched, fetched.contains("7"));
        }
    }

    /** An index created before the columns existed cannot have them, whatever its settings say today. */
    public void testAnOldIndexIsLegacyEvenWithTheOptIn() throws IOException {
        MapperService older = service(Version.V_3_5_0, true);
        try (Directory dir = indexBatches(older, List.of(List.of(doc(1, "")))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertNull("the version guard wins over the setting", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));
            IndexFieldData<?> parent = older.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            assertTrue(parent instanceof SortedSetOrdinalsIndexFieldData);
            // And nothing throws.
            ScriptDocValues<?> whole = parent.load(leaf).getScriptValues();
            whole.setNextDocId(0);
            assertEquals(List.of("attributes.1"), List.copyOf(whole));
        }
    }

    /** The setting cannot be changed on an existing index, because the columns are written at index time. */
    public void testTheSettingIsFinal() {
        assertTrue(
            "must be final, or an index could be flipped and split in two",
            FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getProperties()
                .contains(org.opensearch.common.settings.Setting.Property.Final)
        );
        assertFalse(
            "must not be dynamic",
            FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getProperties()
                .contains(org.opensearch.common.settings.Setting.Property.Dynamic)
        );
        assertFalse(
            "and false unless asked for",
            FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getDefault(Settings.EMPTY)
        );
    }

    /**
     * A segment whose documents have the field but no column must refuse, not answer emptily.
     *
     * <p>The setting says whether the column <em>should</em> exist, and it is only a proxy for whether it does -- metadata can
     * say enabled for a shard whose segments were written without it, through a restore or a mixed upgrade. Where the two
     * disagree the failure is otherwise silent: Lucene answers an absent doc-values field with an empty iterator rather than
     * an error, so a read reports a document that has a value as having none.
     */
    public void testASegmentWithDocumentsButNoColumnIsRefused() throws IOException {
        MapperService enabled = service(Version.CURRENT, true);
        MapperService wroteNoColumns = service(Version.CURRENT, false);
        try (
            Directory dir = indexBatches(wroteNoColumns, List.of(List.of(doc(1, ""))));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertNotNull("the field's terms are present", leaf.reader().getFieldInfos().fieldInfo(FIELD));
            assertNull("but the column is not", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));

            LeafFieldData leafData = fielddata(enabled, "status").load(leaf);
            IllegalStateException refused = expectThrows(IllegalStateException.class, leafData::getScriptValues);
            assertTrue(refused.getMessage(), refused.getMessage().contains("no [" + FIELD + "._blob] column"));
            assertTrue("the message should say what to do", refused.getMessage().contains("Reindex"));

            IllegalStateException fetchRefused = expectThrows(
                IllegalStateException.class,
                () -> leafData.getLeafValueFetcher(enabled.fieldType(FIELD + ".status").docValueFormat(null, null))
            );
            assertTrue(fetchRefused.getMessage(), fetchRefused.getMessage().contains("no [" + FIELD + "._blob] column"));
        }
    }

    /** An older index keeps writing exactly what it wrote before: no columns, so nothing new to reject or read. */
    public void testOlderIndicesWriteNoColumns() throws IOException {
        MapperService older = service(Version.V_3_5_0, true);
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
            assertEquals("only those two columns", 2, countColumns(infos));
        }
    }

    /** The Lucene fields the field writes: its own terms, the two subfields, and exactly two doc-values columns. */
    private static int countColumns(org.apache.lucene.index.FieldInfos infos) {
        int columns = 0;
        for (org.apache.lucene.index.FieldInfo info : infos) {
            if (info.name.startsWith(FIELD + "._blob")) {
                columns++;
            }
        }
        return columns;
    }

    /** Values must survive a merge, which remaps every ordinal. */
    public void testValuesSurviveAMerge() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<List<String>> batches = List.of(
            List.of(doc(1, "\"a\":1,"), doc(2, "\"b\":2,")),
            List.of(doc(3, "\"c\":3,")),
            List.of(doc(4, "\"zzz\":4,"), doc(5, ""))
        );
        try (Directory dir = indexBatches(mapperService, batches)) {
            try (DirectoryReader before = DirectoryReader.open(dir)) {
                assertEquals("three commits, three segments", 3, before.leaves().size());
                assertEquals(List.of(1L, 2L, 3L, 4L, 5L), readAll(mapperService, before, "status"));
            }
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
                writer.forceMerge(1);
            }
            try (DirectoryReader after = DirectoryReader.open(dir)) {
                assertEquals("merged to one segment", 1, after.leaves().size());
                // A merge is free to reorder documents, so the set is what has to survive, not the order.
                assertEquals(
                    "every value survives the ordinal remap",
                    new HashSet<>(List.of(1L, 2L, 3L, 4L, 5L)),
                    new HashSet<>(readAll(mapperService, after, "status"))
                );
            }
        }
    }

    /**
     * The invariant the whole reader rests on: a document's ordinals come back ascending and deduplicated, so field id
     * {@code i} is the {@code i}-th of them.
     */
    public void testDocumentOrdinalsAreAscendingAndDeduplicated() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        // Keys deliberately out of sorted order, and one repeated at two levels.
        String json = "{\"attributes\":{\"zebra\":1,\"alpha\":2,\"middle\":{\"alpha\":3},\"beta\":4}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            SortedSetDocValues names = DocValues.getSortedSet(reader.leaves().get(0).reader(), FIELD + "._blobnames");
            assertTrue(names.advanceExact(0));
            List<Long> ordinals = new ArrayList<>();
            for (int i = 0; i < names.docValueCount(); i++) {
                ordinals.add(names.nextOrd());
            }
            List<Long> sorted = new ArrayList<>(ordinals);
            Collections.sort(sorted);
            assertEquals("ordinals must be ascending", sorted, ordinals);
            assertEquals("and deduplicated", new HashSet<>(ordinals).size(), ordinals.size());
            assertEquals("four distinct names: zebra, alpha, middle, beta", 4, ordinals.size());

            // And the read agrees with the document, which is what the invariant is for.
            LeafReaderContext leaf = reader.leaves().get(0);
            assertEquals(1L, read(mapperService, leaf, "zebra", 0));
            assertEquals(2L, read(mapperService, leaf, "alpha", 0));
            assertEquals(3L, read(mapperService, leaf, "middle.alpha", 0));
            assertEquals(4L, read(mapperService, leaf, "beta", 0));
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
            assertEquals(List.of(7L, 9L), readAll(mapperService, reader, "status"));
        }
    }

    /** Reading documents out of order must restart the forward-only blob cursor rather than report them as absent. */
    public void testBackwardsReadsWork() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        List<String> docs = List.of(doc(1, ""), doc(2, ""), doc(3, ""), doc(4, ""));
        try (Directory dir = indexBatches(mapperService, List.of(docs)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            ScriptDocValues<?> values = fielddata(mapperService, "status").load(leaf).getScriptValues();
            for (int docId : new int[] { 3, 0, 2, 1, 3, 0 }) {
                values.setNextDocId(docId);
                assertFalse("doc " + docId + " must have a value", values.isEmpty());
                assertEquals("doc " + docId, (long) (docId + 1), values.get(0));
            }
        }
    }

    public void testTopLevelArrayKeepsLegacyBehaviorWithoutVariantColumns() throws IOException {
        String json = "{\"attributes\":[{\"host\":\"a\",\"status\":200},{\"host\":\"b\",\"status\":500}]}";
        ParsedDocument legacy = service(Version.CURRENT, false).documentMapper().parse(source(json));
        assertNull(legacy.rootDoc().getField(FIELD + "._blob"));
        assertNotNull(legacy.rootDoc().getField(FIELD + "._valueAndPath"));
    }

    public void testTopLevelArraysAreRejectedWithVariantColumns() throws IOException {
        MapperService mapperService = service(Version.CURRENT, true);
        for (String value : List.of("[]", "[null]", "[{\"a\":1}]", "[{\"a\":1},{\"b\":2}]", "[[{\"a\":1}]]")) {
            MapperParsingException refused = expectThrows(
                MapperParsingException.class,
                () -> mapperService.documentMapper().parse(source("{\"attributes\":" + value + "}"))
            );
            assertTrue(
                stackTraceOf(refused),
                stackTraceOf(refused).contains(
                    "flat_object field [attributes] does not support a top-level array when Variant doc values are enabled"
                )
            );
        }
    }

    public void testAScalarInATopLevelArrayIsRefusedWithOrWithoutVariantColumns() throws IOException {
        String json = "{\"attributes\":[{\"a\":1},2]}";
        for (boolean variantDocValues : List.of(false, true)) {
            MapperService mapperService = service(Version.CURRENT, variantDocValues);
            expectThrows(Exception.class, () -> mapperService.documentMapper().parse(source(json)));
        }
    }

    /**
     * The columns take those names as Lucene fields; a key of the same name is a name inside the value tree, so both are
     * readable and neither disturbs the other.
     */
    public void testAKeyNamedLikeAColumnIsReadable() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String json = "{\"attributes\":{\"_blob\":1,\"_blobnames\":2,\"_blobmeta\":3,\"status\":4}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertEquals(1L, read(mapperService, leaf, "_blob", 0));
            assertEquals(2L, read(mapperService, leaf, "_blobnames", 0));
            assertEquals(3L, read(mapperService, leaf, "_blobmeta", 0));
            assertEquals(4L, read(mapperService, leaf, "status", 0));
        }
    }

    /** The JSON parser rejects a repeated object key before either flat_object writer sees it, with or without the column. */
    public void testDuplicateObjectKeyIsRejectedWithOrWithoutTheColumn() throws IOException {
        String json = "{\"attributes\":{\"status\":1,\"status\":2}}";
        for (boolean variantDocValues : List.of(false, true)) {
            MapperService mapperService = service(Version.CURRENT, variantDocValues);
            MapperParsingException refused = expectThrows(
                MapperParsingException.class,
                () -> mapperService.documentMapper().parse(source(json))
            );
            assertTrue(stackTraceOf(refused), stackTraceOf(refused).contains("Duplicate Object property"));
        }
    }

    /**
     * The final dictionary order is known before any container bytes are written, so a nested object can widen directly
     * from an early insertion id to the 257th sorted id without relabelling or re-encoding.
     */
    public void testMoreThan256KeysAreEncodedOnceWithFinalFieldIdWidths() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        StringBuilder json = new StringBuilder("{\"attributes\":{\"root\":{\"zzzzzz\":42}");
        for (int i = 0; i < 255; i++) {
            json.append(",\"k").append(String.format(Locale.ROOT, "%03d", i)).append("\":").append(i);
        }
        json.append("}}");

        try (
            Directory dir = indexBatches(mapperService, List.of(List.of(json.toString())));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertEquals(42L, read(mapperService, leaf, "root.zzzzzz", 0));
            assertEquals(254L, read(mapperService, leaf, "k254", 0));
        }
    }

    /**
     * More keys than the column takes is indexed with its terms and an unavailable sentinel, not refused.
     *
     * <p>Refusing would reject a document plain {@code flat_object} accepts. Writing something rather than nothing keeps the
     * column present, so a segment in which every document landed here answers missing instead of looking broken.
     */
    public void testMoreKeysThanTheColumnTakesIsIndexedWithoutABlob() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        StringBuilder json = new StringBuilder("{\"attributes\":{");
        // MAX_KEYS_PER_DOCUMENT is 0xFFFF and package-private to org.opensearch.index.mapper; one more than that.
        int keys = 0xFFFF + 1;
        for (int i = 0; i < keys; i++) {
            json.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
        }
        json.append("}}");
        try (
            Directory dir = indexBatches(mapperService, List.of(List.of(json.toString())));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            LeafReaderContext leaf = reader.leaves().get(0);
            assertNotNull("the column stays present", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));
            assertSame("but holds no value for this document", PathResolver.MISSING, read(mapperService, leaf, "k0", 0));
        }
    }

    /**
     * The unavailable sentinel must not read as an empty object, or a document the column has no value for would be
     * indistinguishable from one whose value really is {@code {}}.
     */
    public void testTheUnavailableSentinelIsNotAnEmptyObject() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String blobField = FIELD + "._blob";
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
                writer.addDocument(mapperService.documentMapper().parse(source("{\"attributes\":{}}")).rootDoc());

                ParsedDocument parsed = mapperService.documentMapper().parse(source(doc(1, "")));
                List<IndexableField> fields = new ArrayList<>();
                for (IndexableField field : parsed.rootDoc().getFields()) {
                    if (field.name().equals(blobField)) {
                        fields.add(new BinaryDocValuesField(blobField, new BytesRef(unavailableValue())));
                    } else {
                        fields.add(field);
                    }
                }
                writer.addDocument(fields);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);

                // A real empty object: the whole-value script read sees a present, empty map.
                IndexFieldData<?> parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
                ScriptDocValues<?> whole = parent.load(leaf).getScriptValues();
                whole.setNextDocId(0);
                assertEquals("an indexed {} is present", 1, whole.size());
                assertEquals(Map.of(), whole.get(0));

                // The sentinel: absent for both the whole value and any path.
                whole.setNextDocId(1);
                assertEquals("the sentinel reads as no value", 0, whole.size());
                assertSame(PathResolver.MISSING, read(mapperService, leaf, "status", 1));
            }
        }
    }

    /**
     * A number too wide for the encoding costs the document's blob, not the document, and never its type.
     *
     * <p>Variant's widest integer holds 38 decimal digits. Past that the three available answers are all bad, and only one of
     * them is honest: rounding through a double reports a number the document does not contain, keeping the text turns a JSON
     * number into a string a script would read as one, and refusing the document rejects one plain {@code flat_object}
     * accepts. So the document is indexed with its terms and the column records that it has no value -- which a script reads
     * as absent, and which is true.
     */
    public void testANumberTooWideForTheEncodingLosesTheBlobNotTheDocument() throws IOException {
        MapperService mapperService = service(Version.CURRENT);
        String wide = "9".repeat(39);
        String json = "{\"attributes\":{\"huge\":" + wide + ",\"status\":1}}";
        try (Directory dir = indexBatches(mapperService, List.of(List.of(json))); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);

            // Indexed: its terms are there, so an ordinary query still finds it.
            assertNotNull("the document was indexed", leaf.reader().getFieldInfos().fieldInfo(FIELD));
            assertNotNull("and the column stays present", leaf.reader().getFieldInfos().fieldInfo(FIELD + "._blob"));

            // But nothing is readable from the column, and in particular nothing of the wrong type.
            Object huge = read(mapperService, leaf, "huge", 0);
            assertSame("never a String standing in for a number", PathResolver.MISSING, huge);
            assertSame(
                "the whole document's blob is abandoned, not just the one value",
                PathResolver.MISSING,
                read(mapperService, leaf, "status", 0)
            );

            // Including the whole-value read.
            IndexFieldData<?> parent = mapperService.fieldType(FIELD).fielddataBuilder("test", () -> null).build(null, null);
            ScriptDocValues<?> whole = parent.load(leaf).getScriptValues();
            whole.setNextDocId(0);
            assertEquals("the root reads as absent too", 0, whole.size());
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
        // what matters here, it refuses it identically whether or not the index writes columns.
        for (Version version : List.of(Version.V_3_5_0, Version.CURRENT)) {
            MapperService mapperService = service(version);
            MapperParsingException refused = expectThrows(
                MapperParsingException.class,
                () -> mapperService.documentMapper().parse(source(json))
            );
            assertTrue(stackTraceOf(refused), stackTraceOf(refused).contains("Broken surrogate pair"));
        }
    }

    private static Variant decodedVariant(ParsedDocument document) {
        BytesRef value = document.rootDoc().getField(FIELD + "._blob").binaryValue();
        IndexableField[] nameFields = document.rootDoc().getFields(FIELD + "._blobnames");
        byte[][] names = new byte[nameFields.length][];
        int[] ordinals = new int[nameFields.length];
        for (int i = 0; i < nameFields.length; i++) {
            names[i] = BytesRef.deepCopyOf(nameFields[i].binaryValue()).bytes;
            ordinals[i] = i;
        }
        return new Variant(
            new VariantMetadata(names, ordinals, names.length),
            value.bytes,
            value.offset,
            value.offset,
            value.offset + value.length
        );
    }

    /**
     * The bytes the mapper writes for a document whose value cannot be encoded: a Variant null.
     *
     * <p>Built here rather than read off the mapper, so the sentinel does not have to be public production API. If the two
     * ever disagree, {@link #testTheUnavailableSentinelIsNotAnEmptyObject} fails, which is the point.
     */
    private static byte[] unavailableValue() {
        VariantBuilder builder = new VariantBuilder(8);
        builder.appendNull();
        return builder.finish().valueBytes();
    }

    private static String stackTraceOf(Throwable throwable) {
        StringBuilder text = new StringBuilder();
        for (Throwable at = throwable; at != null && at != at.getCause(); at = at.getCause()) {
            text.append(at).append('\n');
        }
        return text.toString();
    }
}
