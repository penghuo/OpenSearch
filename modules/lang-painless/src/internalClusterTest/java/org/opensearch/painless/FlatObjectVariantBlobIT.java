/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Reading a {@code flat_object} value from its Variant doc-values column through Painless, on a real cluster.
 *
 * <p>The point of the column is that a script can read one path without loading, decompressing and parsing {@code _source}.
 * So the index under test has {@code _source} disabled: every value these scripts return could only have come from the
 * column, which makes the test prove the feature rather than merely exercise it.
 *
 * <p>Aggregation and sorting are deliberately absent. The column supports neither, and the refusals are covered by unit and
 * REST tests.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class FlatObjectVariantBlobIT extends OpenSearchIntegTestCase {

    private static final String NO_SOURCE_INDEX = "read-via-column-nosource";
    private static final String WITH_SOURCE_INDEX = "read-via-column";
    private static final int DOC_COUNT = 40;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    private void createIndex(String index, boolean sourceEnabled) {
        createIndex(index, sourceEnabled, 2);
    }

    private void createIndex(String index, boolean sourceEnabled, int shards) {
        String mapping = "{"
            + (sourceEnabled ? "" : "\"_source\":{\"enabled\":false},")
            + "\"properties\":{\"order\":{\"type\":\"keyword\"},\"attributes\":{\"type\":\"flat_object\"}}}";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", 0)
                        .put(FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getKey(), true)
                )
                .setMapping(mapping)
        );
    }

    private static List<String> documents() {
        List<String> sources = new ArrayList<>(DOC_COUNT);
        for (int i = 0; i < DOC_COUNT; i++) {
            sources.add(
                "{\"order\":\"d"
                    + i
                    + "\",\"attributes\":{"
                    + "\"status\":"
                    + (200 + (i % 5) * 100)
                    + ","
                    + "\"level\":\""
                    + ((i % 3 == 0) ? "error" : "info")
                    + "\","
                    + "\"k8s.namespace\":\"ns-"
                    + (i % 4)
                    + "\","
                    + "\"ratio\":"
                    + (i / 10.0)
                    + ","
                    + "\"enabled\":"
                    + (i % 2 == 0)
                    + ","
                    + "\"nested\":{\"deep\":{\"value\":"
                    + i
                    + "}},"
                    + "\"events\":[{\"status\":"
                    + i
                    + "},{\"status\":"
                    + (i + 1)
                    + "}]"
                    + "}}"
            );
        }
        return sources;
    }

    private void indexAll(String index) throws Exception {
        BulkRequestBuilder bulk = client().prepareBulk();
        List<String> sources = documents();
        for (int i = 0; i < sources.size(); i++) {
            bulk.add(client().prepareIndex(index).setId(String.valueOf(i)).setSource(sources.get(i), MediaTypeRegistry.JSON));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh(index).get();
    }

    private static Script script(String source) {
        return new Script(ScriptType.INLINE, "painless", source, Map.of());
    }

    private Map<String, DocumentField> fieldsOf(String index, String id, Map<String, String> scripts) {
        var request = client().prepareSearch(index).setQuery(QueryBuilders.termQuery("order", id));
        for (Map.Entry<String, String> entry : scripts.entrySet()) {
            request.addScriptField(entry.getKey(), script(entry.getValue()));
        }
        SearchResponse response = request.get();
        assertEquals("expected exactly one hit for " + id, 1, response.getHits().getTotalHits().value());
        return response.getHits().getAt(0).getFields();
    }

    /**
     * Scalars keep the JSON type the document had, read from the column with no {@code _source} to fall back to.
     */
    public void testScalarPathsKeepTheirTypesWithSourceDisabled() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(NO_SOURCE_INDEX);

        Map<String, DocumentField> fields = fieldsOf(
            NO_SOURCE_INDEX,
            "d3",
            Map.of(
                "status",
                "doc['attributes.status'].value",
                "level",
                "doc['attributes.level'].value",
                "namespace",
                "doc[\"attributes.['k8s.namespace']\"].value",
                "ratio",
                "doc['attributes.ratio'].value",
                "enabled",
                "doc['attributes.enabled'].value",
                "deep",
                "doc['attributes.nested.deep.value'].value"
            )
        );

        assertEquals("a JSON integer arrives as a Long", Long.valueOf(500L), fields.get("status").getValue());
        assertEquals("error", fields.get("level").getValue());
        assertEquals("a key containing a dot needs bracket quoting", "ns-3", fields.get("namespace").getValue());
        assertEquals("a JSON float arrives as a Double", 0.3, (Double) fields.get("ratio").getValue(), 0.0);
        assertEquals("a JSON boolean arrives as a Boolean", Boolean.FALSE, fields.get("enabled").getValue());
        assertEquals("nesting resolves without a selector", Long.valueOf(3L), fields.get("deep").getValue());
    }

    /** Array selectors, and the rule that an array is never traversed without one. */
    public void testArraySelectorsWithSourceDisabled() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(NO_SOURCE_INDEX);

        Map<String, DocumentField> fields = fieldsOf(
            NO_SOURCE_INDEX,
            "d7",
            Map.of(
                "all",
                "doc['attributes.events{}.status'].value",
                "star",
                "doc['attributes.events{*}.status'].value",
                "first",
                "doc['attributes.events{0}.status'].value",
                "second",
                "doc['attributes.events{1}.status'].value",
                "pastTheEnd",
                "doc['attributes.events{5}.status'].size()",
                "implicit",
                "doc['attributes.events.status'].size()",
                "onANonArray",
                "doc['attributes.status{}'].size()"
            )
        );

        // Document 7 holds events [{status: 7}, {status: 8}]. The script received one List; script_fields spreads a returned
        // collection across the response field's values, which is its own shaping and not the ScriptDocValues contract.
        assertEquals(List.of(7L, 8L), fields.get("all").getValues());
        assertEquals("{*} is the same selector spelled out", List.of(7L, 8L), fields.get("star").getValues());
        assertEquals(Long.valueOf(7L), fields.get("first").getValue());
        assertEquals(Long.valueOf(8L), fields.get("second").getValue());
        assertEquals("an index past the end is no match", Integer.valueOf(0), fields.get("pastTheEnd").getValue());
        assertEquals("an array is never traversed implicitly", Integer.valueOf(0), fields.get("implicit").getValue());
        assertEquals("a selector on a non-array is no match", Integer.valueOf(0), fields.get("onANonArray").getValue());
    }

    /** Missing, explicit null, and an empty array are three different answers. */
    public void testMissingNullAndEmptyAreDistinguishableWithSourceDisabled() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        client().prepareIndex(NO_SOURCE_INDEX)
            .setId("edges")
            .setSource(
                "{\"order\":\"edges\",\"attributes\":{"
                    + "\"present\":1,\"nothing\":null,\"empty\":[],"
                    + "\"holes\":[{\"v\":1},{\"other\":9},{\"v\":null},{\"v\":4}]"
                    + "}}",
                MediaTypeRegistry.JSON
            )
            .get();
        client().admin().indices().prepareRefresh(NO_SOURCE_INDEX).get();

        Map<String, DocumentField> fields = fieldsOf(
            NO_SOURCE_INDEX,
            "edges",
            Map.of(
                "absentSize",
                "doc['attributes.absent'].size()",
                "nullSize",
                "doc['attributes.nothing'].size()",
                "emptyWildcard",
                "doc['attributes.empty{}.x'].value.size()",
                "emptyWildcardSize",
                "doc['attributes.empty{}.x'].size()",
                "holes",
                "doc['attributes.holes{}.v'].value.size()",
                "firstHoleIsNull",
                "def v = doc['attributes.holes{}.v'].value; return v[1] == null",
                "presentSize",
                "doc['attributes.present'].size()"
            )
        );

        assertEquals("an absent path has no value", Integer.valueOf(0), fields.get("absentSize").getValue());
        // An explicit top-level null is present but its value is null, which the PPL helper collapses with missing.
        assertEquals("an explicit null is one value", Integer.valueOf(1), fields.get("nullSize").getValue());
        assertEquals("an empty array's wildcard is present", Integer.valueOf(1), fields.get("emptyWildcardSize").getValue());
        assertEquals("and its value is an empty list", Integer.valueOf(0), fields.get("emptyWildcard").getValue());
        assertEquals("a missing member is skipped, an explicit null kept", Integer.valueOf(3), fields.get("holes").getValue());
        assertEquals(Boolean.TRUE, fields.get("firstHoleIsNull").getValue());
        assertEquals(Integer.valueOf(1), fields.get("presentSize").getValue());
    }

    /**
     * The whole value comes back as an ordinary map, decoded in full.
     *
     * <p>A script wanting one value should name it -- {@code doc['attributes.level']} decodes only that path. Asking for the
     * object is asking for the object.
     */
    public void testTheWholeValueIsAStableMapWithSourceDisabled() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(NO_SOURCE_INDEX);

        Map<String, DocumentField> fields = fieldsOf(
            NO_SOURCE_INDEX,
            "d5",
            Map.of(
                "level",
                "doc['attributes'].value['level']",
                "keys",
                "doc['attributes'].value.size()",
                "namespace",
                "doc['attributes'].value['k8s.namespace']",
                "nestedThroughTheMap",
                "doc['attributes'].value['nested']['deep']['value']"
            )
        );

        assertEquals("info", fields.get("level").getValue());
        assertEquals("seven keys in every document", Integer.valueOf(7), fields.get("keys").getValue());
        assertEquals("a dotted key is one key in the map", "ns-1", fields.get("namespace").getValue());
        assertEquals(Long.valueOf(5L), fields.get("nestedThroughTheMap").getValue());
    }

    /**
     * A script returning the whole map, over several hits, must give each hit its own complete value.
     *
     * <p>This is the shape PPL uses when it selects {@code attributes}: the value leaves the script rather than being
     * dereferenced inside it. The fetch phase does not consume it where it produces it -- it can hold hit 1's value, advance
     * the doc values to hit 2 and serialise hit 1 afterwards -- so a value that still read reader state would come back as
     * another hit's data or fail outright. Asserting on more than one hit is what makes the difference visible; a single-hit
     * test passes either way.
     */
    public void testReturningTheWholeMapGivesEachHitItsOwnValueWithSourceDisabled() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(NO_SOURCE_INDEX);

        SearchResponse response = client().prepareSearch(NO_SOURCE_INDEX)
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort("order", org.opensearch.search.sort.SortOrder.ASC)
            .setSize(DOC_COUNT)
            // Exactly the whole map, returned rather than dereferenced.
            .addScriptField("whole", script("doc['attributes'].value"))
            .get();

        assertEquals(DOC_COUNT, response.getHits().getHits().length);
        for (org.opensearch.search.SearchHit hit : response.getHits().getHits()) {
            int i = Integer.parseInt(hit.getId());
            Object value = hit.getFields().get("whole").getValue();
            assertTrue("hit " + i + " must be a map, was " + value, value instanceof Map);
            Map<?, ?> map = (Map<?, ?>) value;
            // Every key of that document's own value, so a value from a neighbouring hit would be caught.
            assertEquals("hit " + i + " status", Long.valueOf(200 + (i % 5) * 100L), map.get("status"));
            assertEquals("hit " + i + " level", (i % 3 == 0) ? "error" : "info", map.get("level"));
            assertEquals("hit " + i + " namespace", "ns-" + (i % 4), map.get("k8s.namespace"));
            assertEquals("hit " + i + " ratio", (Double) (i / 10.0), (Double) map.get("ratio"), 0.0);
            assertEquals("hit " + i + " enabled", i % 2 == 0, map.get("enabled"));
            assertEquals("hit " + i + " keys", 7, map.size());
            // Nested and array members survive whole too.
            assertEquals("hit " + i + " deep", Map.of("value", (long) i), ((Map<?, ?>) map.get("nested")).get("deep"));
            assertEquals("hit " + i + " events", List.of(Map.of("status", (long) i), Map.of("status", i + 1L)), map.get("events"));
        }
    }

    /**
     * The same value, taken through the response rather than the hit objects, so it is serialised after the search context
     * that produced it has gone.
     */
    public void testTheWholeMapSerialisesAfterTheContextCloses() throws Exception {
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(NO_SOURCE_INDEX);

        SearchResponse response = client().prepareSearch(NO_SOURCE_INDEX)
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort("order", org.opensearch.search.sort.SortOrder.ASC)
            .setSize(DOC_COUNT)
            .addScriptField("whole", script("doc['attributes'].value"))
            .get();

        String rendered = org.opensearch.common.xcontent.XContentHelper.toXContent(
            response,
            org.opensearch.common.xcontent.XContentType.JSON,
            true
        ).utf8ToString();
        for (int i = 0; i < DOC_COUNT; i++) {
            assertTrue("hit " + i + "'s namespace must appear in the response", rendered.contains("ns-" + (i % 4)));
        }
        assertTrue("and the nested member", rendered.contains("\"deep\""));
    }

    /** Reading through a script must not change what a query matches, whether or not {@code _source} exists. */
    public void testQueriesAreUnchangedByTheColumn() throws Exception {
        createIndex(WITH_SOURCE_INDEX, true);
        createIndex(NO_SOURCE_INDEX, false);
        indexAll(WITH_SOURCE_INDEX);
        indexAll(NO_SOURCE_INDEX);

        for (String index : List.of(WITH_SOURCE_INDEX, NO_SOURCE_INDEX)) {
            assertEquals(
                "every document is searchable in " + index,
                DOC_COUNT,
                client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value()
            );
            assertEquals(
                "a value term still matches in " + index,
                DOC_COUNT / 4,
                client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("attributes", "ns-1"))
                    .setSize(0)
                    .get()
                    .getHits()
                    .getTotalHits()
                    .value()
            );
            assertEquals(
                "a dot-path term still matches in " + index,
                DOC_COUNT / 5,
                client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("attributes.status", "200"))
                    .setSize(0)
                    .get()
                    .getHits()
                    .getTotalHits()
                    .value()
            );
        }
    }

    /**
     * A merge rewrites the column and remaps every ordinal, so the values have to survive it.
     *
     * <p>One shard and a refresh between batches, so there really are several segments to merge. With a single bulk and one
     * refresh the shard may already hold one segment, and the force merge would prove nothing about remapping.
     */
    public void testValuesSurviveForceMerge() throws Exception {
        createIndex(NO_SOURCE_INDEX, false, 1);
        List<String> sources = documents();
        for (int batch = 0; batch < 4; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = batch; i < sources.size(); i += 4) {
                bulk.add(client().prepareIndex(NO_SOURCE_INDEX).setId(String.valueOf(i)).setSource(sources.get(i), MediaTypeRegistry.JSON));
            }
            assertFalse(bulk.get().hasFailures());
            // A refresh per batch, which is what closes a segment.
            client().admin().indices().prepareRefresh(NO_SOURCE_INDEX).get();
        }
        assertTrue(
            "the merge has to have several segments to remap",
            client().admin()
                .indices()
                .prepareSegments(NO_SOURCE_INDEX)
                .get()
                .getIndices()
                .get(NO_SOURCE_INDEX)
                .getShards()
                .values()
                .stream()
                .flatMap(shard -> java.util.Arrays.stream(shard.getShards()))
                .anyMatch(shard -> shard.getSegments().size() > 1)
        );

        Map<String, DocumentField> before = fieldsOf(
            NO_SOURCE_INDEX,
            "d11",
            Map.of("status", "doc['attributes.status'].value", "events", "doc['attributes.events{}.status'].value")
        );

        client().admin().indices().prepareForceMerge(NO_SOURCE_INDEX).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(NO_SOURCE_INDEX).get();
        assertEquals(
            "merged to one segment",
            1,
            client().admin()
                .indices()
                .prepareSegments(NO_SOURCE_INDEX)
                .get()
                .getIndices()
                .get(NO_SOURCE_INDEX)
                .getShards()
                .values()
                .stream()
                .flatMap(shard -> java.util.Arrays.stream(shard.getShards()))
                .mapToInt(shard -> shard.getSegments().size())
                .max()
                .orElse(0)
        );

        Map<String, DocumentField> after = fieldsOf(
            NO_SOURCE_INDEX,
            "d11",
            Map.of("status", "doc['attributes.status'].value", "events", "doc['attributes.events{}.status'].value")
        );

        assertEquals("a scalar survives the merge", Long.valueOf(300L), before.get("status").getValue());
        assertEquals("a scalar survives the merge", Long.valueOf(300L), after.get("status").getValue());
        assertEquals("and so does a wildcard list", before.get("events").getValues(), after.get("events").getValues());
        assertEquals(List.of(11L, 12L), after.get("events").getValues());
    }
}
