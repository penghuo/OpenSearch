/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end tests that Solution A and Solution B produce identical query results (C1.3 through C1.6, and C3.1).
 *
 * <p>Both arms expose the same derived field names and types, so the aggregation, the query plan and the framework cost are
 * the same in both; only the script body differs, and it differs exactly in which store it reads. Anything that diverges
 * here is a property of the value store.
 *
 * <p>Lives in the painless module because the {@code variant()} accessor is reached through a painless script, and the
 * server's own integration tests do not load the scripting module.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class FlatObjectVariantBlobIT extends OpenSearchIntegTestCase {

    private static final String ARM_A = "arm-a";
    private static final String ARM_B = "arm-b";
    private static final String ARM_B_NO_SOURCE = "arm-b-nosource";

    private static final int DOC_COUNT = 200;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    private static final String SOURCE_STATUS_SCRIPT = "def a = params._source.attributes; "
        + "if (a != null && a.status != null) { emit(((Number)a.status).longValue()); }";
    private static final String SOURCE_NAMESPACE_SCRIPT = "def a = params._source.attributes; "
        + "if (a != null && a['k8s.namespace'] != null) { emit(a['k8s.namespace']); }";
    private static final String BLOB_STATUS_SCRIPT = "def v = variant('attributes'); "
        + "if (v != null) { def s = v.getLong('status'); if (s != null) { emit(s); } }";
    private static final String BLOB_NAMESPACE_SCRIPT = "def v = variant('attributes'); "
        + "if (v != null) { def n = v.getString('k8s.namespace'); if (n != null) { emit(n); } }";

    private void createArm(String index, boolean variantBlob, boolean sourceEnabled) {
        String statusScript = variantBlob ? BLOB_STATUS_SCRIPT : SOURCE_STATUS_SCRIPT;
        String namespaceScript = variantBlob ? BLOB_NAMESPACE_SCRIPT : SOURCE_NAMESPACE_SCRIPT;
        String mapping = "{"
            + (sourceEnabled ? "" : "\"_source\":{\"enabled\":false},")
            + "\"properties\":{\"attributes\":{\"type\":\"flat_object\""
            + "}},"
            + "\"derived\":{"
            + "\"attr_status\":{\"type\":\"long\",\"script\":{\"source\":\""
            + escape(statusScript)
            + "\",\"lang\":\"painless\"}},"
            + "\"attr_namespace\":{\"type\":\"keyword\",\"script\":{\"source\":\""
            + escape(namespaceScript)
            + "\",\"lang\":\"painless\"}}"
            + "}}";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(mapping)
        );
    }

    private static String escape(String script) {
        return script.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * The documents both arms see. Values are chosen so the aggregations have a known answer, not just a matching one.
     */
    private static List<String> documents() {
        List<String> sources = new ArrayList<>(DOC_COUNT);
        for (int i = 0; i < DOC_COUNT; i++) {
            long status = 200 + (i % 5) * 100;
            String namespace = "ns-" + (i % 4);
            String level = (i % 3 == 0) ? "error" : "info";
            sources.add(
                "{\"body\":\"doc-"
                    + i
                    + "\",\"attributes\":{"
                    + "\"status\":"
                    + status
                    + ","
                    + "\"level\":\""
                    + level
                    + "\","
                    + "\"k8s.namespace\":\""
                    + namespace
                    + "\","
                    + "\"ratio\":"
                    + (i / 10.0)
                    + ","
                    + "\"nested\":{\"deep\":{\"value\":"
                    + i
                    + "}}"
                    + "}}"
            );
        }
        return sources;
    }

    private void indexAll(String... indices) throws Exception {
        List<String> sources = documents();
        for (String index : indices) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < sources.size(); i++) {
                bulk.add(client().prepareIndex(index).setId(String.valueOf(i)).setSource(sources.get(i), MediaTypeRegistry.JSON));
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(index).get();
        }
    }

    private void setUpBothArms() throws Exception {
        createArm(ARM_A, false, true);
        createArm(ARM_B, true, true);
        indexAll(ARM_A, ARM_B);
    }

    // ------------------------------------------------------------------- C1.3

    public void testAggregationEquivalence() throws Exception {
        setUpBothArms();

        SearchResponse a = client().prepareSearch(ARM_A)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .addAggregation(AggregationBuilders.avg("mean").field("attr_status"))
            .addAggregation(AggregationBuilders.min("low").field("attr_status"))
            .addAggregation(AggregationBuilders.max("high").field("attr_status"))
            .addAggregation(AggregationBuilders.count("counted").field("attr_status"))
            .get();
        SearchResponse b = client().prepareSearch(ARM_B)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .addAggregation(AggregationBuilders.avg("mean").field("attr_status"))
            .addAggregation(AggregationBuilders.min("low").field("attr_status"))
            .addAggregation(AggregationBuilders.max("high").field("attr_status"))
            .addAggregation(AggregationBuilders.count("counted").field("attr_status"))
            .get();

        assertEquals(
            "sum must match",
            ((Sum) a.getAggregations().get("total")).getValue(),
            ((Sum) b.getAggregations().get("total")).getValue(),
            0.0
        );
        assertEquals(((Avg) a.getAggregations().get("mean")).getValue(), ((Avg) b.getAggregations().get("mean")).getValue(), 0.0);
        assertEquals(((Min) a.getAggregations().get("low")).getValue(), ((Min) b.getAggregations().get("low")).getValue(), 0.0);
        assertEquals(((Max) a.getAggregations().get("high")).getValue(), ((Max) b.getAggregations().get("high")).getValue(), 0.0);

        // Also correct, not merely equal: statuses cycle 200,300,400,500,600 over 200 documents.
        double expectedSum = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            expectedSum += 200 + (i % 5) * 100;
        }
        assertEquals(expectedSum, ((Sum) a.getAggregations().get("total")).getValue(), 0.0);
        assertEquals(expectedSum, ((Sum) b.getAggregations().get("total")).getValue(), 0.0);
        assertEquals(400.0, ((Avg) b.getAggregations().get("mean")).getValue(), 0.0);
        assertEquals(200.0, ((Min) b.getAggregations().get("low")).getValue(), 0.0);
        assertEquals(600.0, ((Max) b.getAggregations().get("high")).getValue(), 0.0);
    }

    // ------------------------------------------------------------------- C1.4

    public void testGroupByEquivalence() throws Exception {
        setUpBothArms();

        Terms termsA = groupBy(ARM_A);
        Terms termsB = groupBy(ARM_B);

        assertEquals("bucket count", termsA.getBuckets().size(), termsB.getBuckets().size());
        assertEquals(4, termsB.getBuckets().size());
        for (int i = 0; i < termsA.getBuckets().size(); i++) {
            Terms.Bucket bucketA = termsA.getBuckets().get(i);
            Terms.Bucket bucketB = termsB.getBuckets().get(i);
            assertEquals("bucket key at " + i, bucketA.getKeyAsString(), bucketB.getKeyAsString());
            assertEquals("bucket count at " + i, bucketA.getDocCount(), bucketB.getDocCount());
            assertEquals(
                "bucket sum at " + i,
                ((Sum) bucketA.getAggregations().get("total")).getValue(),
                ((Sum) bucketB.getAggregations().get("total")).getValue(),
                0.0
            );
            assertEquals("each namespace covers a quarter of the corpus", DOC_COUNT / 4, bucketB.getDocCount());
        }
    }

    private Terms groupBy(String index) {
        return client().prepareSearch(index)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_ns")
                    .field("attr_namespace")
                    .size(16)
                    .subAggregation(AggregationBuilders.sum("total").field("attr_status"))
            )
            .get()
            .getAggregations()
            .get("by_ns");
    }

    // ------------------------------------------------------------------- C1.5

    /**
     * Filtering comes from {@code flat_object} and is not the variable under study, so it must be identical. A difference
     * here would mean the blob column changed the inverted index, invalidating every other comparison.
     */
    public void testFilteringIsUnchanged() throws Exception {
        setUpBothArms();

        assertSameHitCount("term on a value", QueryBuilders.termQuery("attributes", "info"));
        assertSameHitCount("term on a dotted path", QueryBuilders.termQuery("attributes.k8s.namespace", "ns-1"));
        assertSameHitCount("terms", QueryBuilders.termsQuery("attributes", "info", "error"));
        assertSameHitCount("prefix", QueryBuilders.prefixQuery("attributes", "ns-"));
        assertSameHitCount("exists", QueryBuilders.existsQuery("attributes"));
        assertSameHitCount("range", QueryBuilders.rangeQuery("attributes").from("a").to("z"));

        // And the counts are real, not zero on both sides.
        long infoHits = client().prepareSearch(ARM_A)
            .setQuery(QueryBuilders.termQuery("attributes", "info"))
            .get()
            .getHits()
            .getTotalHits()
            .value();
        assertEquals("two thirds of the corpus is info", DOC_COUNT - (DOC_COUNT / 3 + 1), infoHits);
    }

    private void assertSameHitCount(String description, org.opensearch.index.query.QueryBuilder query) {
        long hitsA = client().prepareSearch(ARM_A).setQuery(query).setSize(0).get().getHits().getTotalHits().value();
        long hitsB = client().prepareSearch(ARM_B).setQuery(query).setSize(0).get().getHits().getTotalHits().value();
        assertEquals(description + " must match across arms", hitsA, hitsB);
    }

    // ------------------------------------------------------------------- C1.6

    public void testMixedTypePathEquivalence() throws Exception {
        createArm(ARM_A, false, true);
        createArm(ARM_B, true, true);
        // The same path holds a number in one document and a word in another.
        List<String> sources = List.of("{\"attributes\":{\"code\":200}}", "{\"attributes\":{\"code\":\"OK\"}}");
        for (String index : List.of(ARM_A, ARM_B)) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < sources.size(); i++) {
                bulk.add(client().prepareIndex(index).setId(String.valueOf(i)).setSource(sources.get(i), MediaTypeRegistry.JSON));
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(index).get();
        }

        // Both arms see the same hits for the numeric and the textual form.
        assertSameHitCount("numeric form", QueryBuilders.termQuery("attributes", "200"));
        assertSameHitCount("textual form", QueryBuilders.termQuery("attributes", "OK"));
    }

    // ------------------------------------------------------------------- C3.1

    /**
     * C3.1, and a finding rather than a pass.
     *
     * <p>The blob genuinely is independent of {@code _source}: it indexes fine into a {@code _source}-disabled index, and
     * {@code AccessorEquivalenceTests.testSourceDisabledLeavesOnlyTheBlob} shows the values are readable there while
     * {@code _source} has nothing to offer.
     *
     * <p>But that benefit cannot currently be reached through a query. {@link
     * org.opensearch.index.mapper.DerivedFieldType#getDerivedFieldLeafFactory} rejects <em>any</em> derived field on a
     * {@code _source}-disabled index, without regard to whether the script reads {@code _source} — so a blob-backed script
     * that never touches it is refused too. The design's "works with {@code _source} disabled" row therefore holds at the
     * storage layer and is blocked at the query layer.
     *
     * <p>This test pins that blocker instead of asserting the outcome the design hoped for. Lifting the precondition is a
     * change to shared derived-field behaviour — today's clear error would become silently empty results for
     * {@code _source}-backed scripts — so it is deliberately not attempted here.
     */
    public void testSourceDisabledIndexesButCannotBeQueriedThroughDerivedFields() throws Exception {
        createArm(ARM_B_NO_SOURCE, true, false);
        indexAll(ARM_B_NO_SOURCE);

        // The write path works: documents land, and the blob column is written.
        assertEquals(DOC_COUNT, client().prepareSearch(ARM_B_NO_SOURCE).setSize(0).get().getHits().getTotalHits().value());
        // Filtering still works, because it comes from the terms rather than from _source.
        assertTrue(
            client().prepareSearch(ARM_B_NO_SOURCE)
                .setQuery(QueryBuilders.termQuery("attributes", "info"))
                .setSize(0)
                .get()
                .getHits()
                .getTotalHits()
                .value() > 0
        );

        // The read path does not, and fails for a reason unrelated to where the value is stored.
        Exception failure = expectThrows(
            Exception.class,
            () -> client().prepareSearch(ARM_B_NO_SOURCE)
                .setSize(0)
                .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
                .get()
        );
        assertTrue(
            "expected the derived-field _source precondition, but got: " + failure,
            stackTraceOf(failure).contains("_source is disabled in the mappings")
        );
    }

    private static String stackTraceOf(Throwable throwable) {
        StringBuilder text = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            text.append(current).append('\n');
            if (current.getCause() == current) {
                break;
            }
        }
        return text.toString();
    }

    // ------------------------------------------------------------- durability

    /**
     * Merging rewrites the doc-values column, so the results have to survive it.
     */
    public void testResultsSurviveForceMerge() throws Exception {
        setUpBothArms();

        for (String index : List.of(ARM_A, ARM_B)) {
            ForceMergeResponse response = client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
            assertEquals(0, response.getFailedShards());
        }
        client().admin().indices().prepareRefresh(ARM_A, ARM_B).get();

        Sum sumA = client().prepareSearch(ARM_A)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .get()
            .getAggregations()
            .get("total");
        Sum sumB = client().prepareSearch(ARM_B)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .get()
            .getAggregations()
            .get("total");
        assertEquals(sumA.getValue(), sumB.getValue(), 0.0);

        Terms termsA = groupBy(ARM_A);
        Terms termsB = groupBy(ARM_B);
        assertEquals(termsA.getBuckets().size(), termsB.getBuckets().size());
        assertSameHitCount("term after merge", QueryBuilders.termQuery("attributes", "info"));
    }

    /**
     * Documents that lack the field must not break either arm, and must be skipped identically.
     */
    public void testDocumentsWithoutTheFieldAreSkippedIdentically() throws Exception {
        createArm(ARM_A, false, true);
        createArm(ARM_B, true, true);
        for (String index : List.of(ARM_A, ARM_B)) {
            BulkRequestBuilder bulk = client().prepareBulk();
            bulk.add(client().prepareIndex(index).setId("1").setSource("{\"attributes\":{\"status\":200}}", MediaTypeRegistry.JSON));
            bulk.add(client().prepareIndex(index).setId("2").setSource("{\"body\":\"no attributes\"}", MediaTypeRegistry.JSON));
            bulk.add(client().prepareIndex(index).setId("3").setSource("{\"attributes\":{\"other\":1}}", MediaTypeRegistry.JSON));
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(index).get();
        }

        Sum sumA = client().prepareSearch(ARM_A)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .get()
            .getAggregations()
            .get("total");
        Sum sumB = client().prepareSearch(ARM_B)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("attr_status"))
            .get()
            .getAggregations()
            .get("total");
        assertEquals(200.0, sumA.getValue(), 0.0);
        assertEquals("only the one document contributes, in both arms", sumA.getValue(), sumB.getValue(), 0.0);
    }
}
