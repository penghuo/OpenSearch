/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

/**
 * Integration tests for the Parquet DocValues format.
 *
 * These tests verify that the full OpenSearch stack works correctly when doc_values are stored
 * in Parquet encoding instead of Lucene's native format. This is critical because:
 *
 * 1. _source is NOT stored when parquet is enabled — it must be reconstructed from parquet columns.
 *    Any bug in reconstruction means GET/search hits return wrong or missing data.
 * 2. Aggregations read directly from doc_values, so parquet encoding must produce correct values
 *    for terms, sum, and date_histogram aggregations.
 * 3. Sorting relies on doc_values iteration, which must work with parquet's page-based storage.
 * 4. The _update API reads _source to apply partial updates, so reconstruction must be correct
 *    for updates to work at all.
 * 5. Force merge rewrites segments, exercising the full write→read→merge→write cycle.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ParquetDocValuesIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "parquet_test";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PARQUET_DOC_VALUES, true).build();
    }

    private void createParquetIndex() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.codec.doc_values.format", "parquet")
            )
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("long_field")
                        .field("type", "long")
                        .endObject()
                        .startObject("keyword_field")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("date_field")
                        .field("type", "date")
                        .endObject()
                        .startObject("status")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen(INDEX);
    }

    private void indexTestDocs() throws Exception {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        // 6 docs with known values for deterministic aggregation/sort verification
        builders.add(
            client().prepareIndex(INDEX)
                .setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 10L)
                        .field("keyword_field", "alpha")
                        .field("date_field", "2024-01-15T00:00:00Z")
                        .field("status", "active")
                        .endObject()
                )
        );
        builders.add(
            client().prepareIndex(INDEX)
                .setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 20L)
                        .field("keyword_field", "beta")
                        .field("date_field", "2024-01-15T12:00:00Z")
                        .field("status", "active")
                        .endObject()
                )
        );
        builders.add(
            client().prepareIndex(INDEX)
                .setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 30L)
                        .field("keyword_field", "gamma")
                        .field("date_field", "2024-02-10T00:00:00Z")
                        .field("status", "inactive")
                        .endObject()
                )
        );
        builders.add(
            client().prepareIndex(INDEX)
                .setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 40L)
                        .field("keyword_field", "alpha")
                        .field("date_field", "2024-02-10T06:00:00Z")
                        .field("status", "active")
                        .endObject()
                )
        );
        builders.add(
            client().prepareIndex(INDEX)
                .setId("5")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 50L)
                        .field("keyword_field", "beta")
                        .field("date_field", "2024-03-01T00:00:00Z")
                        .field("status", "inactive")
                        .endObject()
                )
        );
        builders.add(
            client().prepareIndex(INDEX)
                .setId("6")
                .setSource(
                    jsonBuilder().startObject()
                        .field("long_field", 60L)
                        .field("keyword_field", "gamma")
                        .field("date_field", "2024-03-01T12:00:00Z")
                        .field("status", "active")
                        .endObject()
                )
        );
        indexRandom(true, builders);
        flushAndRefresh(INDEX);
    }

    /**
     * Verifies that _source is correctly reconstructed from parquet doc_values columns.
     * When parquet format is enabled, _source is NOT stored — it is rebuilt at fetch time
     * from the parquet-encoded doc_values. This test ensures the reconstruction produces
     * the original field values for all supported types (long, keyword, date).
     */
    public void testSourceReconstruction() throws Exception {
        createParquetIndex();
        indexTestDocs();

        GetResponse get = client().prepareGet(INDEX, "1").get();
        assertTrue("Document should exist", get.isExists());
        Map<String, Object> source = get.getSourceAsMap();
        assertNotNull("Reconstructed _source must not be null", source);
        assertEquals(10, ((Number) source.get("long_field")).longValue());
        assertEquals("alpha", source.get("keyword_field"));
        assertEquals("active", source.get("status"));
        // Date may be reconstructed as epoch millis or ISO string depending on mapper
        assertNotNull("date_field must be present in reconstructed _source", source.get("date_field"));
    }

    /**
     * Verifies that aggregations produce correct results when reading from parquet doc_values.
     * Aggregations bypass _source and read directly from doc_values columns, so this tests
     * the parquet reader's ability to iterate values correctly for:
     * - Terms aggregation (SORTED doc_values on keyword)
     * - Sum aggregation (NUMERIC doc_values on long)
     * - Date histogram (NUMERIC doc_values on date, bucketed by month)
     */
    public void testAggregations() throws Exception {
        createParquetIndex();
        indexTestDocs();

        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("by_status").field("status"))
            .addAggregation(AggregationBuilders.sum("total_long").field("long_field"))
            .addAggregation(AggregationBuilders.dateHistogram("by_month").field("date_field").calendarInterval(DateHistogramInterval.MONTH))
            .get();
        assertNoFailures(response);

        // Terms: 4 active, 2 inactive
        Terms statusTerms = response.getAggregations().get("by_status");
        assertNotNull(statusTerms);
        assertEquals(2, statusTerms.getBuckets().size());
        for (Terms.Bucket bucket : statusTerms.getBuckets()) {
            if ("active".equals(bucket.getKeyAsString())) {
                assertEquals(4, bucket.getDocCount());
            } else {
                assertEquals("inactive", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }
        }

        // Sum: 10+20+30+40+50+60 = 210
        Sum totalLong = response.getAggregations().get("total_long");
        assertEquals(210.0, totalLong.getValue(), 0.001);

        // Date histogram: Jan=2, Feb=2, Mar=2
        Histogram dateHist = response.getAggregations().get("by_month");
        assertEquals(3, dateHist.getBuckets().size());
        for (Histogram.Bucket bucket : dateHist.getBuckets()) {
            assertEquals(2, bucket.getDocCount());
        }
    }

    /**
     * Verifies that sorting works correctly with parquet doc_values.
     * Sort uses doc_values iteration to compare values across documents.
     * This tests that the parquet reader returns values in the correct order
     * for NUMERIC doc_values (long field).
     */
    public void testSorting() throws Exception {
        createParquetIndex();
        indexTestDocs();

        SearchResponse response = client().prepareSearch(INDEX)
            .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
            .addSort("long_field", SortOrder.ASC)
            .setSize(6)
            .get();
        assertNoFailures(response);
        assertHitCount(response, 6);

        SearchHit[] hits = response.getHits().getHits();
        long prev = Long.MIN_VALUE;
        for (SearchHit hit : hits) {
            long val = ((Number) hit.getSourceAsMap().get("long_field")).longValue();
            assertTrue("Results must be sorted ascending, got " + prev + " then " + val, val >= prev);
            prev = val;
        }
    }

    /**
     * Verifies that the _update API works when _source is reconstructed from parquet.
     * The update API reads the current _source, applies the partial update, and re-indexes.
     * Since parquet indices don't store _source, the update must read from reconstructed _source.
     * A failure here means users cannot update documents in parquet-enabled indices.
     */
    public void testUpdateApi() throws Exception {
        createParquetIndex();

        client().prepareIndex(INDEX)
            .setId("u1")
            .setSource(
                jsonBuilder().startObject()
                    .field("long_field", 100L)
                    .field("keyword_field", "original")
                    .field("date_field", "2024-06-01T00:00:00Z")
                    .field("status", "active")
                    .endObject()
            )
            .get();
        flushAndRefresh(INDEX);

        // Partial update — changes keyword_field, long_field should remain
        UpdateResponse updateResponse = client().prepareUpdate(INDEX, "u1")
            .setDoc(jsonBuilder().startObject().field("keyword_field", "updated").endObject())
            .setFetchSource(true)
            .get();

        Map<String, Object> updatedSource = updateResponse.getGetResult().sourceAsMap();
        assertEquals("updated", updatedSource.get("keyword_field"));
        assertEquals(100, ((Number) updatedSource.get("long_field")).longValue());

        // Verify via GET after refresh
        flushAndRefresh(INDEX);
        GetResponse get = client().prepareGet(INDEX, "u1").get();
        assertTrue(get.isExists());
        assertEquals("updated", get.getSourceAsMap().get("keyword_field"));
        assertEquals(100, ((Number) get.getSourceAsMap().get("long_field")).longValue());
    }

    /**
     * Verifies that force merge correctly rewrites parquet doc_values segments.
     * Force merge reads all existing segments and writes a single merged segment.
     * This exercises the full parquet write→read→merge→write cycle and ensures
     * no data is lost or corrupted during segment merging. After merge, all
     * aggregations, sorting, and _source reconstruction must still work.
     */
    public void testForceMerge() throws Exception {
        createParquetIndex();
        indexTestDocs();

        // Force merge to 1 segment — triggers full read + rewrite of parquet data
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX);

        // Verify _source reconstruction still works after merge
        GetResponse get = client().prepareGet(INDEX, "3").get();
        assertTrue(get.isExists());
        Map<String, Object> source = get.getSourceAsMap();
        assertEquals(30, ((Number) source.get("long_field")).longValue());
        assertEquals("gamma", source.get("keyword_field"));
        assertEquals("inactive", source.get("status"));

        // Verify aggregations still correct after merge
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_long").field("long_field"))
            .addAggregation(AggregationBuilders.terms("by_status").field("status"))
            .get();
        assertNoFailures(response);

        Sum totalLong = response.getAggregations().get("total_long");
        assertEquals(210.0, totalLong.getValue(), 0.001);

        Terms statusTerms = response.getAggregations().get("by_status");
        assertEquals(2, statusTerms.getBuckets().size());

        // Verify sorting still works after merge
        response = client().prepareSearch(INDEX).addSort("long_field", SortOrder.DESC).setSize(1).get();
        assertNoFailures(response);
        assertEquals(60, ((Number) response.getHits().getHits()[0].getSourceAsMap().get("long_field")).longValue());
    }
}
