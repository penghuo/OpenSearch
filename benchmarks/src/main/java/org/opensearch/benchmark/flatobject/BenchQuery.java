/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import org.opensearch.test.flatobject.CorpusConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * The fixed, numbered set of queries the end-to-end comparison runs.
 *
 * <p>Hard-coded on purpose. An earlier version assembled queries at run time from a query-type × selectivity cross
 * product, which made it ambiguous how many queries there actually were and made results hard to refer to. Every query
 * here has one number, one body, and one stated purpose.
 *
 * <p>Both arms receive byte-identical bodies. The only difference between arms is the script inside the derived field the
 * query names, which is what makes the measured difference attributable to the value store.
 */
final class BenchQuery {

    /** Epoch millis of the first generated document; document i is stamped at BASE + i. */
    private static final long BASE_TIMESTAMP_MILLIS = 1_755_720_000_000L;

    final String id;
    final String name;
    final String purpose;
    /** Documents the query has to scan, for reporting alongside the latency. */
    final long docsScanned;
    final String body;

    private BenchQuery(String id, String name, String purpose, long docsScanned, String body) {
        this.id = id;
        this.name = name;
        this.purpose = purpose;
        this.docsScanned = docsScanned;
        this.body = body;
    }

    /**
     * Builds the query set for a corpus.
     *
     * <p>Scoping is expressed as a {@code range} inside the {@code query} clause rather than a {@code post_filter}: a post
     * filter runs after aggregation, so it would leave an aggregation scanning the whole corpus while appearing to scope
     * it.
     */
    static List<BenchQuery> forCorpus(CorpusConfig config, BenchProbes probes) {
        long total = config.docCount();
        String tenPercent = rangeClause(config, 10);
        String onePercent = rangeClause(config, 1);
        String term = "{\"term\":{\"attributes\":\"info\"}}";

        List<BenchQuery> queries = new ArrayList<>();

        queries.add(
            new BenchQuery(
                "Q1",
                "sum, dense path, full corpus",
                "Baseline cost of reading one always-present value, at full scale",
                total,
                "{\"size\":0,\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_status\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q2",
                "sum, dense path, 10% of corpus",
                "Same read, scoped by time range so more iterations fit in the same wall clock",
                total / 10,
                "{\"size\":0,\"query\":" + tenPercent + ",\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_status\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q3",
                "sum, dense path, 1% of corpus",
                "Same read, scoped further; the cheapest query and so the most stable percentiles",
                total / 100,
                "{\"size\":0,\"query\":" + onePercent + ",\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_status\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q4",
                "terms + sum group-by, full corpus",
                "Grouped aggregation reading two paths per document: the worst case for a row-oriented store",
                total,
                "{\"size\":0,\"aggs\":{\"by_ns\":{\"terms\":{\"field\":\"attr_namespace\",\"size\":32},"
                    + "\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_status\"}}}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q5",
                "terms + sum group-by, 10% of corpus",
                "The same grouped shape, scoped, so its percentiles rest on more samples than Q4's",
                total / 10,
                "{\"size\":0,\"query\":"
                    + tenPercent
                    + ",\"aggs\":{\"by_ns\":{\"terms\":{\"field\":\"attr_namespace\","
                    + "\"size\":32},\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_status\"}}}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q6",
                "sum, sparse path (" + pct(probes.sparsePresence) + " of docs), full corpus",
                "Cost when most documents do not carry the path at all",
                total,
                "{\"size\":0,\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_sparse\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q7",
                "sum, rare path (" + pct(probes.rarePresence) + " of docs), full corpus",
                "Same, for a path that is almost always absent",
                total,
                "{\"size\":0,\"aggs\":{\"total\":{\"sum\":{\"field\":\"attr_rare\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q8",
                "five metrics on one path, full corpus",
                "Whether five aggregations over one path cost one read or five",
                total,
                "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"attr_status\"}},\"a\":{\"avg\":{\"field\":\"attr_status\"}},"
                    + "\"mn\":{\"min\":{\"field\":\"attr_status\"}},\"mx\":{\"max\":{\"field\":\"attr_status\"}},"
                    + "\"c\":{\"value_count\":{\"field\":\"attr_status\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q9",
                "sum on two different paths, full corpus",
                "Marginal cost of a second path: one store re-parses nothing, the other searches again",
                total,
                "{\"size\":0,\"aggs\":{\"s1\":{\"sum\":{\"field\":\"attr_status\"}},\"s2\":{\"sum\":{\"field\":\"attr_sparse\"}}}}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q10",
                "fetch top 50 documents with a derived field",
                "Should favour _source: the document is loaded and parsed for the hits anyway, so the blob is extra work",
                50,
                "{\"size\":50,\"query\":" + term + ",\"fields\":[\"attr_status\"]}"
            )
        );
        queries.add(
            new BenchQuery(
                "Q11",
                "filter only, no derived field (control)",
                "Touches only the shared flat_object terms. Must show no A/B difference; if it does, the experiment is broken",
                total,
                "{\"size\":0,\"query\":" + term + ",\"track_total_hits\":true}"
            )
        );
        return queries;
    }

    private static String rangeClause(CorpusConfig config, int percent) {
        long upper = BASE_TIMESTAMP_MILLIS + (long) (config.docCount() * (percent / 100.0));
        return "{\"range\":{\"@timestamp\":{\"lt\":" + upper + "}}}";
    }

    private static String pct(double fraction) {
        return String.format(java.util.Locale.ROOT, "%.2f%%", fraction * 100);
    }

    static BenchQuery byId(List<BenchQuery> queries, String id) {
        for (BenchQuery query : queries) {
            if (query.id.equalsIgnoreCase(id)) {
                return query;
            }
        }
        throw new IllegalArgumentException("unknown query [" + id + "]; expected Q1..Q" + queries.size());
    }
}
