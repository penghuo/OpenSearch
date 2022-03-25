/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder.REGISTRY_KEY;

/**
 * Factory of {@link MultiTermsAggregator}.
 */
public class MultiTermsAggregationFactory extends AggregatorFactory {

    private final List<ValuesSourceConfig> configs;
    private final List<DocValueFormat> formats;

    /**
     * Fields inherent from Terms Aggregation Factory.
     */
    private final BucketOrder order;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;

    public MultiTermsAggregationFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<ValuesSourceConfig> configs,
        List<DocValueFormat> formats,
        BucketOrder order,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        boolean showTermDocCountError
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
        this.formats = formats;
        this.order = order;
        this.bucketCountThresholds = bucketCountThresholds;
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    protected Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && bucketCountThresholds.getShardSize() == TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();
        return new MultiTermsAggregator(
            name,
            factories,
            showTermDocCountError,
            new MultiTermsAggregator.MultiTermsValuesSource(
                configs.stream()
                    .map(config -> queryShardContext.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config).build(config))
                    .collect(Collectors.toList())
            ),
            configs.stream().map(ValuesSourceConfig::format).collect(Collectors.toList()),
            order,
            bucketCountThresholds,
            searchContext,
            parent,
            cardinality,
            metadata
        );
    }

    public interface InternalValuesSourceSupplier {
        MultiTermsAggregator.InternalValuesSource build(ValuesSourceConfig valuesSourceConfig);
    }
}
