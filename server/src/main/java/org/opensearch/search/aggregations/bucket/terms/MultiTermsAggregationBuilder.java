/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS;

/**
 * Todo. MultiTermsAggregationBuilder.
 */
public class MultiTermsAggregationBuilder extends AbstractAggregationBuilder<MultiTermsAggregationBuilder> {
    public static final String NAME = "multi_terms";
    public static final ObjectParser<MultiTermsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        MultiTermsAggregationBuilder::new
    );

    public static final ParseField TERMS_FIELD = new ParseField("terms");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");

    @Override
    public String getType() {
        return NAME;
    }

    static {
        final ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> parser = MultiTermsValuesSourceConfig.PARSER.apply(
            true,
            true,
            false,
            true,
            true
        );
        PARSER.declareObjectArray(MultiTermsAggregationBuilder::terms, (p, c) -> parser.parse(p, null).build(), TERMS_FIELD);

        PARSER.declareBoolean(MultiTermsAggregationBuilder::showTermDocCountError, SHOW_TERM_DOC_COUNT_ERROR);

        PARSER.declareInt(MultiTermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(MultiTermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(MultiTermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(MultiTermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareObjectArray(MultiTermsAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p), ORDER_FIELD);

        PARSER.declareField(
            MultiTermsAggregationBuilder::collectMode,
            (p, c) -> Aggregator.SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
            Aggregator.SubAggCollectionMode.KEY,
            ObjectParser.ValueType.STRING
        );
    }

    public static final ValuesSourceRegistry.RegistryKey<MultiTermsAggregationFactory.InternalValuesSourceSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(
            MultiTermsAggregationBuilder.NAME,
            MultiTermsAggregationFactory.InternalValuesSourceSupplier.class
        );

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            REGISTRY_KEY,
            org.opensearch.common.collect.List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            config -> new MultiTermsAggregator.BytesInternalValuesSource(config.getValuesSource()),
            true
        );

        builder.register(REGISTRY_KEY, org.opensearch.common.collect.List.of(CoreValuesSourceType.NUMERIC), config -> {
            ValuesSource.Numeric valuesSource = ((ValuesSource.Numeric) config.getValuesSource());
            return valuesSource.isFloatingPoint()
                ? new MultiTermsAggregator.DoubleInternalValuesSource(valuesSource)
                : new MultiTermsAggregator.LongInternalValuesSource(valuesSource);
        }, true);

        builder.register(
            REGISTRY_KEY,
            org.opensearch.common.collect.List.of(CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE),
            config -> {
                ValuesSource.Numeric valuesSource = ((ValuesSource.Numeric) config.getValuesSource());
                return new MultiTermsAggregator.LongInternalValuesSource(valuesSource);
            },
            true
        );

        builder.registerUsage(NAME);
    }

    private List<MultiTermsValuesSourceConfig> terms;

    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false)); // automatically adds tie-breaker key asc order
    private Aggregator.SubAggCollectionMode collectMode = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS
    );
    private boolean showTermDocCountError = false;
    private String format = null;

    public MultiTermsAggregationBuilder(String name) {
        super(name);
    }

    protected MultiTermsAggregationBuilder(
        MultiTermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.terms = new ArrayList<>(clone.terms);
        this.order = clone.order;
        this.collectMode = clone.collectMode;
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(clone.bucketCountThresholds);
        this.showTermDocCountError = clone.showTermDocCountError;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MultiTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public MultiTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        terms = in.readList(MultiTermsValuesSourceConfig::new);
        bucketCountThresholds = new TermsAggregator.BucketCountThresholds(in);
        collectMode = in.readOptionalWriteable(Aggregator.SubAggCollectionMode::readFromStream);
        order = InternalOrder.Streams.readOrder(in);
        showTermDocCountError = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(terms);
        bucketCountThresholds.writeTo(out);
        out.writeOptionalWriteable(collectMode);
        order.writeTo(out);
        out.writeBoolean(showTermDocCountError);
    }

    @Override
    protected AggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        List<ValuesSourceConfig> configs = terms.stream()
            .map(
                f -> ValuesSourceConfig.resolveUnregistered(
                    queryShardContext,
                    f.getUserValueTypeHint(),
                    f.getFieldName(),
                    f.getScript(),
                    f.getMissing(),
                    f.getTimeZone(),
                    f.getFormat(),
                    CoreValuesSourceType.BYTES
                )
            )
            .collect(Collectors.toList());
        return new MultiTermsAggregationFactory(
            name,
            queryShardContext,
            parent,
            subfactoriesBuilder,
            metadata,
            configs,
            configs.stream().map(ValuesSourceConfig::format).collect(Collectors.toList()),
            order,
            bucketCountThresholds,
            showTermDocCountError
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (terms != null) {
            builder.field(TERMS_FIELD.getPreferredName(), terms);
        }
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (collectMode != null) {
            builder.field(Aggregator.SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        }
        builder.endObject();
        return builder;
    }

    /**
     * Set the terms.
     */
    public MultiTermsAggregationBuilder terms(List<MultiTermsValuesSourceConfig> terms) {
        if (terms == null) {
            throw new IllegalArgumentException("[terms] must not be null. Found null terms in [" + name + "]");
        }
        if (terms.size() < 2) {
            throw new IllegalArgumentException(
                "multi term aggregation must has at least 2 terms. Found ["
                    + terms.size()
                    + "] in"
                    + " ["
                    + name
                    + "]"
                    + (terms.size() == 1 ? " Use terms aggregation for single term aggregation" : "")
            );
        }
        this.terms = terms;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public MultiTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Returns the number of term buckets currently configured
     */
    public int size() {
        return bucketCountThresholds.getRequiredSize();
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public MultiTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Returns the number of term buckets per shard that are currently configured
     */
    public int shardSize() {
        return bucketCountThresholds.getShardSize();
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public MultiTermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Returns the minimum document count required per term
     */
    public long minDocCount() {
        return bucketCountThresholds.getMinDocCount();
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public MultiTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Returns the minimum document count required per term, per shard
     */
    public long shardMinDocCount() {
        return bucketCountThresholds.getShardMinDocCount();
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public MultiTermsAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if (order instanceof InternalOrder.CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order; // if order already contains a tie-breaker we are good to go
        } else { // otherwise add a tie-breaker by using a compound order
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned. A tie-breaker may be added to avoid non-deterministic
     * ordering.
     */
    public MultiTermsAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public BucketOrder order() {
        return order;
    }

    /**
     * Expert: set the collection mode.
     */
    public MultiTermsAggregationBuilder collectMode(Aggregator.SubAggCollectionMode collectMode) {
        if (collectMode == null) {
            throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
        }
        this.collectMode = collectMode;
        return this;
    }

    /**
     * Expert: get the collection mode.
     */
    public Aggregator.SubAggCollectionMode collectMode() {
        return collectMode;
    }

    /**
     * Get whether doc count error will be return for individual terms
     */
    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    /**
     * Set whether doc count error will be return for individual terms
     */
    public MultiTermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    @SuppressWarnings("unchecked")
    public MultiTermsAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketCountThresholds, collectMode, order, showTermDocCountError);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MultiTermsAggregationBuilder other = (MultiTermsAggregationBuilder) obj;
        return Objects.equals(terms, other.terms)
            && Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
            && Objects.equals(collectMode, other.collectMode)
            && Objects.equals(order, other.order)
            && Objects.equals(showTermDocCountError, other.showTermDocCountError);
    }
}
