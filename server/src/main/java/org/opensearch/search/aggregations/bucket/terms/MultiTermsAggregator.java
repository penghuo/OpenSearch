/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * An aggregator that aggregate with multi-terms.
 */
public class MultiTermsAggregator extends DeferableBucketAggregator {

    private final BytesKeyedBucketOrds bucketOrds;
    private final MultiTermsValuesSource multiTermsValue;
    private final boolean showTermDocCountError;
    private final List<DocValueFormat> formats;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final BucketOrder order;
    private final Comparator<InternalMultiTerms.Bucket> partiallyBuiltBucketComparator;

    public MultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        boolean showTermDocCountError,
        MultiTermsValuesSource multiTermsValue,
        List<DocValueFormat> formats,
        BucketOrder order,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
        this.multiTermsValue = multiTermsValue;
        this.showTermDocCountError = showTermDocCountError;
        this.formats = formats;
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        this.partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalMultiTerms.Bucket[][] topBucketsPerOrd = new InternalMultiTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            // todo, what is it?
            // collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

            int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
            PriorityQueue<InternalMultiTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
            InternalMultiTerms.Bucket spare = null;
            BytesRef dest = null;
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            Supplier<InternalMultiTerms.Bucket> emptyBucketBuilder = () -> InternalMultiTerms.Bucket.EMPTY(formats);
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                otherDocCounts[ordIdx] += docCount;
                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                    continue;
                }
                if (spare == null) {
                    spare = emptyBucketBuilder.get();
                    dest = new BytesRef();
                }

                ordsEnum.readValue(dest);

                spare.termValues = decode(dest);
                spare.docCount = docCount;
                spare.bucketOrd = ordsEnum.ord();
                spare = ordered.insertWithOverflow(spare);
            }

            // Get the top buckets
            InternalMultiTerms.Bucket[] bucketsForOrd = new InternalMultiTerms.Bucket[ordered.size()];
            topBucketsPerOrd[ordIdx] = bucketsForOrd;
            for (int b = ordered.size() - 1; b >= 0; --b) {
                topBucketsPerOrd[ordIdx][b] = ordered.pop();
                otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][b].getDocCount();
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
        }
        return result;
    }

    InternalMultiTerms buildResult(long owningBucketOrd, long otherDocCount, InternalMultiTerms.Bucket[] topBuckets) {
        BucketOrder reduceOrder;
        if (isKeyOrder(order) == false) {
            reduceOrder = InternalOrder.key(true);
            Arrays.sort(topBuckets, reduceOrder.comparator());
        } else {
            reduceOrder = order;
        }
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            otherDocCount,
            0,
            formats,
            org.opensearch.common.collect.List.of(topBuckets)
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return null;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                for (List<Object> value : collector.collect(doc)) {
                    long bucketOrd = bucketOrds.add(owningBucketOrd, encode(value));
                    if (bucketOrd < 0) {
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                    }
                }
            }
        };
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    private static BytesRef encode(List<Object> values) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeCollection(values, StreamOutput::writeGenericValue);
            return output.bytes().toBytesRef();
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private static List<Object> decode(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return input.readList(StreamInput::readGenericValue);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * A collection of {@link InternalValuesSource}
     */
    public static class MultiTermsValuesSource {

        private final List<InternalValuesSource> valuesSources;

        public MultiTermsValuesSource(List<InternalValuesSource> valuesSources) {
            this.valuesSources = valuesSources;
        }

        public MultiTermsValuesSourceCollector getValues(LeafReaderContext ctx) throws IOException {
            List<InternalValuesSourceCollectors> collectors = new ArrayList<>();
            for (InternalValuesSource valuesSource : valuesSources) {
                collectors.add(valuesSource.apply(ctx));
            }
            return new MultiTermsValuesSourceCollector(collectors);
        }
    }

    public static class MultiTermsValuesSourceCollector {
        private final List<InternalValuesSourceCollectors> collectors;

        public MultiTermsValuesSourceCollector(List<InternalValuesSourceCollectors> collectors) {
            this.collectors = collectors;
        }

        /**
         * Cartesian product of values of each {@link InternalValuesSource}.
         */
        public List<List<Object>> collect(int doc) throws IOException {
            List<Supplier<List<Object>>> collectedValues = new ArrayList<>();
            for (InternalValuesSourceCollectors collector : collectors) {
                collectedValues.add(() -> collector.apply(doc));
            }
            List<List<Object>> ret = new ArrayList<>();
            dfs(0, collectedValues, new ArrayList<>(), ret);
            return ret;
        }

        public void dfs(int index, List<Supplier<List<Object>>> collectedValues, List<Object> current, List<List<Object>> values)
            throws IOException {
            if (index == collectedValues.size()) {
                values.add(org.opensearch.common.collect.List.copyOf(current));
            } else if (null != collectedValues.get(index)) {
                for (Object value : collectedValues.get(index).get()) {
                    current.add(value);
                    dfs(index + 1, collectedValues, current, values);
                    current.remove(current.size() - 1);
                }
            }
        }
    }

    /**
     * An adapter of {@link ValuesSource}.
     */
    public interface InternalValuesSource {
        InternalValuesSourceCollectors apply(LeafReaderContext ctx) throws IOException;
    }

    public interface InternalValuesSourceCollectors {
        List<Object> apply(int doc) throws IOException;
    }

    private interface Supplier<T> {
        T get() throws IOException;
    }

    public static class BytesInternalValuesSource implements InternalValuesSource {
        private final ValuesSource valuesSource;

        public BytesInternalValuesSource(ValuesSource valuesSource) {
            this.valuesSource = valuesSource;
        }

        @Override
        public InternalValuesSourceCollectors apply(LeafReaderContext ctx) throws IOException {
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            return doc -> {
                BytesRefBuilder previous = new BytesRefBuilder();

                if (false == values.advanceExact(doc)) {
                    return Collections.emptyList();
                }
                int valuesCount = values.docValueCount();
                List<Object> termValues = new ArrayList<>(valuesCount);

                // SortedBinaryDocValues don't guarantee uniqueness so we
                // need to take care of dups
                previous.clear();
                for (int i = 0; i < valuesCount; ++i) {
                    BytesRef bytes = values.nextValue();
                    if (i > 0 && previous.get().equals(bytes)) {
                        continue;
                    }
                    previous.copyBytes(bytes);
                    termValues.add(BytesRef.deepCopyOf(bytes));
                }
                return termValues;
            };
        }
    }

    public static class LongInternalValuesSource implements InternalValuesSource {
        private final ValuesSource.Numeric valuesSource;

        public LongInternalValuesSource(ValuesSource.Numeric valuesSource) {
            this.valuesSource = valuesSource;
        }

        @Override
        public InternalValuesSourceCollectors apply(LeafReaderContext ctx) throws IOException {
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            return doc -> {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();
                    List<Object> termValues = new ArrayList<>(valuesCount);
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long val = values.nextValue();
                        if (previous != val || i == 0) {
                            previous = val;
                            termValues.add(val);
                        }
                    }
                    return termValues;
                }
                return Collections.emptyList();
            };
        }
    }

    public static class DoubleInternalValuesSource implements InternalValuesSource {
        private final ValuesSource.Numeric valuesSource;

        public DoubleInternalValuesSource(ValuesSource.Numeric valuesSource) {
            this.valuesSource = valuesSource;
        }

        @Override
        public InternalValuesSourceCollectors apply(LeafReaderContext ctx) throws IOException {
            SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
            return doc -> {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();
                    List<Object> termValues = new ArrayList<>(valuesCount);
                    double previous = Double.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        double val = values.nextValue();
                        if (previous != val || i == 0) {
                            previous = val;
                            termValues.add(val);
                        }
                    }
                    return termValues;
                }
                return Collections.emptyList();
            };
        }
    }
}
