/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Rounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * For date histogram aggregation
 */
public abstract class DateHistogramAggregatorBridge extends AggregatorBridge {

    int maxRewriteFilters;

    protected boolean canOptimize(ValuesSourceConfig config, Rounding rounding) {
        /**
         * The filter rewrite optimized path does not support bucket intervals which are not fixed.
         * For this reason we exclude non UTC timezones.
         */
        if (rounding.isUTC() == false) {
            return false;
        }

        if (config.script() == null && config.missing() == null) {
            MappedFieldType fieldType = config.fieldType();
            if (fieldType != null && fieldType.unwrap() instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }

    protected void buildRanges(SearchContext context) throws IOException {
        long[] bounds = Helper.getDateHistoAggBounds(context, fieldType.name());
        this.maxRewriteFilters = context.maxAggRewriteFilters();
        setRanges.accept(buildRanges(bounds, maxRewriteFilters));
    }

    @Override
    final Ranges tryBuildRangesFromSegment(LeafReaderContext leaf) throws IOException {
        long[] bounds = Helper.getSegmentBounds(leaf, fieldType.name());
        return buildRanges(bounds, maxRewriteFilters);
    }

    private Ranges buildRanges(long[] bounds, int maxRewriteFilters) {
        bounds = processHardBounds(bounds);
        if (bounds == null) {
            return null;
        }
        assert bounds[0] <= bounds[1] : "Low bound should be less than high bound";

        final Rounding rounding = getRounding(bounds[0], bounds[1]);
        final OptionalLong intervalOpt = Rounding.getInterval(rounding);
        if (intervalOpt.isEmpty()) {
            return null;
        }
        final long interval = intervalOpt.getAsLong();

        // process the after key of composite agg
        bounds = processAfterKey(bounds, interval);

        return Helper.createRangesFromAgg(
            (DateFieldMapper.DateFieldType) fieldType,
            interval,
            getRoundingPrepared(),
            bounds[0],
            bounds[1],
            maxRewriteFilters
        );
    }

    protected abstract Rounding getRounding(final long low, final long high);

    protected abstract Rounding.Prepared getRoundingPrepared();

    protected long[] processAfterKey(long[] bounds, long interval) {
        return bounds;
    }

    protected long[] processHardBounds(long[] bounds) {
        return processHardBounds(bounds, null);
    }

    protected long[] processHardBounds(long[] bounds, LongBounds hardBounds) {
        if (bounds != null) {
            // Update min/max limit if user specified any hard bounds
            if (hardBounds != null) {
                if (hardBounds.getMin() > bounds[0]) {
                    bounds[0] = hardBounds.getMin();
                }
                if (hardBounds.getMax() - 1 < bounds[1]) {
                    bounds[1] = hardBounds.getMax() - 1; // hard bounds max is exclusive
                }
                if (bounds[0] > bounds[1]) {
                    return null;
                }
            }
        }
        return bounds;
    }

    private DateFieldMapper.DateFieldType getFieldType() {
        assert fieldType != null && fieldType.unwrap() instanceof DateFieldMapper.DateFieldType;
        return (DateFieldMapper.DateFieldType) fieldType;
    }

    /**
     * Get the size of buckets to stop early
     */
    protected int getSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    final FilterRewriteOptimizationContext.OptimizeResult tryOptimize(
        PointValues values,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) throws IOException {
        int size = getSize();

        DateFieldMapper.DateFieldType fieldType = getFieldType();

        Function<Integer, Long> getBucketOrd = (activeIndex) -> {
            long rangeStart = LongPoint.decodeDimension(ranges.lowers[activeIndex], 0);
            rangeStart = fieldType.convertNanosToMillis(rangeStart);
            return getBucketOrd(bucketOrdProducer().apply(rangeStart));
        };

        return getResult(values, incrementDocCount, ranges, getBucketOrd, size, subAggCollectorParam);
    }

    @Override
    final FilterRewriteOptimizationContext.OptimizeResult tryOptimizeWithDocValues(
        SortedNumericDocValues dvs,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges
    ) throws IOException {
        DateFieldMapper.DateFieldType fieldType = getFieldType();
        int rangeSize = ranges.getSize();
        long[] counts = new long[rangeSize];

        // Decode range boundaries to longs for efficient comparison
        long[] lowerVals = new long[rangeSize];
        long[] upperVals = new long[rangeSize];
        for (int i = 0; i < rangeSize; i++) {
            lowerVals[i] = NumericUtils.sortableBytesToLong(ranges.lowers[i], 0);
            upperVals[i] = NumericUtils.sortableBytesToLong(ranges.uppers[i], 0);
        }

        // Check if ranges have uniform interval for O(1) bucket computation
        long interval = upperVals[0] - lowerVals[0];
        boolean uniformInterval = interval > 0;
        for (int i = 1; i < rangeSize && uniformInterval; i++) {
            if (upperVals[i] - lowerVals[i] != interval || lowerVals[i] != upperVals[i - 1]) {
                uniformInterval = false;
            }
        }

        final long rangeMin = lowerVals[0];
        final long rangeMax = upperVals[rangeSize - 1];

        // Linear scan: iterate all docs and bucket each value
        while (dvs.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 0; i < dvs.docValueCount(); i++) {
                long val = dvs.nextValue();
                if (uniformInterval) {
                    // O(1) direct computation for uniform-interval date histograms
                    if (val >= rangeMin && val < rangeMax) {
                        int idx = (int) ((val - rangeMin) / interval);
                        if (idx >= 0 && idx < rangeSize) {
                            counts[idx]++;
                        }
                    }
                } else {
                    // Binary search for the matching range: ranges are sorted, [lower, upper)
                    int lo = 0, hi = rangeSize - 1;
                    while (lo <= hi) {
                        int mid = (lo + hi) >>> 1;
                        if (val < lowerVals[mid]) {
                            hi = mid - 1;
                        } else if (val >= upperVals[mid]) {
                            lo = mid + 1;
                        } else {
                            counts[mid]++;
                            break;
                        }
                    }
                }
            }
        }

        // Emit counts via incrementDocCount
        for (int i = 0; i < rangeSize; i++) {
            if (counts[i] > 0) {
                long rangeStart = LongPoint.decodeDimension(ranges.lowers[i], 0);
                rangeStart = fieldType.convertNanosToMillis(rangeStart);
                long bucketOrd = getBucketOrd(bucketOrdProducer().apply(rangeStart));
                incrementDocCount.accept(bucketOrd, counts[i]);
            }
        }

        return new FilterRewriteOptimizationContext.OptimizeResult();
    }

    @Override
    final FilterRewriteOptimizationContext.OptimizeResult tryOptimizeWithDocValues(
        SortedNumericDocValues dvs,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges,
        DocIdSetIterator matchingDocs
    ) throws IOException {
        DateFieldMapper.DateFieldType fieldType = getFieldType();
        int rangeSize = ranges.getSize();
        long[] counts = new long[rangeSize];

        // Decode range boundaries to longs for efficient comparison
        long[] lowerVals = new long[rangeSize];
        long[] upperVals = new long[rangeSize];
        for (int i = 0; i < rangeSize; i++) {
            lowerVals[i] = NumericUtils.sortableBytesToLong(ranges.lowers[i], 0);
            upperVals[i] = NumericUtils.sortableBytesToLong(ranges.uppers[i], 0);
        }

        // Check if ranges have uniform interval for O(1) bucket computation
        long interval = upperVals[0] - lowerVals[0];
        boolean uniformInterval = interval > 0;
        for (int i = 1; i < rangeSize && uniformInterval; i++) {
            if (upperVals[i] - lowerVals[i] != interval || lowerVals[i] != upperVals[i - 1]) {
                uniformInterval = false;
            }
        }

        final long rangeMin = lowerVals[0];
        final long rangeMax = upperVals[rangeSize - 1];

        // Selectivity-based path: when filter matches >50% of docs, linear DV scan
        // with filter check is faster than random-access advanceExact per matching doc.
        long filterCost = matchingDocs.cost();
        long dvCost = dvs.cost();
        if (filterCost > dvCost / 2) {
            // High selectivity: linear DV scan, skip non-matching docs via advance on matchingDocs
            int filterDoc = matchingDocs.nextDoc();
            while (dvs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                int dvDoc = dvs.docID();
                // Advance filter iterator to catch up with DV iterator
                while (filterDoc < dvDoc) {
                    filterDoc = matchingDocs.nextDoc();
                }
                if (filterDoc != dvDoc) continue;
                for (int i = 0; i < dvs.docValueCount(); i++) {
                    long val = dvs.nextValue();
                    if (uniformInterval) {
                        if (val >= rangeMin && val < rangeMax) {
                            int idx = (int) ((val - rangeMin) / interval);
                            if (idx >= 0 && idx < rangeSize) {
                                counts[idx]++;
                            }
                        }
                    } else {
                        int lo = 0, hi = rangeSize - 1;
                        while (lo <= hi) {
                            int mid = (lo + hi) >>> 1;
                            if (val < lowerVals[mid]) {
                                hi = mid - 1;
                            } else if (val >= upperVals[mid]) {
                                lo = mid + 1;
                            } else {
                                counts[mid]++;
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Low selectivity: iterate matching docs, random-access DV via advanceExact
            int doc;
            while ((doc = matchingDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (dvs.advanceExact(doc)) {
                    for (int i = 0; i < dvs.docValueCount(); i++) {
                        long val = dvs.nextValue();
                        if (uniformInterval) {
                            if (val >= rangeMin && val < rangeMax) {
                                int idx = (int) ((val - rangeMin) / interval);
                                if (idx >= 0 && idx < rangeSize) {
                                    counts[idx]++;
                                }
                            }
                        } else {
                            int lo = 0, hi = rangeSize - 1;
                            while (lo <= hi) {
                                int mid = (lo + hi) >>> 1;
                                if (val < lowerVals[mid]) {
                                    hi = mid - 1;
                                } else if (val >= upperVals[mid]) {
                                    lo = mid + 1;
                                } else {
                                    counts[mid]++;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Emit counts via incrementDocCount
        for (int i = 0; i < rangeSize; i++) {
            if (counts[i] > 0) {
                long rangeStart = LongPoint.decodeDimension(ranges.lowers[i], 0);
                rangeStart = fieldType.convertNanosToMillis(rangeStart);
                long bucketOrd = getBucketOrd(bucketOrdProducer().apply(rangeStart));
                incrementDocCount.accept(bucketOrd, counts[i]);
            }
        }

        return new FilterRewriteOptimizationContext.OptimizeResult();
    }

    private static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }

    /**
    * Provides a function to produce bucket ordinals from the lower bound of the range
    */
    protected abstract Function<Long, Long> bucketOrdProducer();
}
