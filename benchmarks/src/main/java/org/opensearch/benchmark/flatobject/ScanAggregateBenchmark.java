/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import org.opensearch.index.mapper.flatobject.ValueType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Experiments P2 and P6: the cost, and the allocation, of scanning many documents to aggregate one path.
 *
 * <p>Documents are visited in ascending order here, which is what an aggregation actually does — the opposite choice from
 * the point-read benchmark, and deliberate in both cases.
 *
 * <p>Pair with {@code -prof gc} for P6. {@code gc.alloc.rate.norm}, bytes allocated per operation, is a far steadier
 * signal for "does this arm materialise the whole document" than sampling heap usage would be.
 */
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by the benchmarking framework
public class ScanAggregateBenchmark {

    /** How many documents the scan covers. Sweeping this shows whether the gap widens with the number of documents. */
    @Param({ "1000", "10000", "100000" })
    public int docCount;

    private int scanLength;

    @Setup
    public void setup(AccessorBenchmarkState state) {
        scanLength = Math.min(docCount, state.maxDoc);
    }

    /**
     * The analogue of {@code sum(get(attributes, "status", long))}.
     */
    @Benchmark
    public long sumOverScan(AccessorBenchmarkState state) throws IOException {
        long total = 0;
        for (int docId = 0; docId < scanLength; docId++) {
            Object value = state.accessor.get(docId, "status", ValueType.LONG);
            if (value != null) {
                total += (Long) value;
            }
        }
        return total;
    }

    /**
     * The analogue of {@code stats ... by get(attributes, "k8s.namespace", string)}: a grouped aggregation, which reads a
     * string key as well as a numeric value from each document.
     */
    @Benchmark
    public void groupBySumOverScan(AccessorBenchmarkState state, Blackhole blackhole) throws IOException {
        Map<String, long[]> buckets = new HashMap<>();
        for (int docId = 0; docId < scanLength; docId++) {
            Object key = state.accessor.get(docId, "k8s.namespace", ValueType.STRING);
            Object value = state.accessor.get(docId, "status", ValueType.LONG);
            if (key != null && value != null) {
                buckets.computeIfAbsent((String) key, k -> new long[1])[0] += (Long) value;
            }
        }
        blackhole.consume(buckets);
    }
}
