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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Experiment P1: the cost of reading one path out of one document.
 *
 * <p>The headline measurements visit a sparse <b>ascending</b> sample of documents, which is what a real query does and
 * the only pattern both stores can serve on equal terms. See
 * {@link AccessorBenchmarkState#sampledDocIds} for why a fully sequential or fully random order would each distort the
 * comparison, in opposite directions.
 *
 * <p>Run with the node stopped, since a live node's merges delete files underneath an open reader:
 *
 * <pre>
 * ./gradlew :benchmarks:run --args="PointReadBenchmark \
 *   -p indexPath=&lt;shard&gt;/index -p arm=source -prof gc"
 * </pre>
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by the benchmarking framework
public class PointReadBenchmark {

    /**
     * The path to read. {@code status} is a scalar, and {@code k8s.namespace} is a literal dotted key that exercises the
     * path rule's one-probe fast path.
     */
    @Param({ "status", "level", "k8s.namespace" })
    public String path;

    private int cursor;

    private static ValueType typeFor(String path) {
        return "status".equals(path) || "duration_ns".equals(path) ? ValueType.LONG : ValueType.STRING;
    }

    /**
     * The headline point read: one path from a scattered but ascending document sample.
     */
    @Benchmark
    public void getOnePath(AccessorBenchmarkState state, Blackhole blackhole) throws IOException {
        int docId = state.sampledDocIds[cursor++ % state.sampledDocIds.length];
        blackhole.consume(state.accessor.get(docId, path, typeFor(path)));
    }

    /**
     * Reads several paths from the same document, the shape a query with more than one derived field takes. Both arms cache
     * the current document, so this isolates the marginal cost of an extra path from the cost of an extra document.
     */
    @Benchmark
    public void getFourPathsFromOneDocument(AccessorBenchmarkState state, Blackhole blackhole) throws IOException {
        int docId = state.sampledDocIds[cursor++ % state.sampledDocIds.length];
        blackhole.consume(state.accessor.get(docId, "status", ValueType.LONG));
        blackhole.consume(state.accessor.get(docId, "duration_ns", ValueType.LONG));
        blackhole.consume(state.accessor.get(docId, "level", ValueType.STRING));
        blackhole.consume(state.accessor.get(docId, "k8s.namespace", ValueType.STRING));
    }

    /**
     * Reconstructs the whole value: the one operation where the blob has to decode everything rather than slice, and the
     * one where stored fields do no more work than usual.
     */
    @Benchmark
    public void reconstructWholeValue(AccessorBenchmarkState state, Blackhole blackhole) throws IOException {
        int docId = state.sampledDocIds[cursor++ % state.sampledDocIds.length];
        blackhole.consume(state.accessor.getAll(docId));
    }

    /**
     * Random document order, reported as a limitation rather than as a headline.
     *
     * <p>Stored fields are randomly addressable; {@code BinaryDocValues} is a forward-only iterator, so every backward step
     * costs a reopen and a re-advance from document zero. Any consumer that cannot guarantee ascending access pays that,
     * and the difference is large enough to matter — which is why it is measured rather than assumed away. Keep the
     * document count modest here; on a large index this benchmark is quadratic for the blob arm.
     */
    @Benchmark
    @Measurement(iterations = 2, time = 2)
    @Warmup(iterations = 1, time = 1)
    public void getOnePathRandomOrder(AccessorBenchmarkState state, Blackhole blackhole) throws IOException {
        int docId = state.shuffledDocIds[cursor++ % state.shuffledDocIds.length];
        blackhole.consume(state.accessor.get(docId, path, typeFor(path)));
    }
}
