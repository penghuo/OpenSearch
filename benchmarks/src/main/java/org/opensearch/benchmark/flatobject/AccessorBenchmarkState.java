/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.mapper.flatobject.FlatObjectValueAccessor;
import org.opensearch.index.mapper.flatobject.SourceValueAccessor;
import org.opensearch.index.mapper.flatobject.VariantBlobValueAccessor;
import org.opensearch.test.flatobject.OtelDocGenerator;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.file.Path;
import java.util.SplittableRandom;

/**
 * Shared state for the accessor benchmarks: opens an index the cluster wrote and binds one arm's accessor to it.
 *
 * <p>Reading the node's own index rather than building a second copy matters twice over. It avoids re-creating a
 * 100M-document corpus, and it guarantees the benchmark measures the same physical bytes, codec settings and segment
 * geometry the cluster actually serves — a purpose-built index could easily differ in compression or segment count and
 * quietly favour one arm.
 *
 * <p><b>The node must be stopped before running these.</b> A live node's merges delete files underneath an open reader.
 */
@State(Scope.Benchmark)
public class AccessorBenchmarkState {

    /** Path to a shard's Lucene directory, e.g. {@code <data>/nodes/0/indices/<uuid>/0/index}. */
    @Param({ "" })
    public String indexPath;

    /** Which value store to exercise: {@code source} or {@code variant_blob}. */
    @Param({ "source", "variant_blob" })
    public String arm;

    /** The field holding the flat_object value. */
    @Param({ "attributes" })
    public String field;

    private Directory directory;
    private DirectoryReader reader;

    public FlatObjectValueAccessor accessor;
    public int maxDoc;

    /**
     * A sparse sample of documents in <b>ascending</b> order: the realistic point-read pattern.
     *
     * <p>This is what a real query does — match a scattered subset of documents, then read them in document order. It is
     * also the only pattern both stores can serve on equal terms, and choosing it correctly took two attempts:
     *
     * <ul>
     *   <li>A fully <em>sequential</em> walk would let the operating system's readahead serve Solution A's stored-field
     *       blocks far better than any real query does, flattering A.
     *   <li>A fully <em>random</em> order is pathological for Solution B, because {@code BinaryDocValues} is a
     *       forward-only iterator: every backward step forces the iterator to be reopened and re-advanced from document
     *       zero, which is O(n) per read. Measured that way B came out 274x slower — an artifact of the access pattern,
     *       not of the store, and one no OpenSearch code path would produce, since aggregations scan ascending and the
     *       fetch phase sorts document ids before reading.
     * </ul>
     *
     * <p>Ascending-with-gaps avoids both distortions: the gaps deny A its readahead, and the ordering lets B's iterator
     * move forward only. The random-order cost is still measured separately, by
     * {@link PointReadBenchmark#getOnePathRandomOrder}, because it is a genuine asymmetry between the stores rather than
     * something to hide.
     */
    public int[] sampledDocIds;

    /**
     * A fixed pseudo-random document order, kept to quantify the backward-seek penalty rather than as a headline number.
     */
    public int[] shuffledDocIds;

    /** One document in this many is sampled for the ascending point-read pattern. */
    @Param({ "100" })
    public int sampleEveryNth;

    @Setup
    public void setup() throws IOException {
        if (indexPath == null || indexPath.isEmpty()) {
            throw new IllegalArgumentException("set -p indexPath=<shard index directory>");
        }
        directory = FSDirectory.open(Path.of(indexPath));
        reader = DirectoryReader.open(directory);

        // A single segment is preferred, so that segment geometry cannot differ between arms and confound the comparison.
        // Where a force merge was skipped, bind to the largest segment instead of failing: measuring one large segment is
        // still a valid comparison as long as both arms are measured the same way, which the warning makes visible.
        LeafReaderContext context = reader.leaves().get(0);
        for (LeafReaderContext candidate : reader.leaves()) {
            if (candidate.reader().maxDoc() > context.reader().maxDoc()) {
                context = candidate;
            }
        }
        if (reader.leaves().size() != 1) {
            System.out.println(
                "WARNING: "
                    + indexPath
                    + " has "
                    + reader.leaves().size()
                    + " segments; binding to the largest ("
                    + context.reader().maxDoc()
                    + " docs). Force-merge to 1 segment for a cleaner comparison."
            );
        }
        maxDoc = context.reader().maxDoc();

        accessor = newAccessor();
        accessor.setNextReader(context);
        if (accessor.valueStoreAvailable() == false) {
            throw new IllegalStateException("arm [" + arm + "] has no value store in " + indexPath);
        }

        // Fixed seed: reproducible, and identical across arms so both see the same access pattern.
        SplittableRandom random = new SplittableRandom(0x5EED);

        shuffledDocIds = new int[maxDoc];
        for (int i = 0; i < maxDoc; i++) {
            shuffledDocIds[i] = i;
        }
        for (int i = maxDoc - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int swap = shuffledDocIds[i];
            shuffledDocIds[i] = shuffledDocIds[j];
            shuffledDocIds[j] = swap;
        }

        // A sparse ascending sample: one document per stride, jittered within the stride so the gaps are irregular.
        int stride = Math.max(1, sampleEveryNth);
        int sampleSize = Math.max(1, maxDoc / stride);
        sampledDocIds = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            int base = i * stride;
            int jitter = stride > 1 ? random.nextInt(stride) : 0;
            sampledDocIds[i] = Math.min(maxDoc - 1, base + jitter);
        }
    }

    private FlatObjectValueAccessor newAccessor() {
        switch (arm) {
            case SourceValueAccessor.STORE_NAME:
                return new SourceValueAccessor(field);
            case VariantBlobValueAccessor.STORE_NAME:
                return new VariantBlobValueAccessor(field);
            default:
                throw new IllegalArgumentException(
                    "unknown arm [" + arm + "]; expected " + SourceValueAccessor.STORE_NAME + " or " + VariantBlobValueAccessor.STORE_NAME
                );
        }
    }

    /** The stable paths present in every generated document, safe for a benchmark to read. */
    public static String[] benchmarkPaths() {
        return new String[] {
            OtelDocGenerator.KEY_STATUS,
            OtelDocGenerator.KEY_LEVEL,
            OtelDocGenerator.KEY_K8S_NAMESPACE,
            OtelDocGenerator.KEY_DURATION_NS };
    }

    @TearDown
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
    }
}
