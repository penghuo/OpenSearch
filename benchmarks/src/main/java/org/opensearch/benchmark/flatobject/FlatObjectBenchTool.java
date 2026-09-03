/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import org.opensearch.test.flatobject.CorpusConfig;
import org.opensearch.test.flatobject.OtelDocGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Driver for the {@code flat_object} value-store comparison: creates indices, bulk-loads the synthetic corpus, reports
 * index size, and times end-to-end queries.
 *
 * <p>Run it through the dedicated Gradle task, since {@code :benchmarks:run} is wired to JMH:
 *
 * <pre>
 * ./gradlew :benchmarks:flatObjectBench --args="info   --preset SMOKE"
 * ./gradlew :benchmarks:flatObjectBench --args="create --index bench-a --arm a"
 * ./gradlew :benchmarks:flatObjectBench --args="index  --index bench-a --preset SMOKE"
 * ./gradlew :benchmarks:flatObjectBench --args="sizes  --index bench-a --data-dir /path/to/data"
 * ./gradlew :benchmarks:flatObjectBench --args="query  --index bench-a --op agg --iters 50"
 * </pre>
 */
public final class FlatObjectBenchTool {

    private static final String DEFAULT_HOST = "http://localhost:9200";
    private static final int DEFAULT_BULK_SIZE = 2000;
    private static final int DEFAULT_THREADS = 8;
    /**
     * Documents excluded from the throughput figure. The first bulks pay for JIT warm-up, the first segment flushes and
     * the empty-index fast paths, none of which represent steady-state ingestion.
     */
    private static final int DEFAULT_WARMUP_DOCS = 100_000;

    private FlatObjectBenchTool() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        String command = args[0];
        Map<String, String> options = parseOptions(args, 1);
        BenchHttp http = new BenchHttp(options.getOrDefault("host", DEFAULT_HOST));

        switch (command) {
            case "info" -> info(options);
            case "create" -> create(http, options);
            case "index" -> index(http, options);
            case "sizes" -> sizes(http, options);
            case "query" -> query(http, options);
            case "queries" -> listQueries(options);
            default -> {
                System.err.println("unknown command [" + command + "]");
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.out.println("""
            FlatObjectBenchTool <command> [--option value]

            Commands:
              info   --preset P                           print the corpus config and a sample document
              create --index NAME --arm {a|b|b-nosource}  create the index for one arm
              index  --index NAME --preset P              bulk load the corpus, reporting throughput (P4)
              sizes  --index NAME [--data-dir DIR]        report index size, split by file extension (P3)
              queries --preset P                          print the hardened Q1..Q11 query set
              query  --index NAME [--q Q1,Q4]             time the query set end to end (p50/p90)

            Common options:
              --host URL           default http://localhost:9200
              --threads N          bulk indexing concurrency, default 8
              --bulk-size N        documents per bulk request, default 2000
              --warmup N           documents excluded from the throughput figure, default 100000
              --iters N            query repetitions, default 50
              --docs N             override the preset document count (useful for a short run)
              --results FILE       append a JSON line of results here
              --dry-run            for `index`, print the first bulk body instead of sending it
            """);
    }

    // ---------------------------------------------------------------- info

    private static void info(Map<String, String> options) {
        CorpusConfig config = corpus(options);
        OtelDocGenerator generator = new OtelDocGenerator(config);
        System.out.println(config);
        long attrTotal = 0;
        int sample = Math.min(1000, config.docCount());
        for (int i = 0; i < sample; i++) {
            attrTotal += generator.attributesBytes(i);
        }
        System.out.printf(Locale.ROOT, "mean attributes bytes over %d docs: %.1f%n", sample, (double) attrTotal / sample);
        long docTotal = 0;
        for (int i = 0; i < sample; i++) {
            docTotal += generator.document(i).length;
        }
        System.out.printf(Locale.ROOT, "mean document bytes over %d docs: %.1f%n", sample, (double) docTotal / sample);
        System.out.printf(Locale.ROOT, "projected raw corpus: %.2f GB%n", (double) docTotal / sample * config.docCount() / 1e9);

        if (generator.sparseKeyPoolSize() > 0) {
            int minKeys = Integer.MAX_VALUE;
            int maxKeys = 0;
            long keySum = 0;
            java.util.Set<String> distinct = new java.util.HashSet<>();
            for (int i = 0; i < sample; i++) {
                Map<String, Object> attributes = generator.attributesAsMap(i);
                minKeys = Math.min(minKeys, attributes.size());
                maxKeys = Math.max(maxKeys, attributes.size());
                keySum += attributes.size();
                distinct.addAll(attributes.keySet());
            }
            System.out.printf(Locale.ROOT, "key pool: %d keys%n", generator.sparseKeyPoolSize());
            System.out.printf(Locale.ROOT, "keys/doc: %d..%d, mean %.1f%n", minKeys, maxKeys, (double) keySum / sample);
            System.out.printf(Locale.ROOT, "distinct keys seen in %d docs: %d%n", sample, distinct.size());
            System.out.println("probe-key selectivity (measured, not derived):");
            for (int rank : new int[] { 0, 1, 4, 9, 24, 49, 99, 249, 499, 999 }) {
                if (rank >= generator.sparseKeyPoolSize()) {
                    break;
                }
                String key = generator.sparseKeyAtRank(rank);
                System.out.printf(
                    Locale.ROOT,
                    "  rank %4d  %-36s %7.3f%% of docs%n",
                    rank,
                    key,
                    100.0 * generator.measuredPresence(key, sample)
                );
            }
        }
        // Composition of the Variant blob: how much is the per-document key dictionary versus the values themselves.
        long metaBytes = 0;
        long valueBytes = 0;
        long keyNameBytes = 0;
        long keyCount = 0;
        for (int i = 0; i < sample; i++) {
            org.opensearch.common.variant.VariantBuilder builder = new org.opensearch.common.variant.VariantBuilder();
            Map<String, Object> attributes = generator.attributesAsMap(i);
            org.opensearch.common.variant.VariantJson.encodeObject(attributes, builder);
            org.opensearch.common.variant.Variant variant = builder.finish();
            metaBytes += variant.metadataBytes().length;
            valueBytes += variant.valueBytes().length;
            for (String key : attributes.keySet()) {
                keyNameBytes += key.getBytes(StandardCharsets.UTF_8).length;
                keyCount++;
            }
        }
        double total = (double) (metaBytes + valueBytes) / sample;
        System.out.printf(Locale.ROOT, "%nvariant blob composition (mean per document over %d docs):%n", sample);
        System.out.printf(
            Locale.ROOT,
            "  metadata (key dictionary) : %7.1f B  (%.1f%% of blob)%n",
            (double) metaBytes / sample,
            100.0 * metaBytes / (metaBytes + valueBytes)
        );
        System.out.printf(
            Locale.ROOT,
            "  value tree                : %7.1f B  (%.1f%% of blob)%n",
            (double) valueBytes / sample,
            100.0 * valueBytes / (metaBytes + valueBytes)
        );
        System.out.printf(Locale.ROOT, "  total blob                : %7.1f B%n", total);
        System.out.printf(
            Locale.ROOT,
            "  of which raw key names    : %7.1f B  (%.1f keys/doc, %.1f B/key)%n",
            (double) keyNameBytes / sample,
            (double) keyCount / sample,
            (double) keyNameBytes / keyCount
        );
        System.out.println("sample document 0: " + new String(generator.document(0), StandardCharsets.UTF_8));
        System.out.println("sample document 1: " + new String(generator.document(1), StandardCharsets.UTF_8));
    }

    // -------------------------------------------------------------- create

    private static void create(BenchHttp http, Map<String, String> options) throws Exception {
        String index = required(options, "index");
        BenchArm arm = BenchArm.fromLabel(required(options, "arm"));
        BenchProbes probes = BenchProbes.forConfig(CorpusConfig.preset(options.getOrDefault("preset", "E10M")));
        System.out.println("probes: " + probes.describe());
        String body = arm.createIndexBody(probes);
        if (options.containsKey("dry-run")) {
            System.out.println(body);
            return;
        }
        http.deleteIfExists("/" + index);
        http.put("/" + index, body);
        System.out.println("created index [" + index + "] for arm [" + arm.label() + "]");
    }

    // --------------------------------------------------------------- index

    private static void index(BenchHttp http, Map<String, String> options) throws Exception {
        String index = required(options, "index");
        CorpusConfig config = corpus(options);
        int threads = intOption(options, "threads", DEFAULT_THREADS);
        int bulkSize = intOption(options, "bulk-size", DEFAULT_BULK_SIZE);
        int warmupDocs = intOption(options, "warmup", DEFAULT_WARMUP_DOCS);
        int totalDocs = config.docCount();

        OtelDocGenerator generator = new OtelDocGenerator(config);

        if (options.containsKey("dry-run")) {
            System.out.println(new String(bulkBody(generator, 0, Math.min(3, bulkSize), totalDocs), StandardCharsets.UTF_8));
            return;
        }

        System.out.println("indexing " + totalDocs + " docs into [" + index + "] with " + threads + " threads, bulk " + bulkSize);

        AtomicLong completed = new AtomicLong();
        AtomicLong measuredStartNanos = new AtomicLong(-1);
        AtomicLong measuredStartDocs = new AtomicLong(-1);
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());

        // Each thread owns a strided range so that document indices, and therefore documents, are covered exactly once.
        int bulksTotal = (totalDocs + bulkSize - 1) / bulkSize;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long wallStart = System.nanoTime();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                for (int bulk = threadId; bulk < bulksTotal; bulk += threads) {
                    int from = bulk * bulkSize;
                    int count = Math.min(bulkSize, totalDocs - from);
                    byte[] body = bulkBody(generator, from, count, totalDocs);
                    long start = System.nanoTime();
                    try {
                        String response = http.postBytes("/" + index + "/_bulk", body);
                        if (response.contains("\"errors\":true")) {
                            throw new IOException("bulk reported errors: " + response.substring(0, Math.min(600, response.length())));
                        }
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException("bulk starting at doc " + from + " failed", e);
                    }
                    latencies.add(System.nanoTime() - start);
                    long done = completed.addAndGet(count);
                    if (done >= warmupDocs) {
                        // Latch the point at which steady state begins, once.
                        measuredStartNanos.compareAndSet(-1, System.nanoTime());
                        measuredStartDocs.compareAndSet(-1, done);
                    }
                }
                return null;
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);
        long wallEnd = System.nanoTime();

        double wallSeconds = (wallEnd - wallStart) / 1e9;
        double steadySeconds;
        long steadyDocs;
        if (measuredStartNanos.get() > 0 && totalDocs > measuredStartDocs.get()) {
            steadySeconds = (wallEnd - measuredStartNanos.get()) / 1e9;
            steadyDocs = totalDocs - measuredStartDocs.get();
        } else {
            // The corpus never got past warm-up, so the only honest figure is the whole-run one.
            steadySeconds = wallSeconds;
            steadyDocs = totalDocs;
            System.out.println("note: corpus smaller than the warmup threshold, reporting whole-run throughput");
        }

        List<Long> sorted = new ArrayList<>(latencies);
        java.util.Collections.sort(sorted);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("op", "index");
        result.put("index", index);
        result.put("preset", config.name());
        result.put("docs", totalDocs);
        result.put("threads", threads);
        result.put("bulk_size", bulkSize);
        result.put("wall_seconds", round(wallSeconds));
        result.put("steady_docs", steadyDocs);
        result.put("steady_seconds", round(steadySeconds));
        result.put("docs_per_second", round(steadyDocs / steadySeconds));
        result.put("bulk_p50_ms", round(percentile(sorted, 50) / 1e6));
        result.put("bulk_p99_ms", round(percentile(sorted, 99) / 1e6));

        System.out.println("flush and force merge to a single segment");
        http.post("/" + index + "/_flush", "");
        long mergeStart = System.nanoTime();
        forceMergeAndWait(http, index);
        result.put("forcemerge_seconds", round((System.nanoTime() - mergeStart) / 1e9));
        http.post("/" + index + "/_refresh", "");

        long counted = BenchHttp.extractLong(http.get("/" + index + "/_count"), "count");
        result.put("indexed_count", counted);
        if (counted != totalDocs) {
            System.out.println("WARNING: expected " + totalDocs + " documents but the index holds " + counted);
        }

        report(result, options);
    }

    /**
     * Builds an NDJSON bulk body for documents {@code [from, from+count)}.
     */
    private static byte[] bulkBody(OtelDocGenerator generator, int from, int count, int totalDocs) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(count * 512);
        byte[] action = "{\"index\":{}}\n".getBytes(StandardCharsets.UTF_8);
        try {
            for (int i = from; i < from + count && i < totalDocs; i++) {
                out.write(action);
                out.write(generator.document(i));
                out.write('\n');
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    // --------------------------------------------------------------- sizes

    private static void sizes(BenchHttp http, Map<String, String> options) throws Exception {
        String index = required(options, "index");
        waitForStableStore(http, index);
        String stats = http.get("/" + index + "/_stats/store,docs");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("op", "sizes");
        result.put("index", index);
        int segments = countSegments(http, index);
        result.put("segments", segments);
        if (segments != 1) {
            // Small segments are written as compound files, which roll every extension into one `.cfs`. The
            // per-extension breakdown is then meaningless — and silently so, since `.dvd` would appear tiny rather than
            // absent. Say so rather than emit a number that looks usable.
            System.out.println(
                "WARNING: ["
                    + index
                    + "] has "
                    + segments
                    + " segments; compound files hide the per-extension breakdown, "
                    + "so stored-fields and doc-values figures below are NOT comparable. Force-merge to 1 segment first."
            );
            result.put("breakdown_valid", false);
        } else {
            result.put("breakdown_valid", true);
        }
        result.put("docs", BenchHttp.extractLong(stats, "count"));
        result.put("store_bytes", BenchHttp.extractLong(stats, "size_in_bytes"));

        String dataDir = options.get("data-dir");
        if (dataDir != null) {
            String uuid = BenchHttp.extractString(http.get("/" + index + "/_settings"), "uuid");
            if (uuid == null) {
                System.out.println("could not determine the index uuid, skipping the per-extension breakdown");
            } else {
                Map<String, Long> byExtension = extensionSizes(Path.of(dataDir), uuid);
                result.put("by_extension", byExtension);
                // The two figures the storage question turns on: `_source` lives in the stored-fields files, the blob
                // column and the flat_object term columns share the doc-values files.
                result.put("stored_fields_bytes", byExtension.getOrDefault("fdt", 0L) + byExtension.getOrDefault("fdm", 0L));
                result.put("doc_values_bytes", byExtension.getOrDefault("dvd", 0L) + byExtension.getOrDefault("dvm", 0L));
            }
        } else {
            System.out.println("pass --data-dir to also get the per-extension breakdown needed for the storage comparison");
        }
        report(result, options);
    }

    /**
     * Force merges to one segment without holding an HTTP request open for the duration.
     *
     * <p>A synchronous force merge of a 100GB index runs far longer than any sane socket timeout — the first attempt at
     * this failed with {@code HttpTimeoutException} after 10 minutes, which aborted the run and left a later size read
     * measuring a half-merged index. Issuing the merge with {@code wait_for_completion=false} and polling the segment
     * count decouples the wait from the connection.
     */
    private static void forceMergeAndWait(BenchHttp http, String index) throws Exception {
        http.post("/" + index + "/_forcemerge?max_num_segments=1&wait_for_completion=false", "");
        int previous = -1;
        for (int attempt = 0; attempt < 2880; attempt++) { // up to 8 hours at 10s intervals
            Thread.sleep(10_000);
            int segments = countSegments(http, index);
            if (segments == 1) {
                return;
            }
            if (segments != previous) {
                System.out.println("  merging: " + segments + " segments remain");
                previous = segments;
            }
        }
        System.out.println("WARNING: [" + index + "] did not reach a single segment; read measurements will not be comparable");
    }

    private static int countSegments(BenchHttp http, String index) throws Exception {
        String response = http.get("/_cat/segments/" + index + "?h=segment");
        if (response.isBlank()) {
            return 0;
        }
        return response.trim().split("\n").length;
    }

    /**
     * Waits until the reported store size stops changing.
     *
     * <p>Immediately after a force merge the segments that were merged away can still be on disk, so a size read taken
     * right then overstates the index — by 2x in one observed case, which is more than the difference the storage
     * comparison is trying to measure. Polling until two consecutive reads agree removes that trap rather than leaving it
     * for whoever reads the results to notice.
     */
    private static void waitForStableStore(BenchHttp http, String index) throws Exception {
        long previous = -1;
        for (int attempt = 0; attempt < 60; attempt++) {
            long current = BenchHttp.extractLong(http.get("/" + index + "/_stats/store"), "size_in_bytes");
            if (current == previous) {
                return;
            }
            previous = current;
            Thread.sleep(2000);
        }
        System.out.println("WARNING: store size for [" + index + "] did not settle; sizes may include merged-away segments");
    }

    /**
     * Sums Lucene file sizes by extension for one index's shards, found by walking for the index uuid directory.
     */
    private static Map<String, Long> extensionSizes(Path dataDir, String indexUuid) throws IOException {
        Map<String, Long> sizes = new TreeMap<>();
        List<Path> roots = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(dataDir)) {
            stream.filter(Files::isDirectory).filter(p -> p.getFileName().toString().equals(indexUuid)).forEach(roots::add);
        }
        if (roots.isEmpty()) {
            System.out.println("no directory named " + indexUuid + " under " + dataDir);
            return sizes;
        }
        for (Path root : roots) {
            try (Stream<Path> stream = Files.walk(root)) {
                for (Path file : stream.filter(Files::isRegularFile).toList()) {
                    String name = file.getFileName().toString();
                    int dot = name.lastIndexOf('.');
                    String extension = dot < 0 ? "(none)" : name.substring(dot + 1);
                    sizes.merge(extension, Files.size(file), Long::sum);
                }
            }
        }
        return sizes;
    }

    // --------------------------------------------------------------- query

    /**
     * Builds a {@code @timestamp} range clause covering the requested percentage of the corpus. The generator stamps
     * document i at BASE + i milliseconds, so a prefix of the time range is a prefix of the corpus.
     */
    private static String rangeClause(Map<String, String> options, int selectivityPercent) {
        if (selectivityPercent >= 100) {
            return null;
        }
        CorpusConfig config = corpus(options);
        long base = 1_755_720_000_000L;
        long upper = base + (long) (config.docCount() * (selectivityPercent / 100.0));
        return "{\"range\":{\"@timestamp\":{\"lt\":" + upper + "}}}";
    }

    /**
     * Builds the {@code query} clause.
     *
     * <p>Deliberately a query rather than a {@code post_filter}: a post filter runs <em>after</em> aggregations, so it
     * would leave every aggregation scanning the whole corpus while appearing to scope it.
     */
    private static String queryClause(String termClause, String rangeClause) {
        if (termClause == null && rangeClause == null) {
            return "";
        }
        if (termClause == null) {
            return ",\"query\":" + rangeClause;
        }
        if (rangeClause == null) {
            return ",\"query\":" + termClause;
        }
        return ",\"query\":{\"bool\":{\"filter\":[" + termClause + "," + rangeClause + "]}}";
    }

    private static void query(BenchHttp http, Map<String, String> options) throws Exception {
        String index = required(options, "index");
        CorpusConfig config = corpus(options);
        List<BenchQuery> queries = BenchQuery.forCorpus(config, BenchProbes.forConfig(config));
        int iters = intOption(options, "iters", 10);

        List<BenchQuery> selected = new ArrayList<>();
        String only = options.get("q");
        if (only == null) {
            selected.addAll(queries);
        } else {
            for (String id : only.split(",")) {
                selected.add(BenchQuery.byId(queries, id.trim()));
            }
        }

        for (BenchQuery query : selected) {
            runQuery(http, index, query, iters, options);
        }
    }

    /**
     * Times one query. Percentiles reported are p50 and p90 only: with the iteration counts a multi-second aggregation
     * allows, a p99 would be a single sample masquerading as a percentile.
     */
    private static void runQuery(BenchHttp http, String index, BenchQuery query, int iters, Map<String, String> options) throws Exception {
        List<Long> clientLatencies = new ArrayList<>();
        List<Long> serverTook = new ArrayList<>();

        // Untimed warmup, so painless compilation and first-touch page faults do not land in the measurement.
        int warmup = Math.max(2, iters / 5);
        String firstResponse = null;
        for (int i = 0; i < warmup + iters; i++) {
            long start = System.nanoTime();
            // request_cache=false is essential, not defensive. OpenSearch caches size:0 aggregation results per shard, so
            // repeating an identical aggregation would measure the cache: once observed as 16.8s first call, 1ms after.
            String response = http.post("/" + index + "/_search?request_cache=false", query.body);
            long elapsed = System.nanoTime() - start;
            if (i == 0) {
                firstResponse = response;
            }
            if (i >= warmup) {
                clientLatencies.add(elapsed);
                serverTook.add(BenchHttp.extractLong(response, "took"));
            }
        }

        java.util.Collections.sort(clientLatencies);
        java.util.Collections.sort(serverTook);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("op", "query");
        result.put("q", query.id);
        result.put("name", query.name);
        result.put("index", index);
        result.put("docs_scanned", query.docsScanned);
        result.put("iters", iters);
        result.put("took_p50_ms", percentile(serverTook, 50));
        result.put("took_p90_ms", percentile(serverTook, 90));
        result.put("client_p50_ms", round(percentile(clientLatencies, 50) / 1e6));
        result.put("client_p90_ms", round(percentile(clientLatencies, 90) / 1e6));
        if (options.containsKey("verbose") && firstResponse != null) {
            System.out.println("  " + query.id + " first response: " + firstResponse.substring(0, Math.min(300, firstResponse.length())));
        }
        report(result, options);
    }

    /** Prints the hardened query set without contacting a cluster. */
    private static void listQueries(Map<String, String> options) {
        CorpusConfig config = corpus(options);
        BenchProbes probes = BenchProbes.forConfig(config);
        System.out.println("probes: " + probes.describe());
        System.out.println();
        for (BenchQuery query : BenchQuery.forCorpus(config, probes)) {
            System.out.printf(Locale.ROOT, "%-4s %s%n", query.id, query.name);
            System.out.printf(Locale.ROOT, "     purpose      : %s%n", query.purpose);
            System.out.printf(Locale.ROOT, "     docs scanned : %,d%n", query.docsScanned);
            System.out.printf(Locale.ROOT, "     body         : %s%n%n", query.body);
        }
    }

    // --------------------------------------------------------------- utils

    private static CorpusConfig corpus(Map<String, String> options) {
        CorpusConfig config = CorpusConfig.preset(required(options, "preset"));
        String docs = options.get("docs");
        if (docs != null) {
            // Every field must be carried over. An earlier version rebuilt only the size knobs and silently dropped
            // `padded` and the sparse key settings, so `--docs` quietly changed the corpus shape rather than its length.
            CorpusConfig.Builder builder = CorpusConfig.builder(config.name())
                .seed(config.seed())
                .docCount(Integer.parseInt(docs))
                .targetDocBytes(config.targetDocBytes())
                .attrKeys(config.attrKeys())
                .maxDepth(config.maxDepth())
                .padded(config.padded())
                .sparseKeyPoolSize(config.sparseKeyPoolSize())
                .sparseKeysPerDoc(config.minSparseKeysPerDoc(), config.maxSparseKeysPerDoc())
                .zipfExponent(config.zipfExponent())
                .shapeCount(config.shapeCount())
                .privateKeysPerShape(config.privateKeysPerShape())
                .typeMix(config.typeMix());
            if (config.attrTargetBytes() > 0) {
                builder.attrTargetBytes(config.attrTargetBytes());
            } else {
                builder.attrFraction(config.attrFraction());
            }
            config = builder.build();
        }
        return config;
    }

    private static Map<String, String> parseOptions(String[] args, int from) {
        Map<String, String> options = new HashMap<>();
        for (int i = from; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--") == false) {
                throw new IllegalArgumentException("expected an --option but found [" + arg + "]");
            }
            String key = arg.substring(2);
            if (i + 1 < args.length && args[i + 1].startsWith("--") == false) {
                options.put(key, args[++i]);
            } else {
                options.put(key, "true");
            }
        }
        return options;
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null) {
            throw new IllegalArgumentException("missing required option --" + key);
        }
        return value;
    }

    private static int intOption(Map<String, String> options, String key, int defaultValue) {
        String value = options.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static long percentile(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) {
            return -1;
        }
        int index = (int) Math.min(sorted.size() - 1L, Math.round((percentile / 100.0) * (sorted.size() - 1)));
        return sorted.get(index);
    }

    private static double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    /**
     * Prints the result and, when {@code --results} is given, appends it as one JSON line. Results are accumulated in a
     * file rather than transcribed from console output so the write-up can be generated from data.
     */
    private static void report(Map<String, Object> result, Map<String, String> options) throws IOException {
        String json = toJson(result);
        System.out.println(json);
        String resultsFile = options.get("results");
        if (resultsFile != null) {
            Path path = Path.of(resultsFile);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Files.writeString(
                path,
                json + System.lineSeparator(),
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND
            );
        }
    }

    private static String toJson(Object value) {
        if (value instanceof Map<?, ?> map) {
            StringBuilder json = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (first == false) {
                    json.append(',');
                }
                first = false;
                json.append('"').append(entry.getKey()).append("\":").append(toJson(entry.getValue()));
            }
            return json.append('}').toString();
        }
        if (value instanceof String text) {
            return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
        }
        return String.valueOf(value);
    }
}
