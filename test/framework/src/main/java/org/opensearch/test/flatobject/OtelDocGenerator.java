/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.flatobject;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.flatobject.CorpusConfig.ValueKind;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;

/**
 * Generates a deterministic synthetic OTel log corpus of {@code flat_object} documents.
 *
 * <p>Documents have the shape:
 *
 * <pre>
 * {"@timestamp": &lt;epoch millis&gt;, "severity": "INFO", "body": "&lt;filler&gt;", "attributes": { ... }}
 * </pre>
 *
 * <p>Two properties are load-bearing for the harness:
 *
 * <ol>
 *   <li><b>Random access.</b> {@link #document(int)} is a pure function of the configured seed and the document index,
 *       so document <i>i</i> can be produced without replaying the stream. A caller reading documents out of order still
 *       knows what it should have got back.
 *   <li><b>Byte-identical reproduction.</b> The same {@link CorpusConfig} always yields the same bytes, so two indices
 *       built from it hold provably identical input and any difference between them is not corpus drift.
 * </ol>
 *
 * <p>Filler text is drawn from a lowercase-alphanumeric alphabet only. That is not cosmetic: since such characters need
 * no JSON escaping, appending <i>n</i> characters grows the serialized document by exactly <i>n</i> bytes, which lets the
 * generator hit its size targets by direct arithmetic instead of an iterative search.
 *
 * <p>This class deliberately produces a <em>well-typed</em> corpus: each attribute path holds one type across the whole
 * corpus. Mixed-type paths and the other type-fidelity edge cases are covered by explicit fixtures in the correctness
 * tests, because seeding them into the performance corpus would make aggregations partially fail and muddy the latency
 * measurements.
 */
public final class OtelDocGenerator implements Iterable<byte[]> {

    public static final String ATTRIBUTES_FIELD = "attributes";

    public static final String KEY_STATUS = "status";
    public static final String KEY_DURATION_NS = "duration_ns";
    public static final String KEY_LEVEL = "level";
    /** Deliberately a literal dotted key, to exercise the path-resolution ambiguity rule. */
    public static final String KEY_K8S_NAMESPACE = "k8s.namespace";

    /** Keys present in every document, so a caller can rely on them being there. */
    static final String[] STABLE_KEYS = { KEY_STATUS, KEY_DURATION_NS, KEY_LEVEL, KEY_K8S_NAMESPACE };

    /** The filler key that absorbs padding so the corpus hits its attributes-size target. */
    static final String PADDING_KEY = "payload";

    private static final long[] STATUS_VALUES = { 200L, 200L, 200L, 200L, 201L, 204L, 400L, 404L, 429L, 500L, 503L };
    private static final String[] LEVEL_VALUES = { "info", "info", "info", "warn", "error", "debug" };
    private static final int NAMESPACE_CARDINALITY = 16;
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
    private static final long BASE_TIMESTAMP_MILLIS = 1_755_720_000_000L;

    /**
     * Real OpenTelemetry semantic-convention attribute names, ordered roughly by how often they occur in practice. These
     * occupy the head of the key space, so the frequently-present keys have realistic names and lengths rather than
     * synthetic ones.
     */
    private static final String[] OTEL_CONVENTION_KEYS = {
        "http.request.method",
        "http.response.status_code",
        "http.route",
        "url.path",
        "url.scheme",
        "server.address",
        "server.port",
        "client.address",
        "user_agent.original",
        "network.protocol.version",
        "service.name",
        "service.version",
        "service.instance.id",
        "deployment.environment",
        "telemetry.sdk.language",
        "telemetry.sdk.version",
        "host.name",
        "host.arch",
        "os.type",
        "process.pid",
        "process.runtime.name",
        "container.id",
        "container.image.name",
        "k8s.pod.name",
        "k8s.node.name",
        "k8s.deployment.name",
        "k8s.container.name",
        "cloud.region",
        "cloud.availability_zone",
        "cloud.account.id",
        "cloud.provider",
        "db.system",
        "db.namespace",
        "db.operation.name",
        "db.collection.name",
        "db.query.text",
        "messaging.system",
        "messaging.destination.name",
        "messaging.operation",
        "rpc.system",
        "rpc.service",
        "rpc.method",
        "exception.type",
        "exception.message",
        "exception.stacktrace",
        "aws.request_id",
        "aws.lambda.invoked_arn",
        "aws.s3.bucket",
        "aws.dynamodb.table_names",
        "faas.invocation_id",
        "faas.trigger",
        "enduser.id",
        "enduser.role",
        "session.id",
        "thread.name",
        "thread.id",
        "code.function",
        "code.namespace",
        "code.filepath",
        "code.lineno" };

    /** Namespaces for the generated tail of the key space, so it reads like several services each with its own prefix. */
    private static final String[] GENERATED_PREFIXES = {
        "app.checkout",
        "app.catalog",
        "app.payments",
        "app.search",
        "app.identity",
        "infra.cache",
        "infra.queue",
        "infra.gateway",
        "custom.tenant",
        "custom.experiment" };

    private final CorpusConfig config;
    private final ValueKind[] kindTable;
    /** The global attribute-key space, ordered by descending frequency, or empty when sparse keys are disabled. */
    private final String[] sparseKeys;
    /** Cumulative Zipf weights over {@link #sparseKeys}, for inverse-CDF sampling. */
    private final double[] cumulativeWeights;
    /**
     * Fixed key sets the corpus draws from, one per simulated service, or empty when keys are sampled per document.
     *
     * <p>This is the difference between the normal-test-set and the super-test-set. Sampling keys independently per
     * document makes the number of possible key sets combinatorial — C(1000,15) is about 7e32 — so no two documents
     * ever share one and metadata deduplication is impossible. Real telemetry emits a fixed key set per service, so a
     * corpus of N services has N key sets, each recurring many times.
     */
    private final String[][] shapes;
    /** Cumulative Zipf weights over {@link #shapes}: a few services produce most of the traffic. */
    private final double[] shapeWeights;

    public OtelDocGenerator(CorpusConfig config) {
        this.config = config;
        this.kindTable = buildKindTable(config.typeMix());
        this.sparseKeys = buildSparseKeyPool(config.sparseKeyPoolSize());
        this.cumulativeWeights = buildZipfWeights(sparseKeys.length, config.zipfExponent());
        this.shapes = buildShapes(config);
        this.shapeWeights = buildZipfWeights(shapes.length, config.zipfExponent());
    }

    /**
     * Builds the corpus's shapes, each a fixed set of sparse keys drawn from the pool by the same Zipf distribution a
     * per-document draw would use.
     *
     * <p>Deterministic in the seed, so a shape's key set is a property of the corpus rather than of the run. Shapes vary
     * in size for the same reason documents did: records do not all carry the same number of attributes.
     */
    private String[][] buildShapes(CorpusConfig config) {
        int count = config.shapeCount();
        if (count <= 0 || sparseKeys.length == 0) {
            return new String[0][];
        }
        String[][] built = new String[count][];
        for (int shape = 0; shape < count; shape++) {
            SplittableRandom random = new SplittableRandom(mix(config.seed(), shape, 0x5AAE));
            int size = config.minSparseKeysPerDoc() + random.nextInt(
                Math.max(1, config.maxSparseKeysPerDoc() - config.minSparseKeysPerDoc() + 1)
            );
            LinkedHashMap<String, Boolean> keys = new LinkedHashMap<>();
            for (int i = 0; i < size; i++) {
                keys.put(sparseKeys[sampleRank(random)], Boolean.TRUE);
            }
            // Names this shape alone emits. Derived from the shape index rather than from its random stream, so they are
            // unique across shapes by construction -- which is the point: they are what makes the field's vocabulary grow
            // with the number of services instead of staying pinned to the shared pool's size.
            for (int i = 0; i < config.privateKeysPerShape(); i++) {
                keys.put("svc" + shape + ".attr_" + i, Boolean.TRUE);
            }
            built[shape] = keys.keySet().toArray(new String[0]);
        }
        return built;
    }

    /**
     * Builds the attribute-key space: realistic OpenTelemetry semantic-convention names first, then generated
     * {@code service.custom.attr_N} names to reach the configured pool size.
     *
     * <p>Order matters. Keys are sampled by Zipf rank, so the names at the front of this array are the ones that appear in
     * most documents, and the tail appears in very few — which is the point of the pool.
     */
    private static String[] buildSparseKeyPool(int size) {
        if (size <= 0) {
            return new String[0];
        }
        List<String> keys = new ArrayList<>(size);
        for (String name : OTEL_CONVENTION_KEYS) {
            if (keys.size() < size) {
                keys.add(name);
            }
        }
        int generated = 0;
        while (keys.size() < size) {
            // Grouped under a handful of prefixes so the key space looks like several services each with its own
            // namespace, rather than one flat list of unrelated names.
            String prefix = GENERATED_PREFIXES[generated % GENERATED_PREFIXES.length];
            keys.add(prefix + ".attr_" + generated);
            generated++;
        }
        return keys.toArray(new String[0]);
    }

    /**
     * Cumulative weights for a Zipf distribution over {@code n} ranks: weight of rank r is proportional to
     * 1/(r+1)^exponent.
     */
    private static double[] buildZipfWeights(int n, double exponent) {
        double[] cumulative = new double[n];
        double running = 0;
        for (int i = 0; i < n; i++) {
            running += 1.0 / Math.pow(i + 1, exponent);
            cumulative[i] = running;
        }
        for (int i = 0; i < n; i++) {
            cumulative[i] /= running;
        }
        return cumulative;
    }

    /**
     * The value type a key always holds, derived from the key name so it is stable across the whole corpus.
     *
     * <p>Without this, a randomly-chosen type per (document, key) draw would make the same path a number in one document
     * and a string in the next. That is neither realistic — a given attribute has a consistent type in practice — nor
     * usable for measurement, because every aggregation would then be partly composed of coercion failures. Mixed-type
     * paths are still exercised, but by explicit fixtures in the correctness tests where they can be reasoned about.
     */
    private ValueKind kindForKey(String key) {
        int hash = key.hashCode();
        return kindTable[Math.floorMod(hash, kindTable.length)];
    }

    /** Draws a shape by Zipf rank, so a few services account for most documents. */
    private int sampleShape(SplittableRandom random) {
        return sampleFrom(shapeWeights, random.nextDouble());
    }

    /** Draws a key rank from the Zipf distribution by inverse-CDF binary search. */
    private int sampleRank(SplittableRandom random) {
        return sampleFrom(cumulativeWeights, random.nextDouble());
    }

    private static int sampleFrom(double[] cumulative, double u) {
        int low = 0;
        int high = cumulative.length - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (cumulative[mid] < u) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** The number of fixed shapes this corpus draws from, {@code 0} when keys are sampled per document. */
    public int shapeCount() {
        return shapes.length;
    }

    /**
     * The key at a given frequency rank, 0 being the most common. Benchmarks use this to pick probe paths of known
     * selectivity rather than guessing at names.
     */
    public String sparseKeyAtRank(int rank) {
        if (rank < 0 || rank >= sparseKeys.length) {
            throw new IllegalArgumentException("rank " + rank + " outside pool of " + sparseKeys.length);
        }
        return sparseKeys[rank];
    }

    public int sparseKeyPoolSize() {
        return sparseKeys.length;
    }

    /** The stable value type of a key, exposed so a caller can build a matching derived field. */
    public ValueKind kindOf(String key) {
        return kindForKey(key);
    }

    /**
     * The first key at or after {@code fromRank} whose stable type is {@code kind}.
     *
     * <p>Probe paths for a numeric aggregation have to actually hold numbers, and which key holds which type is decided by
     * the key name, so a caller asks for one rather than assuming.
     */
    public String keyNearRank(int fromRank, ValueKind kind) {
        for (int rank = fromRank; rank < sparseKeys.length; rank++) {
            if (kindForKey(sparseKeys[rank]) == kind) {
                return sparseKeys[rank];
            }
        }
        throw new IllegalStateException("no " + kind + " key at or after rank " + fromRank);
    }

    /**
     * Measures the fraction of the first {@code sample} documents that carry {@code key}.
     *
     * <p>Selectivity is measured rather than derived: the Zipf weight gives the probability of a single draw, but a
     * document makes several draws and duplicates collapse, so the closed form would overstate presence.
     */
    public double measuredPresence(String key, int sample) {
        int hits = 0;
        for (int i = 0; i < sample; i++) {
            if (attributesAsMap(i).containsKey(key)) {
                hits++;
            }
        }
        return (double) hits / sample;
    }

    public CorpusConfig config() {
        return config;
    }

    /**
     * Returns the JSON bytes of document {@code docIndex}. Pure function of the seed and index.
     */
    public byte[] document(int docIndex) {
        return serialize(documentAsMap(docIndex));
    }

    /**
     * Returns document {@code docIndex} as a map, for tests that need the expected values without re-parsing JSON.
     */
    public Map<String, Object> documentAsMap(int docIndex) {
        Map<String, Object> attributes = attributesAsMap(docIndex);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("@timestamp", BASE_TIMESTAMP_MILLIS + docIndex);
        doc.put("severity", LEVEL_VALUES[docIndex % LEVEL_VALUES.length].toUpperCase(Locale.ROOT));
        if (config.padded() == false) {
            // No filler: every field is one a query could touch, so document size is whatever the attributes occupy.
            doc.put(ATTRIBUTES_FIELD, attributes);
            return doc;
        }
        doc.put("body", "");
        doc.put(ATTRIBUTES_FIELD, attributes);

        // With an escape-free alphabet, n extra characters cost exactly n extra bytes, so the body length that hits the
        // document-size target can be computed in one shot.
        int withEmptyBody = serialize(doc).length;
        int bodyLength = Math.max(0, config.targetDocBytes() - withEmptyBody);
        doc.put("body", filler(new SplittableRandom(mix(config.seed(), docIndex, 0x60D1)), bodyLength));
        return doc;
    }

    /**
     * Returns the {@code attributes} value of document {@code docIndex} as a map.
     */
    public Map<String, Object> attributesAsMap(int docIndex) {
        SplittableRandom random = new SplittableRandom(mix(config.seed(), docIndex, 0xA771));

        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(KEY_STATUS, STATUS_VALUES[random.nextInt(STATUS_VALUES.length)]);
        attributes.put(KEY_DURATION_NS, 1_000L + random.nextLong(5_000_000_000L));
        attributes.put(KEY_LEVEL, LEVEL_VALUES[random.nextInt(LEVEL_VALUES.length)]);
        attributes.put(KEY_K8S_NAMESPACE, "ns-" + String.format(Locale.ROOT, "%02d", random.nextInt(NAMESPACE_CARDINALITY)));

        if (shapes.length > 0) {
            // Normal-test-set: the document belongs to one service, so it carries exactly that service's key set. Key
            // sets therefore recur, which is what makes metadata deduplication possible.
            String[] shape = shapes[sampleShape(random)];
            for (String key : shape) {
                if (attributes.containsKey(key) == false) {
                    attributes.put(key, randomValue(random, kindForKey(key), 1));
                }
            }
        } else if (sparseKeys.length > 0) {
            // Super-test-set: keys drawn independently per document. Duplicate draws collapse, so a document ends up
            // with at most the requested number of distinct sparse keys. Realistic in count, but not in composition --
            // it makes every document's key set unique, which no real corpus does.
            int draws = config.minSparseKeysPerDoc() + random.nextInt(
                Math.max(1, config.maxSparseKeysPerDoc() - config.minSparseKeysPerDoc() + 1)
            );
            for (int i = 0; i < draws; i++) {
                String key = sparseKeys[sampleRank(random)];
                if (attributes.containsKey(key) == false) {
                    attributes.put(key, randomValue(random, kindForKey(key), 1));
                }
            }
        }

        // Fixed filler keys, only when the corpus is padded to a size target.
        if (config.padded()) {
            int fillerKeys = config.attrKeys() - STABLE_KEYS.length - 1;
            for (int i = 0; i < fillerKeys; i++) {
                attributes.put("attr_" + i, randomValue(random, kindTable[random.nextInt(kindTable.length)], 1));
            }
            attributes.put(PADDING_KEY, "");
            int withEmptyPadding = serialize(attributes).length;
            int paddingLength = Math.max(0, config.resolvedAttrBytes() - withEmptyPadding);
            attributes.put(PADDING_KEY, filler(random, paddingLength));
        }
        return attributes;
    }

    /**
     * Returns the serialized byte length of the {@code attributes} value of document {@code docIndex}. Used to verify the
     * attributes-size target and to predict the blob column's size offline.
     */
    public int attributesBytes(int docIndex) {
        return serialize(attributesAsMap(docIndex)).length;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new Iterator<>() {
            private int next = 0;

            @Override
            public boolean hasNext() {
                return next < config.docCount();
            }

            @Override
            public byte[] next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return document(next++);
            }
        };
    }

    private Object randomValue(SplittableRandom random, ValueKind kind, int depth) {
        switch (kind) {
            case LONG:
                return random.nextLong(-1_000_000L, 1_000_000L);
            case DOUBLE:
                // Two decimal places keeps the serialized form short and predictable.
                return Math.round(random.nextDouble(-10_000.0, 10_000.0) * 100.0) / 100.0;
            case STRING:
                return filler(random, 4 + random.nextInt(12));
            case BOOLEAN:
                return random.nextBoolean();
            case NULL:
                return null;
            case ARRAY: {
                int length = 2 + random.nextInt(3);
                List<Object> values = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    values.add(randomValue(random, scalarKind(random), depth));
                }
                return values;
            }
            case OBJECT: {
                if (depth >= config.maxDepth()) {
                    return randomValue(random, scalarKind(random), depth);
                }
                int size = 2 + random.nextInt(2);
                Map<String, Object> nested = new LinkedHashMap<>();
                for (int i = 0; i < size; i++) {
                    nested.put("f" + i, randomValue(random, kindTable[random.nextInt(kindTable.length)], depth + 1));
                }
                return nested;
            }
            default:
                throw new IllegalStateException("unhandled value kind [" + kind + "]");
        }
    }

    private static ValueKind scalarKind(SplittableRandom random) {
        ValueKind[] scalars = { ValueKind.LONG, ValueKind.DOUBLE, ValueKind.STRING, ValueKind.BOOLEAN };
        return scalars[random.nextInt(scalars.length)];
    }

    private static String filler(SplittableRandom random, int length) {
        if (length <= 0) {
            return "";
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = ALPHABET[random.nextInt(ALPHABET.length)];
        }
        return new String(chars);
    }

    /**
     * Expands the weighted type mix into a flat lookup table so a kind can be drawn with one bounded random call.
     */
    private static ValueKind[] buildKindTable(Map<ValueKind, Integer> typeMix) {
        List<ValueKind> table = new ArrayList<>();
        for (Map.Entry<ValueKind, Integer> entry : typeMix.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                table.add(entry.getKey());
            }
        }
        return table.toArray(new ValueKind[0]);
    }

    /**
     * Derives an independent, well-distributed seed for (corpus seed, document index, purpose). Multiplying by the
     * golden-ratio constant decorrelates adjacent document indices, which a plain {@code seed + index} would not.
     */
    private static long mix(long seed, int docIndex, int purpose) {
        long z = seed ^ ((docIndex + 1L) * 0x9E3779B97F4A7C15L) ^ ((long) purpose << 32);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private static byte[] serialize(Map<String, Object> map) {
        try {
            // BytesReference.bytes() closes the builder, so try-with-resources would double-close it.
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(map);
            return BytesReference.toBytes(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize generated document", e);
        }
    }

    /**
     * The keys present in every generated document.
     */
    public static List<String> stableKeys() {
        return Arrays.asList(STABLE_KEYS);
    }
}
