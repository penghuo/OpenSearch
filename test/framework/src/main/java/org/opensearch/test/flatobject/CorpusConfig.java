/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.flatobject;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for {@link OtelDocGenerator}, describing the shape of a synthetic OTel log corpus.
 *
 * <p>The knobs are document size, the size of the {@code attributes} value, keys per document, type mix, and nesting
 * depth.
 *
 * <p>The size of {@code attributes} can be expressed two ways, and the distinction matters. {@link #attrFraction()}
 * scales attributes <em>with</em> the document, while {@link #attrTargetBytes()} pins attributes to an absolute size so
 * document size can be swept independently. Holding attributes fixed while growing the document is what separates a
 * reader whose cost tracks the attributes from one whose cost tracks the whole document.
 */
public final class CorpusConfig {

    /**
     * The kinds of value that may appear at an attribute leaf.
     */
    public enum ValueKind {
        LONG,
        DOUBLE,
        STRING,
        BOOLEAN,
        NULL,
        ARRAY,
        OBJECT
    }

    private final String name;
    private final long seed;
    private final int docCount;
    private final int targetDocBytes;
    private final double attrFraction;
    private final int attrTargetBytes;
    private final int attrKeys;
    private final int maxDepth;
    private final boolean padded;
    private final int sparseKeyPoolSize;
    private final int minSparseKeysPerDoc;
    private final int maxSparseKeysPerDoc;
    private final double zipfExponent;
    private final int shapeCount;
    private final int privateKeysPerShape;
    private final Map<ValueKind, Integer> typeMix;

    private CorpusConfig(Builder builder) {
        this.name = builder.name;
        this.seed = builder.seed;
        this.docCount = builder.docCount;
        this.targetDocBytes = builder.targetDocBytes;
        this.attrFraction = builder.attrFraction;
        this.attrTargetBytes = builder.attrTargetBytes;
        this.attrKeys = builder.attrKeys;
        this.maxDepth = builder.maxDepth;
        this.padded = builder.padded;
        this.sparseKeyPoolSize = builder.sparseKeyPoolSize;
        this.minSparseKeysPerDoc = builder.minSparseKeysPerDoc;
        this.maxSparseKeysPerDoc = builder.maxSparseKeysPerDoc;
        this.zipfExponent = builder.zipfExponent;
        this.shapeCount = builder.shapeCount;
        this.privateKeysPerShape = builder.privateKeysPerShape;
        this.typeMix = Collections.unmodifiableMap(new EnumMap<>(builder.typeMix));
    }

    public String name() {
        return name;
    }

    public long seed() {
        return seed;
    }

    public int docCount() {
        return docCount;
    }

    public int targetDocBytes() {
        return targetDocBytes;
    }

    public double attrFraction() {
        return attrFraction;
    }

    /**
     * Absolute target size in bytes for the serialized {@code attributes} value, or {@code -1} to derive it from
     * {@link #attrFraction()}.
     */
    public int attrTargetBytes() {
        return attrTargetBytes;
    }

    /**
     * The resolved target size in bytes for the serialized {@code attributes} value.
     */
    public int resolvedAttrBytes() {
        return attrTargetBytes > 0 ? attrTargetBytes : (int) Math.round(targetDocBytes * attrFraction);
    }

    public int attrKeys() {
        return attrKeys;
    }

    public int maxDepth() {
        return maxDepth;
    }

    /**
     * Whether documents carry filler purely to reach a size target: a {@code body} field and a {@code payload} attribute.
     *
     * <p>When false, every field in the document is one a query could plausibly touch, and the size knobs
     * ({@link #targetDocBytes()}, {@link #attrTargetBytes()}) are ignored — document size becomes whatever
     * {@link #attrKeys()} keys of realistic values happen to occupy.
     *
     * <p>The trade-off is worth being explicit about. Filler is artificial, but it is the only way to make
     * {@code attributes} a <em>small fraction of a large document</em>, which is the shape a per-field value store is
     * supposed to win on. With filler removed, {@code attributes} is most of the document and the measurement runs on the
     * least favourable shape for a separate value column.
     */
    public boolean padded() {
        return padded;
    }

    /**
     * Size of the global attribute-key space. Real observability data has thousands of distinct attribute keys while any
     * one record carries only a handful, so a corpus where every document has the same keys understates how often a
     * queried path is simply <em>absent</em>.
     *
     * <p>Zero disables sparse keys, leaving only the always-present core keys.
     */
    public int sparseKeyPoolSize() {
        return sparseKeyPoolSize;
    }

    public int minSparseKeysPerDoc() {
        return minSparseKeysPerDoc;
    }

    public int maxSparseKeysPerDoc() {
        return maxSparseKeysPerDoc;
    }

    /**
     * Exponent of the Zipf distribution used to pick which keys a document carries. 1.0 gives the familiar
     * "a few keys almost everywhere, a long tail almost nowhere" shape; 0 would make every key equally likely.
     */
    public double zipfExponent() {
        return zipfExponent;
    }

    /**
     * How many distinct attribute <em>shapes</em> the corpus draws from, or {@code 0} for none.
     *
     * <p>A shape is a fixed set of keys, standing in for one service or event type that always emits the same
     * attributes. This is the variable that governs whether key metadata can be deduplicated at all, and it is
     * combinatorial rather than gradual:
     *
     * <ul>
     *   <li>{@code 0} — keys are drawn independently per document, so the number of possible key sets is
     *       C(poolSize, keysPerDoc), astronomically larger than any corpus. Measured over 10M documents: 9,942,838
     *       distinct key sets, 99.4% unique. Deduplication saves nothing. Call this the <b>super-test-set</b>.
     *   <li>{@code 1000} — a thousand services each with a fixed key set, so ~1000 distinct key sets each recurring
     *       ~10,000 times in a 10M-document corpus. Deduplication is near-perfect. Call this the
     *       <b>normal-test-set</b>, and it is what real telemetry looks like.
     * </ul>
     *
     * <p>Every measurement taken before this knob existed used {@code 0}, which is why several storage conclusions are
     * expected to invert once it is set.
     */
    public int shapeCount() {
        return shapeCount;
    }

    /**
     * How many keys each shape owns exclusively, on top of the ones it draws from the shared pool.
     *
     * <p>This is what decides the field's <em>vocabulary</em>, as distinct from how many key <em>sets</em> exist. With
     * {@code 0} every shape draws from one shared pool, so a thousand shapes and ten thousand shapes both see a vocabulary
     * of {@code sparseKeyPoolSize}; that is what every measurement before this knob existed did, and it left vocabulary
     * pinned at ~1,000 while key-set count swept over six orders of magnitude.
     *
     * <p>Real telemetry is not like that. Services share the semantic conventions ({@code http.*}, {@code db.*}) and then
     * emit their own attributes, which nothing else emits. Setting this to {@code n} gives a vocabulary of
     * {@code sparseKeyPoolSize + shapeCount * n} — so 10,000 services with 76 private keys each is ~761,000 distinct
     * names, against ~1,000 before.
     *
     * <p>That matters because vocabulary is the one resource the shared-name column does not bound: the reader holds every
     * distinct name in memory per segment.
     */
    public int privateKeysPerShape() {
        return privateKeysPerShape;
    }

    public Map<ValueKind, Integer> typeMix() {
        return typeMix;
    }

    @Override
    public String toString() {
        return "CorpusConfig["
            + "name="
            + name
            + ", docs="
            + docCount
            + ", docBytes="
            + targetDocBytes
            + ", attrBytes="
            + resolvedAttrBytes()
            + ", attrKeys="
            + attrKeys
            + ", maxDepth="
            + maxDepth
            + ", padded="
            + padded
            + ", sparseKeyPool="
            + sparseKeyPoolSize
            + ", shapes="
            + (shapeCount == 0 ? "unique-per-doc" : String.valueOf(shapeCount))
            + ", sparseKeysPerDoc="
            + minSparseKeysPerDoc
            + "-"
            + maxSparseKeysPerDoc
            + ", privateKeysPerShape="
            + privateKeysPerShape
            + ", seed="
            + seed
            + ']';
    }

    /**
     * The default type mix: skewed toward the scalar types that dominate real OTel attributes, but with every kind
     * represented so correctness tests exercise arrays, nested objects, and nulls.
     */
    public static Map<ValueKind, Integer> defaultTypeMix() {
        EnumMap<ValueKind, Integer> mix = new EnumMap<>(ValueKind.class);
        mix.put(ValueKind.STRING, 40);
        mix.put(ValueKind.LONG, 25);
        mix.put(ValueKind.DOUBLE, 15);
        mix.put(ValueKind.BOOLEAN, 8);
        mix.put(ValueKind.NULL, 4);
        mix.put(ValueKind.ARRAY, 4);
        mix.put(ValueKind.OBJECT, 4);
        return mix;
    }

    /** With sparse keys supplying the rest, only the stable keys are fixed. */
    private static final int STABLE_PLUS_NONE = 4;

    /** Attribute-key count shared by every preset. See {@link #ATTR_BASELINE_BYTES} for why it is not swept. */
    private static final int PRESET_ATTR_KEYS = 8;

    /**
     * The baseline serialized size of {@code attributes} in bytes.
     *
     * <p>This is not an arbitrary round number. With 8 keys the generator's <em>unpaddable floor</em> — the size of a
     * document whose padding key is empty — was measured at min 133 / mean 152 / max 253 bytes, the spread coming from
     * filler values that happen to be arrays or nested objects. A target below the observed maximum cannot be hit,
     * because those documents already exceed it and padding can only add bytes. 288 clears the measured maximum with
     * headroom, so every document pads up to exactly the target and the corpus size is near-deterministic.
     */
    public static final int ATTR_BASELINE_BYTES = 288;

    /**
     * Resolves a named preset. See the implementation plan for the rationale behind each set of numbers.
     *
     * @throws IllegalArgumentException if the name is not a known preset
     */
    public static CorpusConfig preset(String presetName) {
        switch (presetName) {
            // Tiny corpus for validating the harness end to end without waiting.
            case "SMOKE":
                return sized("SMOKE", 10_000, 448, ATTR_BASELINE_BYTES);

            // Smallest end-to-end scale: ~1.1GB raw. Indexes and force-merges in a couple of minutes per index, so the
            // whole matrix -- storage, correctness, read latency, allocation, write throughput -- can be completed and
            // re-run cheaply. Per-document costs measured here held constant from 10k to 2M documents, so this scale
            // establishes the verdict and the larger ones confirm it.
            case "S1_1G":
                return sized("S1_1G", 2_500_000, 448, ATTR_BASELINE_BYTES);

            // End-to-end run with no filler at all: every field is one a query could touch. Document size is whatever 8
            // realistic attribute keys occupy (~200 B), so `attributes` is most of the document. Small enough that
            // full-corpus aggregations can be repeated often enough for a real P90/P99.
            case "E10M":
                return builder("E10M").docCount(10_000_000)
                    .attrKeys(STABLE_PLUS_NONE)
                    .padded(false)
                    .sparseKeyPoolSize(1000)
                    .sparseKeysPerDoc(4, 20)
                    .zipfExponent(1.0)
                    .build();

            // The normal-test-set: same key space and same keys per document as E10M, but drawn from 1000 fixed shapes
            // rather than sampled independently. One shape stands in for one service that always emits the same
            // attributes, which is what real telemetry looks like and the condition under which key metadata can be
            // deduplicated at all. Everything else is held identical to E10M so the two are directly comparable.
            case "E10M_NORMAL":
                return builder("E10M_NORMAL").docCount(10_000_000)
                    .attrKeys(STABLE_PLUS_NONE)
                    .padded(false)
                    .sparseKeyPoolSize(1000)
                    .sparseKeysPerDoc(4, 20)
                    .zipfExponent(1.0)
                    .shapeCount(1000)
                    .build();

            // Shape-count sweep, for locating the point where deduplication stops paying. Small corpora: the question is
            // how many distinct key sets result, which needs no scale to answer.
            case "SHAPES_100":
                return shaped("SHAPES_100", 100);
            case "SHAPES_1K":
                return shaped("SHAPES_1K", 1_000);
            case "SHAPES_10K":
                return shaped("SHAPES_10K", 10_000);
            case "SHAPES_100K":
                return shaped("SHAPES_100K", 100_000);
            case "SHAPES_UNIQUE":
                return shaped("SHAPES_UNIQUE", 0);

            // Vocabulary sweep: 10,000 services, ~100 attributes each, most of the names owned by one service.
            case "SVC_10K":
                return services("SVC_10K");

            // Baseline run: ~11GB raw, over the 10GB floor, and shaped like a real OTel log record (a few hundred bytes
            // of body, a few hundred of attributes). Sized so that indexing *and* the force merge both finish in
            // minutes rather than hours — a 100GB index takes ~50 minutes to load and well over an hour to merge to a
            // single segment, which buys confirmation rather than new information once the per-document costs are known
            // to be constant.
            case "S1_10G":
                return sized("S1_10G", 25_000_000, 448, ATTR_BASELINE_BYTES);

            // Extension run: ~45GB raw. Same shape at 4x the documents, for confirming the trend at scale once the
            // baseline verdict is in. Expect roughly an hour to index and longer to force merge, per index.
            case "S1":
                return sized("S1", 100_000_000, 448, ATTR_BASELINE_BYTES);

            // Document-size sweep: attributes pinned while the document grows via `body`.
            // Expected: the column's cost stays flat while the _source cost rises.
            case "S2_SIZE_512":
                return sized("S2_SIZE_512", 2_000_000, 512, ATTR_BASELINE_BYTES);
            case "S2_SIZE_2K":
                return sized("S2_SIZE_2K", 2_000_000, 2048, ATTR_BASELINE_BYTES);
            case "S2_SIZE_8K":
                return sized("S2_SIZE_8K", 2_000_000, 8192, ATTR_BASELINE_BYTES);

            // Attributes-size sweep: document pinned at 2048 bytes while attributes grow. S2_ATTR_288 is the shared
            // corner of the two sweeps and is identical to S2_SIZE_2K, so only one index needs building for it.
            // Expected: the column's cost rises while the _source cost stays flat-ish.
            case "S2_ATTR_288":
                return sized("S2_ATTR_288", 2_000_000, 2048, ATTR_BASELINE_BYTES);
            case "S2_ATTR_700":
                return sized("S2_ATTR_700", 2_000_000, 2048, 700);
            case "S2_ATTR_1500":
                return sized("S2_ATTR_1500", 2_000_000, 2048, 1500);

            default:
                throw new IllegalArgumentException(
                    "unknown corpus preset ["
                        + presetName
                        + "]; expected one of "
                        + "SMOKE, E10M, E10M_NORMAL, SHAPES_100..SHAPES_UNIQUE, S1_1G, S1_10G, S1, S2_SIZE_512, S2_SIZE_2K, S2_SIZE_8K, S2_ATTR_288, S2_ATTR_700, S2_ATTR_1500"
                );
        }
    }

    /**
     * 10,000 services, each emitting ~100 attributes, and each owning most of its attribute names.
     *
     * <p>The first preset that moves the field's <em>vocabulary</em> rather than its key-set count. Earlier presets swept
     * key-set count over six orders of magnitude while every one of them held vocabulary at ~1,000 names, because all
     * shapes drew from a single shared pool. Here each of the 10,000 shapes owns 76 names nothing else emits, which is how
     * services actually behave: they share the semantic conventions and then emit their own attributes.
     *
     * <pre>
     *   per document   4 stable keys      status, duration_ns, level, k8s.namespace -- the query probes land here
     *                + ~20 shared keys    drawn from the 1,000-name pool by Zipf rank
     *                + 76 private keys    owned by that document's service
     *                = ~100 attributes
     *
     *   vocabulary     1,000 + 10,000 x 76  =  ~761,000 distinct names
     * </pre>
     *
     * <p>One million documents rather than ten: at ~100 attributes each, ten million would be several hundred gigabytes per
     * index and hours of indexing per index.
     */
    private static CorpusConfig services(String name) {
        return builder(name).docCount(1_000_000)
            .attrKeys(STABLE_PLUS_NONE)
            .padded(false)
            .sparseKeyPoolSize(1000)
            .sparseKeysPerDoc(16, 24)
            .privateKeysPerShape(76)
            .zipfExponent(1.0)
            .shapeCount(10_000)
            .build();
    }

    /** A shape-sweep preset: E10M's shape, at a fixed shape count, small enough to measure key-set diversity quickly. */
    private static CorpusConfig shaped(String name, int shapeCount) {
        return builder(name).docCount(1_000_000)
            .attrKeys(STABLE_PLUS_NONE)
            .padded(false)
            .sparseKeyPoolSize(1000)
            .sparseKeysPerDoc(4, 20)
            .zipfExponent(1.0)
            .shapeCount(shapeCount)
            .build();
    }

    /** The shape-count sweep, in ascending order of key-set diversity. */
    public static List<String> shapeSweep() {
        return List.of("SHAPES_100", "SHAPES_1K", "SHAPES_10K", "SHAPES_100K", "SHAPES_UNIQUE");
    }

    private static CorpusConfig sized(String name, int docCount, int docBytes, int attrBytes) {
        return builder(name).docCount(docCount).targetDocBytes(docBytes).attrTargetBytes(attrBytes).attrKeys(PRESET_ATTR_KEYS).build();
    }

    /** The preset names that make up the document-size sweep, in ascending document size. */
    public static List<String> sizeSweep() {
        return List.of("S2_SIZE_512", "S2_SIZE_2K", "S2_SIZE_8K");
    }

    /** The preset names that make up the attributes-size sweep, in ascending attributes size. */
    public static List<String> attrSweep() {
        return List.of("S2_ATTR_288", "S2_ATTR_700", "S2_ATTR_1500");
    }

    /** Every preset. */
    public static List<String> allBenchmarkPresets() {
        return List.of("S1_10G", "S2_SIZE_512", "S2_SIZE_2K", "S2_SIZE_8K", "S2_ATTR_700", "S2_ATTR_1500");
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * Builder for {@link CorpusConfig}.
     */
    public static final class Builder {
        private final String name;
        private long seed = 42L;
        private int docCount = 10_000;
        private int targetDocBytes = 320;
        private double attrFraction = 0.5;
        private int attrTargetBytes = -1;
        private int attrKeys = 8;
        private int maxDepth = 3;
        private boolean padded = true;
        private int sparseKeyPoolSize = 0;
        private int minSparseKeysPerDoc = 4;
        private int maxSparseKeysPerDoc = 20;
        private double zipfExponent = 1.0;
        private int shapeCount = 0;
        private int privateKeysPerShape = 0;
        private Map<ValueKind, Integer> typeMix = defaultTypeMix();

        private Builder(String name) {
            this.name = name;
        }

        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder docCount(int docCount) {
            this.docCount = docCount;
            return this;
        }

        public Builder targetDocBytes(int targetDocBytes) {
            this.targetDocBytes = targetDocBytes;
            return this;
        }

        public Builder attrFraction(double attrFraction) {
            this.attrFraction = attrFraction;
            this.attrTargetBytes = -1;
            return this;
        }

        public Builder attrTargetBytes(int attrTargetBytes) {
            this.attrTargetBytes = attrTargetBytes;
            return this;
        }

        public Builder attrKeys(int attrKeys) {
            this.attrKeys = attrKeys;
            return this;
        }

        public Builder maxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder padded(boolean padded) {
            this.padded = padded;
            return this;
        }

        public Builder sparseKeyPoolSize(int sparseKeyPoolSize) {
            this.sparseKeyPoolSize = sparseKeyPoolSize;
            return this;
        }

        public Builder sparseKeysPerDoc(int min, int max) {
            this.minSparseKeysPerDoc = min;
            this.maxSparseKeysPerDoc = max;
            return this;
        }

        public Builder shapeCount(int shapeCount) {
            this.shapeCount = shapeCount;
            return this;
        }

        public Builder privateKeysPerShape(int privateKeysPerShape) {
            this.privateKeysPerShape = privateKeysPerShape;
            return this;
        }

        public Builder zipfExponent(double zipfExponent) {
            this.zipfExponent = zipfExponent;
            return this;
        }

        public Builder typeMix(Map<ValueKind, Integer> typeMix) {
            this.typeMix = new EnumMap<>(typeMix);
            return this;
        }

        public CorpusConfig build() {
            if (docCount <= 0) {
                throw new IllegalArgumentException("docCount must be positive but was [" + docCount + "]");
            }
            // The stable keys, plus one more reserved for size padding when padding is in use.
            int minimumKeys = OtelDocGenerator.STABLE_KEYS.length + (padded ? 1 : 0);
            if (attrKeys < minimumKeys) {
                throw new IllegalArgumentException(
                    "attrKeys must be at least " + minimumKeys + " to hold the stable keys and the padding key but was [" + attrKeys + "]"
                );
            }
            if (maxDepth < 1) {
                throw new IllegalArgumentException("maxDepth must be at least 1 but was [" + maxDepth + "]");
            }
            if (attrTargetBytes <= 0 && (attrFraction <= 0.0 || attrFraction > 1.0)) {
                throw new IllegalArgumentException("attrFraction must be in (0, 1] but was [" + attrFraction + "]");
            }
            if (typeMix.isEmpty() || typeMix.values().stream().anyMatch(w -> w < 0)) {
                throw new IllegalArgumentException("typeMix must be non-empty with non-negative weights");
            }
            if (typeMix.values().stream().mapToInt(Integer::intValue).sum() == 0) {
                throw new IllegalArgumentException("typeMix weights must not sum to zero");
            }
            return new CorpusConfig(this);
        }
    }
}
