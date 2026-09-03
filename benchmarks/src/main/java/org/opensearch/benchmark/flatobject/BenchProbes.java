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

import java.util.Locale;

/**
 * The attribute paths the benchmark queries, chosen from the corpus rather than hard-coded.
 *
 * <p>With a sparse key space, <em>which</em> path a query names decides what is being measured. A path present in every
 * document measures the cost of reading a value; a path present in a few per cent measures the cost of discovering a value
 * is <b>absent</b> — and the two stores differ most there, because {@code _source} must be parsed in full before absence
 * is known while a key dictionary can be searched and missed.
 *
 * <p>Selectivity is measured over a document sample rather than derived from the Zipf weight, because a document makes
 * several draws and duplicates collapse, so the closed form would overstate presence.
 */
final class BenchProbes {

    /** Always present; the baseline "read a value" path. */
    final String dense;
    /** Always present, low cardinality; the group-by key. */
    final String groupBy;
    /** Present in a few per cent of documents. */
    final String sparse;
    /** Present in well under one per cent. */
    final String rare;

    final double densePresence;
    final double sparsePresence;
    final double rarePresence;

    private BenchProbes(String dense, String groupBy, String sparse, String rare, double d, double s, double r) {
        this.dense = dense;
        this.groupBy = groupBy;
        this.sparse = sparse;
        this.rare = rare;
        this.densePresence = d;
        this.sparsePresence = s;
        this.rarePresence = r;
    }

    static BenchProbes forConfig(CorpusConfig config) {
        OtelDocGenerator generator = new OtelDocGenerator(config);
        int sample = 20_000;
        if (generator.sparseKeyPoolSize() == 0) {
            return new BenchProbes(OtelDocGenerator.KEY_STATUS, OtelDocGenerator.KEY_K8S_NAMESPACE, null, null, 1.0, 0, 0);
        }
        // Numeric so that a sum aggregation over them is meaningful.
        String sparse = generator.keyNearRank(20, CorpusConfig.ValueKind.LONG);
        String rare = generator.keyNearRank(300, CorpusConfig.ValueKind.LONG);
        return new BenchProbes(
            OtelDocGenerator.KEY_STATUS,
            OtelDocGenerator.KEY_K8S_NAMESPACE,
            sparse,
            rare,
            1.0,
            generator.measuredPresence(sparse, sample),
            generator.measuredPresence(rare, sample)
        );
    }

    boolean hasSparse() {
        return sparse != null;
    }

    String describe() {
        if (hasSparse() == false) {
            return String.format(Locale.ROOT, "dense=%s (100%%), groupBy=%s", dense, groupBy);
        }
        return String.format(
            Locale.ROOT,
            "dense=%s (100%%), groupBy=%s, sparse=%s (%.2f%%), rare=%s (%.3f%%)",
            dense,
            groupBy,
            sparse,
            sparsePresence * 100,
            rare,
            rarePresence * 100
        );
    }
}
