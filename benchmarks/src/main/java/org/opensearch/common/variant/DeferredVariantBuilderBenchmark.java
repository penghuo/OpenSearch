/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

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
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Compares the previous relabel/re-encode writer with deferred final encoding.
 */
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class DeferredVariantBuilderBenchmark {

    @Param({ "10", "257" })
    private int keyCount;

    private List<String> insertionOrder;

    @Setup
    public void setup() {
        insertionOrder = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            insertionOrder.add(String.format(Locale.ROOT, "key_%03d", i));
        }
        Collections.reverse(insertionOrder);

        byte[] current = currentHybrid();
        byte[] deferred = deferred();
        if (current.length != deferred.length) {
            throw new IllegalStateException("value length changed from " + current.length + " to " + deferred.length);
        }
        if (decode(current).toJavaObject().equals(decode(deferred).toJavaObject()) == false) {
            throw new IllegalStateException("deferred encoding changed the value");
        }
    }

    @Benchmark
    public byte[] currentHybrid() {
        VariantBuilder builder = new VariantBuilder();
        append(builder);
        Variant variant = builder.finish();

        List<String> keys = builder.dictionaryKeys();
        byte[][] keyBytes = new byte[keyCount][];
        Integer[] byName = new Integer[keyCount];
        for (int i = 0; i < keyCount; i++) {
            keyBytes[i] = keys.get(i).getBytes(StandardCharsets.UTF_8);
            byName[i] = i;
        }
        Arrays.sort(byName, (left, right) -> Arrays.compareUnsigned(keyBytes[left], keyBytes[right]));

        if (keyCount <= 256) {
            int[] idMap = new int[keyCount];
            for (int rank = 0; rank < keyCount; rank++) {
                idMap[byName[rank]] = rank;
            }
            variant.relabelFieldIds(idMap);
            return variant.valueBytes();
        }

        List<String> sorted = new ArrayList<>(keyCount);
        for (int rank = 0; rank < keyCount; rank++) {
            sorted.add(keys.get(byName[rank]));
        }
        return variant.reencodeWithDictionary(sorted);
    }

    @Benchmark
    public byte[] deferred() {
        DeferredVariantBuilder builder = new DeferredVariantBuilder();
        append(builder);
        return builder.finish().valueBytes();
    }

    private void append(VariantBuilder builder) {
        builder.startObject();
        for (int i = 0; i < keyCount; i++) {
            builder.appendKey(insertionOrder.get(i));
            builder.appendLong(i);
        }
        builder.endObject();
    }

    private void append(DeferredVariantBuilder builder) {
        builder.startObject();
        for (int i = 0; i < keyCount; i++) {
            builder.appendKey(insertionOrder.get(i));
            builder.appendLong(i);
        }
        builder.endObject();
    }

    private Variant decode(byte[] valueBytes) {
        List<String> names = new ArrayList<>(insertionOrder);
        Collections.sort(names);
        byte[][] nameBytes = new byte[names.size()][];
        int[] ordinals = new int[names.size()];
        for (int i = 0; i < names.size(); i++) {
            nameBytes[i] = names.get(i).getBytes(StandardCharsets.UTF_8);
            ordinals[i] = i;
        }
        return new Variant(new VariantMetadata(nameBytes, ordinals, names.size()), valueBytes, 0);
    }
}
