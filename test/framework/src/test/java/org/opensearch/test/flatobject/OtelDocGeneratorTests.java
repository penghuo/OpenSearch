/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.flatobject;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OtelDocGeneratorTests extends OpenSearchTestCase {

    private static final int SAMPLE = 500;

    public void testDeterministicForSameSeed() {
        CorpusConfig config = CorpusConfig.preset("SMOKE");
        OtelDocGenerator first = new OtelDocGenerator(config);
        OtelDocGenerator second = new OtelDocGenerator(config);
        for (int docIndex : new int[] { 0, 1, 7, 42, 999 }) {
            assertArrayEquals("document " + docIndex + " must be reproducible", first.document(docIndex), second.document(docIndex));
        }
    }

    public void testDifferentSeedsProduceDifferentDocuments() {
        OtelDocGenerator a = new OtelDocGenerator(CorpusConfig.builder("a").seed(1L).build());
        OtelDocGenerator b = new OtelDocGenerator(CorpusConfig.builder("b").seed(2L).build());
        assertFalse(java.util.Arrays.equals(a.document(0), b.document(0)));
    }

    /**
     * A caller may address documents out of order, so random access must agree with the sequential stream.
     */
    public void testRandomAccessMatchesSequential() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.builder("seq").docCount(50).build());
        List<byte[]> sequential = new ArrayList<>();
        for (Iterator<byte[]> it = generator.iterator(); it.hasNext();) {
            sequential.add(it.next());
        }
        assertEquals(50, sequential.size());
        for (int i = 0; i < sequential.size(); i++) {
            assertArrayEquals("document " + i, sequential.get(i), generator.document(i));
        }
    }

    public void testAdjacentDocumentsAreNotCorrelated() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        // A weak seed derivation such as seed+index would make neighbouring documents share attribute values.
        assertFalse(java.util.Arrays.equals(generator.document(0), generator.document(1)));
        assertFalse(java.util.Arrays.equals(generator.document(1), generator.document(2)));
    }

    public void testDocumentSizeTracksTarget() {
        for (String preset : new String[] { "SMOKE", "S2_SIZE_512", "S2_SIZE_2K", "S2_SIZE_8K", "S2_ATTR_700", "S2_ATTR_1500" }) {
            CorpusConfig config = CorpusConfig.preset(preset);
            OtelDocGenerator generator = new OtelDocGenerator(config);
            long total = 0;
            for (int i = 0; i < SAMPLE; i++) {
                total += generator.document(i).length;
            }
            double mean = (double) total / SAMPLE;
            assertWithin(preset + " mean document size", config.targetDocBytes(), mean, 0.10);
        }
    }

    public void testAttributesSizeTracksTarget() {
        for (String preset : new String[] { "SMOKE", "S2_SIZE_512", "S2_ATTR_288", "S2_ATTR_700", "S2_ATTR_1500" }) {
            CorpusConfig config = CorpusConfig.preset(preset);
            OtelDocGenerator generator = new OtelDocGenerator(config);
            long total = 0;
            for (int i = 0; i < SAMPLE; i++) {
                total += generator.attributesBytes(i);
            }
            double mean = (double) total / SAMPLE;
            assertWithin(preset + " mean attributes size", config.resolvedAttrBytes(), mean, 0.10);
        }
    }

    /**
     * The document-size sweep is only able to separate "tracks attributes bytes" from "tracks whole-document bytes" if
     * attributes stay the same size as the document grows.
     */
    public void testSizeSweepHoldsAttributesConstant() {
        double previous = -1;
        for (String preset : CorpusConfig.sizeSweep()) {
            OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset(preset));
            long total = 0;
            for (int i = 0; i < SAMPLE; i++) {
                total += generator.attributesBytes(i);
            }
            double mean = (double) total / SAMPLE;
            if (previous > 0) {
                assertWithin("attributes size must not drift across the document-size sweep", previous, mean, 0.05);
            }
            previous = mean;
        }
    }

    /**
     * The attributes-size sweep must actually vary attributes while the document stays fixed.
     */
    public void testAttrSweepVariesAttributesAtFixedDocumentSize() {
        List<String> sweep = CorpusConfig.attrSweep();
        double previous = -1;
        for (String preset : sweep) {
            double mean = meanAttributesBytes(preset);
            assertTrue("attributes must grow across the attributes sweep at " + preset, mean > previous);
            previous = mean;
        }

        double docSmall = meanDocumentBytes(sweep.get(0));
        double docLarge = meanDocumentBytes(sweep.get(sweep.size() - 1));
        assertWithin("document size must stay fixed across the attributes sweep", docSmall, docLarge, 0.10);
    }

    /**
     * The document-size sweep must actually vary document size.
     */
    public void testSizeSweepVariesDocumentSize() {
        double previous = -1;
        for (String preset : CorpusConfig.sizeSweep()) {
            double mean = meanDocumentBytes(preset);
            assertTrue("document size must grow across the document-size sweep at " + preset, mean > previous);
            previous = mean;
        }
    }

    /**
     * The two sweeps share a corner, so that index can be built once and used by both.
     */
    public void testSweepsShareACorner() {
        OtelDocGenerator viaSize = new OtelDocGenerator(CorpusConfig.preset("S2_SIZE_2K"));
        OtelDocGenerator viaAttr = new OtelDocGenerator(CorpusConfig.preset("S2_ATTR_288"));
        for (int i = 0; i < 20; i++) {
            assertArrayEquals("shared corner must generate identical documents", viaSize.document(i), viaAttr.document(i));
        }
    }

    public void testStableKeysAlwaysPresent() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        for (int i = 0; i < SAMPLE; i++) {
            Map<String, Object> attributes = generator.attributesAsMap(i);
            for (String key : OtelDocGenerator.stableKeys()) {
                assertTrue("document " + i + " is missing stable key [" + key + "]", attributes.containsKey(key));
                assertNotNull("document " + i + " has null stable key [" + key + "]", attributes.get(key));
            }
        }
    }

    public void testDottedKeyIsLiteralNotNested() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        Map<String, Object> attributes = generator.attributesAsMap(0);
        // The key must be the literal string "k8s.namespace", not a nested {"k8s": {"namespace": ...}} object.
        assertTrue(attributes.containsKey(OtelDocGenerator.KEY_K8S_NAMESPACE));
        assertFalse(attributes.containsKey("k8s"));
        assertTrue(attributes.get(OtelDocGenerator.KEY_K8S_NAMESPACE) instanceof String);
    }

    public void testStableKeyTypesAreConsistentAcrossCorpus() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        for (int i = 0; i < SAMPLE; i++) {
            Map<String, Object> attributes = generator.attributesAsMap(i);
            assertTrue(attributes.get(OtelDocGenerator.KEY_STATUS) instanceof Long);
            assertTrue(attributes.get(OtelDocGenerator.KEY_DURATION_NS) instanceof Long);
            assertTrue(attributes.get(OtelDocGenerator.KEY_LEVEL) instanceof String);
            assertTrue(attributes.get(OtelDocGenerator.KEY_K8S_NAMESPACE) instanceof String);
        }
    }

    public void testSerializedFormParsesBackToTheGeneratedMap() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        for (int i = 0; i < 50; i++) {
            Map<String, Object> expected = generator.documentAsMap(i);
            Map<String, Object> parsed = parse(generator.document(i));
            // Compared after widening integers: see testJsonRoundTripNarrowsLongToInteger for why an exact comparison
            // fails, and why that is a finding rather than a generator bug.
            assertEquals("document " + i + " must round-trip through JSON", widenIntegers(expected), widenIntegers(parsed));
        }
    }

    /**
     * Pins an observation that matters for the type-fidelity question this harness exists to answer: a value the
     * generator emitted as a {@code long} comes back from JSON as an {@link Integer} whenever it fits in 32 bits.
     *
     * <p>Nothing in the JSON text distinguishes the two — {@code 201} is just {@code 201} — so the width is decided by
     * the reader, not the document. That is exactly the mechanism by which a value read back from {@code _source} can lose
     * integer width while one read from the column, which records a type tag at write time, does not. Asserted as current
     * behaviour so that a change to it fails loudly.
     */
    public void testJsonRoundTripNarrowsLongToInteger() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        Map<String, Object> generated = generator.attributesAsMap(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = (Map<String, Object>) parse(generator.document(0)).get(OtelDocGenerator.ATTRIBUTES_FIELD);

        // status is small enough to fit in an int, so the width does not survive the round trip.
        assertTrue("generator emits a long", generated.get(OtelDocGenerator.KEY_STATUS) instanceof Long);
        assertTrue("JSON gives back an int", parsed.get(OtelDocGenerator.KEY_STATUS) instanceof Integer);

        // Values beyond int range keep their width, because nothing narrower can hold them.
        assertTrue(generated.get(OtelDocGenerator.KEY_DURATION_NS) instanceof Long);
        Object duration = parsed.get(OtelDocGenerator.KEY_DURATION_NS);
        long durationValue = ((Number) duration).longValue();
        if (durationValue > Integer.MAX_VALUE) {
            assertTrue("values beyond int range stay long", duration instanceof Long);
        }

        // The numeric value itself is always preserved; only the declared width is lost.
        assertEquals(
            ((Number) generated.get(OtelDocGenerator.KEY_STATUS)).longValue(),
            ((Number) parsed.get(OtelDocGenerator.KEY_STATUS)).longValue()
        );
    }

    private static Map<String, Object> parse(byte[] json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, MediaTypeRegistry.JSON).v2();
    }

    /**
     * Recursively widens {@link Integer} to {@link Long} so two maps can be compared on numeric value rather than on the
     * incidental boxing chosen by whichever side produced them.
     */
    private static Object widenIntegers(Object value) {
        if (value instanceof Integer integer) {
            return integer.longValue();
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> widened = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                widened.put(entry.getKey(), widenIntegers(entry.getValue()));
            }
            return widened;
        }
        if (value instanceof List<?> list) {
            List<Object> widened = new ArrayList<>(list.size());
            for (Object element : list) {
                widened.add(widenIntegers(element));
            }
            return widened;
        }
        return value;
    }

    public void testAttributeCountMatchesConfiguration() {
        CorpusConfig config = CorpusConfig.preset("SMOKE");
        OtelDocGenerator generator = new OtelDocGenerator(config);
        for (int i = 0; i < 50; i++) {
            assertEquals("document " + i + " key count", config.attrKeys(), generator.attributesAsMap(i).size());
        }
    }

    public void testTypeMixCoversEveryValueKind() {
        // Over a large enough sample every configured kind should appear at least once, otherwise the correctness suite
        // would silently never exercise arrays, nested objects, or nulls.
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.builder("mix").attrKeys(12).maxDepth(3).build());
        boolean sawNull = false, sawList = false, sawMap = false, sawBoolean = false, sawDouble = false, sawLong = false;
        for (int i = 0; i < 2000; i++) {
            for (Map.Entry<String, Object> entry : generator.attributesAsMap(i).entrySet()) {
                if (OtelDocGenerator.stableKeys().contains(entry.getKey())) {
                    continue;
                }
                Object value = entry.getValue();
                if (value == null) {
                    sawNull = true;
                } else if (value instanceof List) {
                    sawList = true;
                } else if (value instanceof Map) {
                    sawMap = true;
                } else if (value instanceof Boolean) {
                    sawBoolean = true;
                } else if (value instanceof Double) {
                    sawDouble = true;
                } else if (value instanceof Long) {
                    sawLong = true;
                }
            }
        }
        assertTrue("no null value generated", sawNull);
        assertTrue("no array value generated", sawList);
        assertTrue("no nested object generated", sawMap);
        assertTrue("no boolean value generated", sawBoolean);
        assertTrue("no double value generated", sawDouble);
        assertTrue("no long value generated", sawLong);
    }

    public void testNamespaceCardinalityIsBounded() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("SMOKE"));
        java.util.Set<Object> namespaces = new java.util.HashSet<>();
        for (int i = 0; i < 2000; i++) {
            namespaces.add(generator.attributesAsMap(i).get(OtelDocGenerator.KEY_K8S_NAMESPACE));
        }
        // Group-by tests need a small, stable set of buckets.
        assertEquals(16, namespaces.size());
    }

    public void testRejectsTooFewAttributeKeys() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CorpusConfig.builder("bad").attrKeys(4).build());
        assertTrue(e.getMessage(), e.getMessage().contains("attrKeys must be at least 5"));
    }

    public void testRejectsUnknownPreset() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CorpusConfig.preset("NOPE"));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown corpus preset"));
    }

    public void testAbsoluteAttributeBytesOverridesFraction() {
        CorpusConfig config = CorpusConfig.builder("abs").targetDocBytes(4096).attrFraction(0.9).attrTargetBytes(300).build();
        assertEquals(300, config.resolvedAttrBytes());
    }

    private double meanAttributesBytes(String preset) {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset(preset));
        long total = 0;
        for (int i = 0; i < SAMPLE; i++) {
            total += generator.attributesBytes(i);
        }
        return (double) total / SAMPLE;
    }

    private double meanDocumentBytes(String preset) {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset(preset));
        long total = 0;
        for (int i = 0; i < SAMPLE; i++) {
            total += generator.document(i).length;
        }
        return (double) total / SAMPLE;
    }

    private static void assertWithin(String message, double expected, double actual, double tolerance) {
        double delta = Math.abs(actual - expected) / expected;
        assertTrue(
            message
                + ": expected ~"
                + expected
                + " but was "
                + actual
                + " ("
                + Math.round(delta * 100)
                + "% off, tolerance "
                + Math.round(tolerance * 100)
                + "%)",
            delta <= tolerance
        );
    }
}
