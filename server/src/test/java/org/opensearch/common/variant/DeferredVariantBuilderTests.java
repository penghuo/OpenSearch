/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Map;

public class DeferredVariantBuilderTests extends OpenSearchTestCase {

    public void testEncodesWithFieldIdsInNameOrder() {
        DeferredVariantBuilder builder = new DeferredVariantBuilder();
        builder.startObject();
        builder.appendKey("z");
        builder.appendLong(1);
        builder.appendKey("a");
        builder.startArray();
        builder.appendBoolean(true);
        builder.appendNull();
        builder.appendString("value");
        builder.endArray();
        builder.endObject();

        DeferredVariantBuilder.EncodedValue encoded = builder.finish();
        assertNotNull(encoded);
        assertEquals(2, encoded.sortedKeyBytes().length);
        assertArrayEquals(new byte[] { 'a' }, encoded.sortedKeyBytes()[0]);
        assertArrayEquals(new byte[] { 'z' }, encoded.sortedKeyBytes()[1]);

        Variant decoded = decode(encoded);
        assertEquals(Map.of("a", Arrays.asList(true, null, "value"), "z", 1L), decoded.toJavaObject());
        assertNotNull(decoded.objectGetByFieldId(0));
        assertNotNull(decoded.objectGetByFieldId(1));
    }

    public void testDistinctStringsWithTheSameUtf8BytesAreUnavailable() {
        DeferredVariantBuilder builder = new DeferredVariantBuilder();
        builder.startObject();
        builder.appendKey("?");
        builder.appendLong(1);
        builder.appendKey("\uD800");
        builder.appendLong(2);
        builder.endObject();

        assertNull(builder.finish());
    }

    public void testDuplicateKeyInOneObjectIsUnavailable() {
        DeferredVariantBuilder builder = new DeferredVariantBuilder();
        builder.startObject();
        builder.appendKey("value");
        builder.appendLong(1);
        builder.appendKey("value");
        builder.appendLong(2);
        builder.endObject();

        assertNull(builder.finish());
    }

    public void testInvalidStructureFailsWhenTheDeferredEventsAreEncoded() {
        DeferredVariantBuilder builder = new DeferredVariantBuilder();
        builder.startObject();
        builder.appendKey("value");
        builder.appendLong(1);

        expectThrows(IllegalStateException.class, builder::finish);
    }

    private static Variant decode(DeferredVariantBuilder.EncodedValue encoded) {
        byte[][] names = encoded.sortedKeyBytes();
        int[] ordinals = new int[names.length];
        for (int i = 0; i < names.length; i++) {
            ordinals[i] = i;
        }
        return new Variant(new VariantMetadata(names, ordinals, names.length), encoded.valueBytes(), 0);
    }
}
