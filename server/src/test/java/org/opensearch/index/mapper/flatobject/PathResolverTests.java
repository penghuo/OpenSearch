/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathResolverTests extends OpenSearchTestCase {

    private static Object resolve(Map<String, Object> root, String path) {
        return PathResolver.resolve(root, path, PathResolver.MAP_NAVIGATOR);
    }

    private static Map<String, Object> map(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    public void testSingleSegment() {
        assertEquals(1, resolve(map("a", 1), "a"));
    }

    public void testNestedPath() {
        assertEquals(2, resolve(map("a", map("b", 2)), "a.b"));
    }

    public void testDeeplyNestedPath() {
        assertEquals(3, resolve(map("a", map("b", map("c", 3))), "a.b.c"));
    }

    public void testLiteralDottedKey() {
        assertEquals(4, resolve(map("k8s.namespace", 4), "k8s.namespace"));
    }

    /**
     * The rule that has to be fixed and shared, because both stores must agree on it.
     */
    public void testLiteralKeyWinsOverNestedInterpretation() {
        Map<String, Object> root = map("a.b", 1, "a", map("b", 2));
        assertEquals("the literal key must win", 1, resolve(root, "a.b"));
    }

    public void testLongestPrefixWinsAtEachLevel() {
        // "a.b" as a literal key at the root, then "c" below it, beats "a" then "b.c".
        Map<String, Object> root = map("a.b", map("c", 1), "a", map("b.c", 2));
        assertEquals(1, resolve(root, "a.b.c"));
    }

    public void testFallsBackToShorterPrefix() {
        Map<String, Object> root = map("a", map("b.c", 2));
        assertEquals(2, resolve(root, "a.b.c"));
    }

    /**
     * Pins the documented absence of backtracking. Once a prefix matches, resolution commits to it.
     */
    public void testDoesNotBacktrack() {
        Map<String, Object> root = map("a", map("x", 1));
        assertSame("no backtracking, so this is missing", PathResolver.MISSING, resolve(root, "a.b"));
    }

    public void testMissingKey() {
        assertSame(PathResolver.MISSING, resolve(map("a", 1), "b"));
    }

    public void testMissingNestedKey() {
        assertSame(PathResolver.MISSING, resolve(map("a", map("b", 1)), "a.c"));
    }

    /**
     * A stored null is not the same as an absent key: the coercion layer must not count a JSON null as a failure.
     */
    public void testStoredNullIsDistinctFromMissing() {
        assertNull("present but null", resolve(map("a", null), "a"));
        assertSame("absent", PathResolver.MISSING, resolve(map("a", null), "b"));
    }

    public void testCannotDescendIntoNull() {
        assertSame(PathResolver.MISSING, resolve(map("a", null), "a.b"));
    }

    public void testCannotDescendIntoScalar() {
        assertSame(PathResolver.MISSING, resolve(map("a", 1), "a.b"));
    }

    public void testResolvesToContainer() {
        Map<String, Object> nested = map("b", 1);
        assertEquals(nested, resolve(map("a", nested), "a"));
    }

    public void testResolvesToList() {
        List<Object> list = List.of(1, 2, 3);
        assertEquals(list, resolve(map("a", list), "a"));
    }

    public void testArrayIndexingIsNotSupported() {
        // Documented scope: paths address object keys only, matching how flat_object flattens arrays.
        assertSame(PathResolver.MISSING, resolve(map("a", List.of(1, 2)), "a.0"));
    }

    public void testEmptyAndNullPaths() {
        assertSame(PathResolver.MISSING, resolve(map("a", 1), ""));
        assertSame(PathResolver.MISSING, resolve(map("a", 1), null));
    }

    public void testNullRoot() {
        assertSame(PathResolver.MISSING, PathResolver.resolve(null, "a", PathResolver.MAP_NAVIGATOR));
    }

    public void testLeadingAndTrailingDots() {
        // A leading dot cannot form a non-empty prefix, so it resolves to missing rather than looping.
        assertSame(PathResolver.MISSING, resolve(map("a", 1), ".a"));
        assertSame(PathResolver.MISSING, resolve(map("a", 1), "a."));
    }

    public void testKeyThatIsOnlyDots() {
        assertEquals(7, resolve(map("..", 7), ".."));
    }

    /**
     * An empty key is unreachable because {@code resolve} short-circuits on an empty path. Pinned deliberately so the
     * limitation is visible rather than surprising.
     */
    public void testEmptyStringKeyIsUnreachable() {
        assertSame(PathResolver.MISSING, resolve(map("", 8), ""));
    }
}
