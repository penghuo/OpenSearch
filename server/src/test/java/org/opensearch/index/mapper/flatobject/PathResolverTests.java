/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The path rules, over plain maps and lists. The Variant store is required to answer identically, which
 * {@link AccessorEquivalenceTests} checks against real documents; this pins what the answer should be.
 */
public class PathResolverTests extends OpenSearchTestCase {

    private static Object resolve(Object root, String path) {
        return PathResolver.resolve(root, FlatObjectPath.compile(path), PathResolver.MAP_NAVIGATOR);
    }

    private static Map<String, Object> map(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static List<Object> list(Object... values) {
        List<Object> out = new ArrayList<>();
        for (Object value : values) {
            out.add(value);
        }
        return out;
    }

    private static Map<String, Object> document() {
        return map("region", "us-east-1", "sub", map("center", 1L), "events", list(map("status", 1L), map("status", 2L)));
    }

    public void testAScalar() {
        assertEquals("us-east-1", resolve(document(), "region"));
    }

    public void testAnObjectComesBackWhole() {
        assertEquals(map("center", 1L), resolve(document(), "sub"));
        assertEquals(1L, resolve(document(), "sub.center"));
    }

    public void testAnArrayComesBackWhole() {
        assertEquals(list(map("status", 1L), map("status", 2L)), resolve(document(), "events"));
    }

    public void testAWildcardCollectsEveryElement() {
        assertEquals(List.of(1L, 2L), resolve(document(), "events{}.status"));
    }

    public void testAnIndexSelectsOneElement() {
        assertEquals(1L, resolve(document(), "events{0}.status"));
        assertEquals(2L, resolve(document(), "events{1}.status"));
        assertEquals(map("status", 1L), resolve(document(), "events{0}"));
    }

    /**
     * The rule the whole syntax exists for: without a selector an array is a dead end, so a read can never quietly lose
     * which element a value came from.
     */
    public void testArraysAreNotTraversedImplicitly() {
        assertSame(PathResolver.MISSING, resolve(document(), "events.status"));
    }

    public void testAnAbsentPathIsMissing() {
        assertSame(PathResolver.MISSING, resolve(document(), "nope"));
        assertSame(PathResolver.MISSING, resolve(document(), "sub.nope"));
        assertSame(PathResolver.MISSING, resolve(document(), "region.nope"));
    }

    public void testAnExplicitNullIsPresent() {
        assertNull(resolve(map("a", null), "a"));
        assertSame(PathResolver.MISSING, resolve(map("a", null), "a.b"));
    }

    public void testAWildcardOverAnEmptyArrayIsPresentAndEmpty() {
        assertEquals(List.of(), resolve(map("events", list()), "events{}.status"));
        assertEquals(List.of(), resolve(map("events", list()), "events{}"));
    }

    /**
     * An array that exists but holds nothing matching is not the same as no array, so the first stays a present empty list.
     */
    public void testAWildcardMatchingNothingIsPresentAndEmpty() {
        assertEquals(List.of(), resolve(map("events", list(map("other", 1L))), "events{}.status"));
    }

    public void testAWildcardOnAnAbsentPathIsMissing() {
        assertSame(PathResolver.MISSING, resolve(map(), "events{}.status"));
    }

    public void testASelectorOnANonArrayIsMissing() {
        assertSame(PathResolver.MISSING, resolve(map("events", map("status", 1L)), "events{}.status"));
        assertSame(PathResolver.MISSING, resolve(map("events", 5L), "events{}"));
        assertSame(PathResolver.MISSING, resolve(map("events", map("status", 1L)), "events{0}.status"));
    }

    public void testAnIndexPastTheEndIsMissing() {
        assertSame(PathResolver.MISSING, resolve(document(), "events{2}.status"));
        assertSame(PathResolver.MISSING, resolve(map("events", list()), "events{0}"));
    }

    public void testMissingMembersAreSkippedAndExplicitNullsKept() {
        Map<String, Object> doc = map("events", list(map("status", 1L), map("other", 9L), map("status", null), map("status", 3L)));
        assertEquals(list(1L, null, 3L), resolve(doc, "events{}.status"));
    }

    public void testRepeatedWildcardsFlattenInEncounterOrder() {
        Map<String, Object> doc = map("outer", list(map("inner", list(map("x", 1L), map("x", 2L))), map("inner", list(map("x", 3L)))));
        assertEquals(List.of(1L, 2L, 3L), resolve(doc, "outer{}.inner{}.x"));
    }

    public void testNestedArraysNeedOneSelectorPerLevel() {
        Map<String, Object> doc = map("matrix", list(list(map("value", 1L)), list(map("value", 2L))));
        assertEquals(List.of(1L, 2L), resolve(doc, "matrix{}{}.value"));
        // One selector reaches the inner arrays, which are not objects, so nothing resolves.
        assertEquals(List.of(), resolve(doc, "matrix{}.value"));
    }

    public void testHeterogeneousElementsKeepTheirOwnTypes() {
        Map<String, Object> doc = map("mixed", list(map("v", 1L), map("v", "text"), map("v", true), map("v", 1.5)));
        assertEquals(list(1L, "text", true, 1.5), resolve(doc, "mixed{}.v"));
    }

    public void testAWildcardOverScalarsYieldsTheScalars() {
        assertEquals(List.of(80L, 443L), resolve(map("ports", list(80L, 443L)), "ports{}"));
    }

    /**
     * An unquoted dot always means nesting and a quoted name is always literal, so neither reading has to be guessed at.
     */
    public void testQuotingDecidesBetweenALiteralKeyAndNesting() {
        Map<String, Object> doc = map("a.b", 1L, "a", map("b", 2L));
        assertEquals(1L, resolve(doc, "['a.b']"));
        assertEquals(2L, resolve(doc, "a.b"));
    }

    public void testALiteralKeyContainingASelectorIsReachable() {
        Map<String, Object> doc = map("events{}", list(map("status", 7L)));
        assertEquals(list(map("status", 7L)), resolve(doc, "['events{}']"));
        assertEquals(List.of(7L), resolve(doc, "['events{}']{}.status"));
        // And the selector reading finds no array named `events`.
        assertSame(PathResolver.MISSING, resolve(doc, "events{}.status"));
    }

    /**
     * A field name may be stored nested or as one literal key, and the mapping decides which, so it keeps the
     * longest-prefix probe that user paths deliberately do not have.
     */
    public void testFieldNameResolutionProbesLongestPrefixFirst() {
        Map<String, Object> flat = map("issue.labels", map("name", "abc"));
        assertEquals(map("name", "abc"), PathResolver.resolveFieldName(flat, "issue.labels", PathResolver.MAP_NAVIGATOR));

        Map<String, Object> nested = map("issue", map("labels", map("name", "abc")));
        assertEquals(map("name", "abc"), PathResolver.resolveFieldName(nested, "issue.labels", PathResolver.MAP_NAVIGATOR));
    }

    public void testFieldNameResolutionDoesNotBacktrack() {
        // `a` matches, and resolution commits to it, so `a.b` is missing even though no other reading exists.
        assertSame(PathResolver.MISSING, PathResolver.resolveFieldName(map("a", map("x", 1L)), "a.b", PathResolver.MAP_NAVIGATOR));
    }

    public void testResolvingAgainstNothing() {
        assertSame(PathResolver.MISSING, resolve(null, "a"));
        assertSame(PathResolver.MISSING, PathResolver.resolveFieldName(null, "a", PathResolver.MAP_NAVIGATOR));
        assertSame(PathResolver.MISSING, PathResolver.resolveFieldName(map("a", 1L), "", PathResolver.MAP_NAVIGATOR));
    }
}
