/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.index.mapper.flatobject.FlatObjectPath.Kind;
import org.opensearch.index.mapper.flatobject.FlatObjectPath.Step;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class FlatObjectPathTests extends OpenSearchTestCase {

    private static List<Step> steps(String path) {
        return FlatObjectPath.compile(path).steps();
    }

    private static void assertKeys(String path, String... expected) {
        List<Step> steps = steps(path);
        assertEquals(path + " step count", expected.length, steps.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(path + " step " + i + " kind", Kind.KEY, steps.get(i).kind());
            assertEquals(path + " step " + i, expected[i], steps.get(i).key());
        }
    }

    public void testAPlainKey() {
        assertKeys("region", "region");
    }

    public void testADottedPathIsNesting() {
        assertKeys("sub.center.value", "sub", "center", "value");
    }

    public void testAWildcardConsumesOneArrayLevel() {
        List<Step> steps = steps("events{}.status");
        assertEquals(3, steps.size());
        assertEquals(Kind.KEY, steps.get(0).kind());
        assertEquals("events", steps.get(0).key());
        assertEquals(Kind.ARRAY_ALL, steps.get(1).kind());
        assertEquals(Kind.KEY, steps.get(2).kind());
        assertEquals("status", steps.get(2).key());
    }

    /** {@code {*}} is the same selector spelled out, which spath accepts and so does this. */
    public void testStarIsTheSameAsAnEmptySelector() {
        assertEquals(steps("events{}.status"), steps("events{*}.status"));
        assertTrue(FlatObjectPath.compile("events{*}.status").isWildcarded());
    }

    public void testAnIndexSelectsOneElement() {
        List<Step> steps = steps("events{0}.status");
        assertEquals(Kind.ARRAY_INDEX, steps.get(1).kind());
        assertEquals(0, steps.get(1).index());
        assertEquals(7, steps("events{7}").get(1).index());
        assertFalse("an index is not a wildcard", FlatObjectPath.compile("events{0}.status").isWildcarded());
    }

    public void testRepeatedSelectorsTraverseRepeatedLevels() {
        List<Step> steps = steps("matrix{}{}.value");
        assertEquals(4, steps.size());
        assertEquals(Kind.KEY, steps.get(0).kind());
        assertEquals(Kind.ARRAY_ALL, steps.get(1).kind());
        assertEquals(Kind.ARRAY_ALL, steps.get(2).kind());
        assertEquals(Kind.KEY, steps.get(3).kind());
    }

    public void testAMixtureOfSelectors() {
        List<Step> steps = steps("a{0}{}.b{2}");
        assertEquals(
            List.of(Kind.KEY, Kind.ARRAY_INDEX, Kind.ARRAY_ALL, Kind.KEY, Kind.ARRAY_INDEX),
            steps.stream().map(Step::kind).toList()
        );
    }

    /**
     * A quoted name is taken whole, which is the only way to reach a key that contains a dot -- and real OTel attribute keys
     * contain dots routinely.
     */
    public void testAQuotedNameIsLiteral() {
        assertKeys("['cluster.name']", "cluster.name");
        assertKeys("attributes.['cluster.name']", "attributes", "cluster.name");
        assertKeys("['a.b'].['c.d']", "a.b", "c.d");
    }

    public void testDoubleQuotesWorkToo() {
        assertKeys("[\"cluster.name\"]", "cluster.name");
    }

    /** Braces inside a quoted name are part of the name, not a selector. */
    public void testAQuotedNameCanContainBraces() {
        assertKeys("['events{}']", "events{}");
        assertFalse(FlatObjectPath.compile("['events{}']").isWildcarded());
    }

    /** flat_object accepts an empty key, so the path syntax has to be able to name it. */
    public void testTheEmptyNameIsReachableWhenQuoted() {
        assertKeys("['']", "");
        assertKeys("a.['']", "a", "");
        // But a bare empty segment stays a typo, not a reference to that key.
        expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile("a..b"));
    }

    public void testQuotesAndBackslashesCanBeEscaped() {
        assertKeys("['it\\'s']", "it's");
        assertKeys("[\"say \\\"hi\\\"\"]", "say \"hi\"");
        assertKeys("['back\\\\slash']", "back\\slash");
        // A key holding both quote characters is reachable either way round.
        assertKeys("['both \\' and \"']", "both ' and \"");
        assertKeys("[\"both ' and \\\"\"]", "both ' and \"");
    }

    public void testAnInvalidEscapeIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile("['a\\nb']"));
        expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile("['a\\"));
    }

    /** And a selector may still follow one, so a literal key holding an array is reachable. */
    public void testASelectorMayFollowAQuotedName() {
        List<Step> steps = steps("['events{}']{}.status");
        assertEquals(3, steps.size());
        assertEquals("events{}", steps.get(0).key());
        assertEquals(Kind.ARRAY_ALL, steps.get(1).kind());
    }

    /** A selector always follows a member name; a path cannot begin with one. */
    public void testAPathCannotBeginWithASelector() {
        for (String bad : List.of("{}", "{}.host", "{0}.host", "events.{}.status", "a.{}")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile(bad));
            assertTrue(e.getMessage(), e.getMessage().contains("empty member name"));
        }
    }

    public void testWildcardedIsOnlyTrueForAWildcard() {
        assertFalse(FlatObjectPath.compile("a.b").isWildcarded());
        assertFalse(FlatObjectPath.compile("a{3}.b").isWildcarded());
        assertTrue(FlatObjectPath.compile("a{}.b").isWildcarded());
        assertTrue(FlatObjectPath.compile("a.b{}").isWildcarded());
    }

    public void testMalformedPathsAreRejected() {
        for (String bad : List.of(
            "",
            ".",
            "a.",
            ".a",
            "a..b",
            "a{",
            "a{x}",
            "a{-1}",
            "a{1.5}",
            "a{ }",
            "a{01}",
            "a['b']",
            "['b",
            "['b']c",
            "[b]",
            "[]"
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile(bad));
            assertTrue("message should name the path, was: " + e.getMessage(), e.getMessage().contains("flat_object path"));
        }
    }

    /** A leading zero is a typo rather than an index, and accepting it would make two paths mean one thing. */
    public void testALeadingZeroIsRejectedButZeroItselfIsNot() {
        expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile("a{00}"));
        assertEquals(0, steps("a{0}").get(1).index());
    }

    public void testNullIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> FlatObjectPath.compile(null));
    }

    public void testToStringIsThePathAsWritten() {
        assertEquals("a{}.b", FlatObjectPath.compile("a{}.b").toString());
    }

    public void testEqualityIsBySource() {
        assertEquals(FlatObjectPath.compile("a{}.b"), FlatObjectPath.compile("a{}.b"));
        assertEquals(FlatObjectPath.compile("a{}.b").hashCode(), FlatObjectPath.compile("a{}.b").hashCode());
        assertNotEquals(FlatObjectPath.compile("a{}.b"), FlatObjectPath.compile("a{0}.b"));
    }
}
