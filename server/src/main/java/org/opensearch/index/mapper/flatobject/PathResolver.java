/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import java.util.Map;

/**
 * Resolves a dotted path against a nested value, independently of how that value is stored.
 *
 * <p><b>Why this class exists.</b> Solution A navigates a parsed {@code _source} map and Solution B navigates a Variant
 * blob, but they must agree on <em>which</em> value a path names. If each arm carried its own lookup logic, a divergence
 * in results could come from the lookup rather than from the value store, and the comparison would no longer be
 * controlled. Both arms therefore share this resolver and differ only in the {@link Navigator} they supply.
 *
 * <p><b>The ambiguity.</b> {@code k8s.namespace} may mean the literal key {@code "k8s.namespace"} or the nested path
 * {@code "k8s"} then {@code "namespace"}. Real OTel attribute keys contain dots routinely, and {@code flat_object} itself
 * flattens with a dot, so the ambiguity is unavoidable and a rule has to be fixed.
 *
 * <p><b>The rule: longest matching prefix at each level, without backtracking.</b> At each level the resolver tries the
 * whole remaining path as a literal key first, then progressively shorter dot-delimited prefixes. The first prefix that
 * exists is taken, and resolution continues below it. So for {@code a.b.c} the probe order at the root is
 * {@code "a.b.c"}, {@code "a.b"}, {@code "a"}.
 *
 * <p>Two consequences worth being explicit about:
 *
 * <ul>
 *   <li>A literal key always wins over a nested interpretation. Given {@code {"a.b": 1, "a": {"b": 2}}}, the path
 *       {@code a.b} resolves to {@code 1}. This also makes the common case the fast case: one probe.
 *   <li>There is <b>no backtracking</b>. Once a prefix matches, the resolver commits to it. Given
 *       {@code {"a": {"x": 1}}} and the path {@code a.b}, the result is missing even though no other reading exists.
 *       Full backtracking would be exponential in the number of segments and is not needed for real data; the behaviour
 *       is pinned by test rather than left implicit.
 * </ul>
 *
 * @opensearch.internal
 */
public final class PathResolver {

    /**
     * Sentinel meaning "no such path". Distinct from {@code null}, which means "the path exists and holds null" — a
     * distinction the coercion layer needs in order not to count a JSON null as a coercion failure.
     */
    public static final Object MISSING = new Object();

    private PathResolver() {}

    /**
     * Looks up a single key within one level of a nested value.
     */
    @FunctionalInterface
    public interface Navigator {
        /**
         * @param node the container to look inside
         * @param key  the key to look up, taken literally
         * @return the child value, {@code null} if the key is present and holds null, or {@link #MISSING} if {@code node}
         *         is not an object or has no such key
         */
        Object child(Object node, String key);
    }

    /**
     * Navigator over a parsed {@code _source} map, as produced by {@code XContentHelper.convertToMap}.
     */
    public static final Navigator MAP_NAVIGATOR = (node, key) -> {
        if (node instanceof Map<?, ?> map) {
            // containsKey rather than a null check on get(), so a stored null is not mistaken for an absent key.
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        return MISSING;
    };

    /**
     * Resolves {@code path} against {@code root}.
     *
     * @return the value at the path, {@code null} if it exists and holds null, or {@link #MISSING} if absent
     */
    public static Object resolve(Object root, String path, Navigator navigator) {
        if (root == null || path == null || path.isEmpty()) {
            return MISSING;
        }

        Object node = root;
        String remaining = path;

        while (true) {
            // The whole remaining path as a literal key: the common case for real OTel keys, so it is probed first.
            Object whole = navigator.child(node, remaining);
            if (whole != MISSING) {
                return whole;
            }

            // Then shorter prefixes, longest first.
            Object matched = MISSING;
            int matchedEnd = -1;
            for (int dot = remaining.lastIndexOf('.'); dot > 0; dot = remaining.lastIndexOf('.', dot - 1)) {
                Object candidate = navigator.child(node, remaining.substring(0, dot));
                if (candidate != MISSING) {
                    matched = candidate;
                    matchedEnd = dot;
                    break;
                }
            }

            if (matchedEnd < 0) {
                return MISSING;
            }
            if (matched == null) {
                // The prefix exists but holds null, so there is nothing to descend into.
                return MISSING;
            }
            node = matched;
            remaining = remaining.substring(matchedEnd + 1);
        }
    }
}
