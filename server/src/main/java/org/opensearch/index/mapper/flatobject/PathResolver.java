/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Walks a {@link FlatObjectPath} over a nested value, independently of how that value is stored.
 *
 * <p><b>Why this class exists.</b> A path can be resolved against a parsed {@code _source} map or against a Variant
 * blob, and the two must name the same value. Held separately, the rules below would drift and a read would answer
 * differently depending on which store served it. So the rules live here once and each store supplies only a
 * {@link Navigator}.
 *
 * <p><b>The rules.</b> Each step consumes exactly one level:
 *
 * <ul>
 *   <li>{@code KEY} looks the name up in an object, taken literally. No dot splitting and no prefix guessing — the path
 *       was already parsed, so {@code ['a.b']} and {@code a.b} arrive here as different step lists.
 *   <li>{@code ARRAY_ALL} replaces the current node with every element of one array level. Applied to anything that is
 *       not an array it selects nothing, rather than treating the value as a one-element array: a selector asserts that
 *       the value is an array, and quietly agreeing when it is not would report the same result for two different
 *       documents.
 *   <li>{@code ARRAY_INDEX} selects one element, or nothing when the node is not an array or the index is past its end.
 * </ul>
 *
 * <p><b>Three outcomes, not two.</b> {@link #MISSING} means no such path. {@code null} means the path exists and holds
 * JSON null. An empty {@link List} means a wildcard traversed a real array but nothing under it matched — which is
 * different from the array being absent, and is the reason a wildcard commits to a list result as soon as it walks one.
 *
 * <p>Once a wildcard has run, later steps map over the surviving nodes and their results concatenate in encounter order.
 * A member missing from one element is skipped; a member holding JSON null is kept as a null element.
 *
 * @opensearch.internal
 */
final class PathResolver {

    /**
     * Sentinel meaning "no such path". Distinct from {@code null}, which means "the path exists and holds null".
     */
    public static final Object MISSING = new Object();

    private PathResolver() {}

    /**
     * Looks inside one level of a nested value.
     *
     * <p>Implementations see only nodes they themselves produced, so each may use its own node representation.
     */
    public interface Navigator {
        /**
         * @param node the container to look inside
         * @param key  the key to look up, taken literally
         * @return the child value, {@code null} if the key is present and holds null, or {@link #MISSING} if {@code node}
         *         is not an object or has no such key
         */
        Object child(Object node, String key);

        /**
         * @return the number of elements, or {@code -1} if {@code node} is not an array
         */
        int arraySize(Object node);

        /**
         * @param index a position known to be within {@link #arraySize}
         * @return the element, or {@code null} if it holds null
         */
        Object arrayGet(Object node, int index);

        /**
         * Converts a resolved node into the plain Java value a caller sees: {@code Long}, {@code Double},
         * {@code BigDecimal}, {@code Boolean}, {@code String}, {@code Map} or {@code List}.
         */
        Object materialise(Object node);
    }

    /**
     * Navigator over a parsed {@code _source} map, as produced by {@code XContentHelper.convertToMap}.
     */
    public static final Navigator MAP_NAVIGATOR = new Navigator() {
        @Override
        public Object child(Object node, String key) {
            if (node instanceof Map<?, ?> map) {
                // containsKey rather than a null check on get(), so a stored null is not mistaken for an absent key.
                if (map.containsKey(key)) {
                    return map.get(key);
                }
            }
            return MISSING;
        }

        @Override
        public int arraySize(Object node) {
            return node instanceof List<?> list ? list.size() : -1;
        }

        @Override
        public Object arrayGet(Object node, int index) {
            return ((List<?>) node).get(index);
        }

        @Override
        public Object materialise(Object node) {
            return node;
        }
    };

    /**
     * Resolves the field's own name within a {@code _source} map.
     *
     * <p>Separate from {@link #resolve} because a field name is not a user path: a mapping may declare
     * {@code issue.labels}, which {@code _source} may hold either nested or as one literal key, and the mapping decides
     * which rather than the reader. So this keeps the longest-matching-prefix probe that a field name needs, and the user
     * path syntax stays free of it.
     *
     * @return the field's value, {@code null} if it is present and null, or {@link #MISSING}
     */
    public static Object resolveFieldName(Object root, String fieldName, Navigator navigator) {
        if (root == null || fieldName == null || fieldName.isEmpty()) {
            return MISSING;
        }
        Object node = root;
        String remaining = fieldName;
        while (true) {
            Object whole = navigator.child(node, remaining);
            if (whole != MISSING) {
                return whole;
            }
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
            if (matchedEnd < 0 || matched == null) {
                return MISSING;
            }
            node = matched;
            remaining = remaining.substring(matchedEnd + 1);
        }
    }

    /**
     * Resolves {@code path} against {@code root}.
     *
     * @return the materialised value; {@code null} if the path exists and holds null; a {@code List} if the path uses a
     *         wildcard; or {@link #MISSING} if absent
     */
    public static Object resolve(Object root, FlatObjectPath path, Navigator navigator) {
        if (root == null) {
            return MISSING;
        }
        // Without a wildcard every step maps one node to one node, so the whole walk needs no collection at all. That is the
        // common case -- attributes.region, attributes.events{0}.status -- and it is the one a PPL projection runs per
        // document, so it is kept allocation-free rather than sharing the fan-out loop below.
        if (path.isWildcarded() == false) {
            return resolveSingle(root, path, navigator);
        }
        return resolveWildcarded(root, path, navigator);
    }

    /** One node in, one node out, for a path with no {@code ARRAY_ALL} step. */
    private static Object resolveSingle(Object root, FlatObjectPath path, Navigator navigator) {
        Object node = root;
        // Indexed rather than enhanced-for: an iterator would be the one allocation on a path that otherwise makes none.
        List<FlatObjectPath.Step> steps = path.steps();
        for (int s = 0, count = steps.size(); s < count; s++) {
            FlatObjectPath.Step step = steps.get(s);
            if (node == null) {
                // A null holds nothing to look inside, and it is only ever the final answer.
                return MISSING;
            }
            switch (step.kind()) {
                case KEY: {
                    node = navigator.child(node, step.key());
                    if (node == MISSING) {
                        return MISSING;
                    }
                    break;
                }
                case ARRAY_INDEX: {
                    int size = navigator.arraySize(node);
                    if (size < 0 || step.index() >= size) {
                        return MISSING;
                    }
                    node = navigator.arrayGet(node, step.index());
                    break;
                }
                default:
                    throw new IllegalStateException("resolveSingle reached " + step.kind());
            }
        }
        return node == null ? null : navigator.materialise(node);
    }

    /** Fan-out for a path with at least one {@code ARRAY_ALL} step. */
    private static Object resolveWildcarded(Object root, FlatObjectPath path, Navigator navigator) {
        List<Object> nodes = new ArrayList<>(1);
        nodes.add(root);
        // Set by the first wildcard that walks a real array. Until then an empty result is "absent"; afterwards it is "the
        // array was there and held nothing matching", which has to stay a different answer.
        boolean walkedAnArray = false;

        for (FlatObjectPath.Step step : path.steps()) {
            List<Object> next = new ArrayList<>(nodes.size());
            for (Object node : nodes) {
                if (node == null) {
                    continue;
                }
                switch (step.kind()) {
                    case KEY: {
                        Object child = navigator.child(node, step.key());
                        if (child != MISSING) {
                            next.add(child);
                        }
                        break;
                    }
                    case ARRAY_ALL: {
                        int size = navigator.arraySize(node);
                        if (size < 0) {
                            // Not an array: no match, and no singleton coercion.
                            break;
                        }
                        walkedAnArray = true;
                        for (int i = 0; i < size; i++) {
                            next.add(navigator.arrayGet(node, i));
                        }
                        break;
                    }
                    case ARRAY_INDEX: {
                        int size = navigator.arraySize(node);
                        if (size < 0 || step.index() >= size) {
                            break;
                        }
                        next.add(navigator.arrayGet(node, step.index()));
                        break;
                    }
                }
            }
            nodes = next;
            if (nodes.isEmpty()) {
                return walkedAnArray ? Collections.emptyList() : MISSING;
            }
        }

        List<Object> out = new ArrayList<>(nodes.size());
        for (Object node : nodes) {
            out.add(node == null ? null : navigator.materialise(node));
        }
        return Collections.unmodifiableList(out);
    }
}
