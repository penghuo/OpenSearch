/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.opensearch.common.annotation.InternalApi;

import java.util.ArrayList;
import java.util.List;

/**
 * A path into a {@code flat_object} value, compiled once from the restricted spath syntax PPL already exposes.
 *
 * <p><b>Grammar.</b> A path is dot-separated segments. A segment names one object member, either bare or
 * bracket-quoted, and may be followed by array selectors:
 *
 * <pre>
 * path     := segment ( '.' segment )*
 * segment  := ( name | "['" quoted "']" | "[\"" quoted "\"]" ) selector*
 * selector := '{' ( '' | '*' | digits ) '}'
 * quoted   := any characters, with \\ \' \" standing for a backslash and the two quotes
 * </pre>
 *
 * <p>So {@code events{}.status} reads {@code status} from every element of the {@code events} array,
 * {@code events{0}.status} from its first element, {@code matrix{}{}.value} descends two array levels, and
 * {@code ['cluster.name']} names a member whose own name contains a dot.
 *
 * <p>A selector always follows a member name. A path cannot begin with one, so a field whose <em>whole</em> value is an array
 * has no keyed read: {@code doc['attributes']} still returns it, and the field-name lookup requires a dot after the root
 * anyway, so there is no name such a path could be written under.
 *
 * <p><b>Why quoting rather than guessing.</b> {@code a.b} is ambiguous in a schemaless object: it may be the member
 * {@code "a.b"} or the member {@code b} of {@code a}. A read path has to be decidable, so here an unquoted dot always
 * means nesting and a literal name is written {@code ['a.b']}. That also makes a name containing {@code {} } reachable,
 * as {@code ['events{}']}, and the empty name -- which {@code flat_object} accepts -- as {@code ['']}. With both quote
 * forms and the escapes, no JSON key is out of reach.
 *
 * <p>The distinction still holds when both shapes occur in one object. For
 * {@code {"a.b":"literal","a":{"b":"nested"}}}, {@code a.b} reads {@code "nested"} and {@code ['a.b']} reads
 * {@code "literal"}. The field's legacy terms deliberately remain flattened and therefore treat those as two values at
 * the same query path; the Variant read path does not reproduce that lossy view.
 *
 * <p><b>Arrays are never traversed implicitly.</b> {@code events.status} over {@code {"events":[{"status":1}]}} does not
 * resolve; only {@code events{}.status} does. Without that rule a path could not distinguish "the array's elements each
 * have a status" from "the object has a status", and every multi-valued read would silently lose which element a value
 * came from.
 *
 * <p><b>Only doc-value reads compile a path.</b> The same string means something different to a query: {@code flat_object}
 * accepts any literal key, braces included, so {@code {"events{}": 1}} is a legal document and
 * {@code {"term": {"attributes.events{}": 1}}} has always matched it through the field's terms. Queries therefore keep
 * reading that string as a literal term path and never come here; a selector is structural only where a value is read
 * out of the Variant column.
 *
 * <p>Instances are immutable and are compiled once per field, not once per document.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectPath {

    /** What one step of a path does. */
    public enum Kind {
        /** Select an object member by exact name. */
        KEY,
        /** Select every element of one array level. */
        ARRAY_ALL,
        /** Select one element of one array level. */
        ARRAY_INDEX
    }

    /**
     * One step. {@code key} is set only for {@link Kind#KEY}, {@code index} only for {@link Kind#ARRAY_INDEX}.
     */
    public record Step(Kind kind, String key, int index) {

        static Step key(String name) {
            return new Step(Kind.KEY, name, -1);
        }

        static Step all() {
            return new Step(Kind.ARRAY_ALL, null, -1);
        }

        static Step at(int index) {
            return new Step(Kind.ARRAY_INDEX, null, index);
        }
    }

    private final String source;
    private final List<Step> steps;
    private final boolean wildcarded;

    private FlatObjectPath(String source, List<Step> steps, boolean wildcarded) {
        this.source = source;
        this.steps = List.copyOf(steps);
        this.wildcarded = wildcarded;
    }

    /**
     * Compiles a path.
     *
     * @throws IllegalArgumentException if the path is empty or malformed, naming the offending position
     */
    public static FlatObjectPath compile(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("a flat_object path cannot be empty");
        }
        List<Step> steps = new ArrayList<>();
        boolean wildcarded = false;
        int at = 0;
        while (true) {
            at = parseSegment(path, at, steps);
            if (at == path.length()) {
                break;
            }
            // parseSegment stops on the separator, which must be a dot with a segment after it.
            if (path.charAt(at) != '.') {
                throw malformed(path, at, "expected '.' or a selector");
            }
            at++;
            if (at == path.length()) {
                throw malformed(path, at, "trailing '.'");
            }
        }
        for (Step step : steps) {
            if (step.kind() == Kind.ARRAY_ALL) {
                wildcarded = true;
                break;
            }
        }
        return new FlatObjectPath(path, steps, wildcarded);
    }

    /** Parses one segment starting at {@code from}; returns the index just past it. */
    private static int parseSegment(String path, int from, List<Step> steps) {
        int at = from;
        String name;
        if (at < path.length() && path.charAt(at) == '[') {
            at = parseQuotedName(path, at, steps);
        } else {
            int start = at;
            while (at < path.length() && path.charAt(at) != '.' && path.charAt(at) != '{' && path.charAt(at) != '[') {
                at++;
            }
            name = path.substring(start, at);
            if (name.isEmpty()) {
                throw malformed(path, start, "empty member name");
            }
            steps.add(Step.key(name));
        }
        // Zero or more selectors, each consuming one array level.
        while (at < path.length() && path.charAt(at) == '{') {
            at = parseSelector(path, at, steps);
        }
        return at;
    }

    /**
     * Parses {@code ['name']} or {@code ["name"]} starting at the bracket.
     *
     * <p>Either quote may be used, and inside the quotes {@code \\}, {@code \'} and {@code \"} stand for a backslash and the
     * two quotes. Between the two forms and the escapes, every JSON key is reachable -- including the empty one, which
     * {@code flat_object} accepts and which {@code ['']} names. A bare empty segment stays invalid, since {@code a..b} is a
     * typo rather than a reference to a key called {@code ""}.
     */
    private static int parseQuotedName(String path, int from, List<Step> steps) {
        int at = from + 1;
        if (at >= path.length()) {
            throw malformed(path, from, "unterminated '['");
        }
        char quote = path.charAt(at);
        if (quote != '\'' && quote != '"') {
            throw malformed(path, at, "a bracketed member name must be quoted, as ['name']");
        }
        StringBuilder name = new StringBuilder();
        int i = at + 1;
        while (true) {
            if (i >= path.length()) {
                throw malformed(path, at, "unterminated quoted member name");
            }
            char c = path.charAt(i);
            if (c == quote) {
                break;
            }
            if (c == '\\') {
                if (i + 1 >= path.length()) {
                    throw malformed(path, i, "trailing '\\' in a quoted member name");
                }
                char escaped = path.charAt(i + 1);
                if (escaped != '\\' && escaped != '\'' && escaped != '"') {
                    throw malformed(path, i, "only \\\\, \\' and \\\" are escapes in a quoted member name");
                }
                name.append(escaped);
                i += 2;
                continue;
            }
            name.append(c);
            i++;
        }
        if (i + 1 >= path.length() || path.charAt(i + 1) != ']') {
            throw malformed(path, i + 1, "expected ']' after a quoted member name");
        }
        steps.add(Step.key(name.toString()));
        return i + 2;
    }

    /** Parses one {@code {}}, {@code {*}} or {@code {N}} starting at the brace. */
    private static int parseSelector(String path, int from, List<Step> steps) {
        int close = path.indexOf('}', from + 1);
        if (close < 0) {
            throw malformed(path, from, "unterminated '{'");
        }
        String body = path.substring(from + 1, close);
        if (body.isEmpty() || body.equals("*")) {
            steps.add(Step.all());
        } else {
            // Deliberately strict: a leading '+', a leading zero beyond "0" itself, or whitespace is a typo rather than an
            // index, and accepting it would make two different paths mean the same thing.
            for (int i = 0; i < body.length(); i++) {
                if (body.charAt(i) < '0' || body.charAt(i) > '9') {
                    throw malformed(path, from + 1, "an array selector must be empty, '*', or a non-negative integer");
                }
            }
            if (body.length() > 1 && body.charAt(0) == '0') {
                throw malformed(path, from + 1, "an array index cannot have a leading zero");
            }
            try {
                steps.add(Step.at(Integer.parseInt(body)));
            } catch (NumberFormatException e) {
                throw malformed(path, from + 1, "array index out of range");
            }
        }
        return close + 1;
    }

    private static IllegalArgumentException malformed(String path, int at, String why) {
        return new IllegalArgumentException("cannot parse flat_object path [" + path + "] at offset " + at + ": " + why);
    }

    public List<Step> steps() {
        return steps;
    }

    /**
     * Whether any step selects a whole array level, which is what makes the result a list rather than a single value.
     */
    public boolean isWildcarded() {
        return wildcarded;
    }

    /** The path as written, for messages. */
    @Override
    public String toString() {
        return source;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FlatObjectPath path && source.equals(path.source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }
}
