/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.flatobject;

import org.opensearch.test.flatobject.OtelDocGenerator;

/**
 * The index configurations under comparison.
 *
 * <p>The mappings below are identical apart from the single thing being studied. In particular both arms map
 * {@code attributes} as {@code flat_object} with the same options, so the inverted index and therefore all filtering is
 * the same in both; and both declare {@code dynamic: false} so {@code severity} and {@code body} live in {@code _source}
 * without contributing index structures. That keeps {@code body} useful as a way to grow the document (which is what the
 * read-granularity experiment needs) without it also growing the terms.
 */
enum BenchArm {

    /** Read the value back out of {@code _source} -- the only way to aggregate a {@code flat_object} subfield today. */
    A("a", false, true),

    /**
     * Read it from the Variant blob columns instead, with the field excluded from {@code _source}.
     *
     * <p>Excluded rather than kept in both, because once the value is in a type-preserving column a second copy as text is
     * redundant; charging this arm for both would overstate its cost. The remaining fields stay in {@code _source}.
     */
    VARIANT_ENC("variant-enc", true, true),

    /**
     * Read it from the doc-values column {@code flat_object} already writes.
     *
     * <p>{@code doc['attributes._valueAndPath']} hands a script every {@code path=value} pair in the document, so it can
     * prefix-match the one it wants. Asking for {@code doc['attributes.status']} does <em>not</em> narrow to that key --
     * it returns the whole column -- so the scan is unavoidable, and the value arrives as text.
     *
     * <p>The mapping is identical to {@link #A}: same {@code flat_object} options, {@code _source} enabled, same derived
     * field names and types. Only the script body differs. This arm exists because it is a second route that already
     * works today, which the comparison against {@code _source} alone overlooked.
     */
    DOC_VALUES("doc-values", false, true);

    private final String label;
    private final boolean variantBlob;
    private final boolean sourceEnabled;

    BenchArm(String label, boolean variantBlob, boolean sourceEnabled) {
        this.label = label;
        this.variantBlob = variantBlob;
        this.sourceEnabled = sourceEnabled;
    }

    String label() {
        return label;
    }

    boolean variantBlob() {
        return variantBlob;
    }

    boolean sourceEnabled() {
        return sourceEnabled;
    }

    static BenchArm fromLabel(String label) {
        for (BenchArm arm : values()) {
            if (arm.label.equals(label)) {
                return arm;
            }
        }
        StringBuilder known = new StringBuilder();
        for (BenchArm arm : values()) {
            known.append(known.length() == 0 ? "" : ", ").append(arm.label);
        }
        throw new IllegalArgumentException("unknown arm [" + label + "]; expected one of " + known);
    }

    /**
     * Builds the create-index body for this arm.
     *
     * <p>Settings are pinned rather than left to defaults: one shard and no replicas so measurements are not averaged
     * over shards or duplicated by replication, and refresh disabled during bulk so indexing throughput is not measuring
     * the refresh interval.
     */
    String createIndexBody(BenchProbes probes) {
        StringBuilder json = new StringBuilder();
        json.append("{\"settings\":{\"index\":{")
            .append("\"number_of_shards\":1,")
            .append("\"number_of_replicas\":0,")
            .append("\"refresh_interval\":\"-1\",")
            .append("\"translog\":{\"durability\":\"async\"}")
            .append("}},\"mappings\":{");

        if (sourceEnabled == false) {
            json.append("\"_source\":{\"enabled\":false},");
        } else if (this == VARIANT_ENC) {
            json.append("\"_source\":{\"excludes\":[\"").append(OtelDocGenerator.ATTRIBUTES_FIELD).append("\"]},");
        }

        json.append("\"dynamic\":false,\"properties\":{")
            .append("\"@timestamp\":{\"type\":\"date\"},")
            .append("\"")
            .append(OtelDocGenerator.ATTRIBUTES_FIELD)
            .append("\":{\"type\":\"flat_object\"");
        if (variantBlob) {
            json.append(",\"variant_blob\":true");
        }
        json.append("}}");

        json.append(",\"derived\":").append(derivedFields(probes));
        json.append("}}");
        return json.toString();
    }

    /**
     * The derived fields the end-to-end queries aggregate over.
     *
     * <p>Both arms expose the same field names and types, so the aggregation and the query plan are identical; only the
     * script body differs, and it differs precisely in which store it reads. That is what makes the end-to-end numbers a
     * comparison of stores rather than of queries.
     */
    private String derivedFields(BenchProbes probes) {
        StringBuilder json = new StringBuilder("{");
        json.append(longField("attr_status", probes.dense));
        json.append(',').append(keywordField("attr_namespace", probes.groupBy));
        if (probes.hasSparse()) {
            json.append(',').append(longField("attr_sparse", probes.sparse));
            json.append(',').append(longField("attr_rare", probes.rare));
        }
        if (this == DOC_VALUES) {
            // A _source-backed field on the same index, so one query pins that both routes read the same value and gives a
            // same-index reference point for the _source latency without re-running its whole query set.
            json.append(",\"attr_status_src\":{\"type\":\"long\",\"script\":{\"source\":\"")
                .append(escape(sourceLongScript(probes.dense)))
                .append("\",\"lang\":\"painless\"}}");
        }
        return json.append('}').toString();
    }

    private String longField(String name, String path) {
        return "\"" + name + "\":{\"type\":\"long\",\"script\":{\"source\":\"" + escape(longScript(path)) + "\",\"lang\":\"painless\"}}";
    }

    private String keywordField(String name, String path) {
        return "\""
            + name
            + "\":{\"type\":\"keyword\",\"script\":{\"source\":\""
            + escape(stringScript(path))
            + "\",\"lang\":\"painless\"}}";
    }

    /**
     * Both arms guard identically -- the column or the object may be absent, and the path may be absent from the document.
     * With a sparse key space the second guard is the common case, not the exception, so keeping the guard structure the
     * same in both arms matters: otherwise the scripts themselves, rather than the stores, would differ.
     */
    private String longScript(String path) {
        if (variantBlob) {
            return "def v = variant('attributes'); if (v != null) { def x = v.getLong('" + path + "'); if (x != null) { emit(x); } }";
        }
        if (this == DOC_VALUES) {
            // break on the first match, which is the most favourable reading of this route: the scan stops halfway on
            // average rather than always running the document's full key set.
            return "def vs = doc['"
                + OtelDocGenerator.ATTRIBUTES_FIELD
                + "._valueAndPath']; String p = '"
                + docValuePrefix(path)
                + "'; for (v in vs) { String s = String.valueOf(v); if (s.startsWith(p)) { "
                + "emit(Long.parseLong(s.substring(p.length()))); break; } }";
        }
        return sourceLongScript(path);
    }

    private String sourceLongScript(String path) {
        return "def a = params._source.attributes; if (a != null) { def x = a['"
            + path
            + "']; "
            + "if (x != null && x instanceof Number) { emit(((Number)x).longValue()); } }";
    }

    /**
     * The prefix a {@code path=value} entry carries in the {@code _valueAndPath} column.
     *
     * <p>Two {@code attributes.} segments, not one: the mapper prefixes the column with the field name and the path it
     * joins already begins with the field name. Verified against a written index rather than derived from the code.
     */
    private static String docValuePrefix(String path) {
        String field = OtelDocGenerator.ATTRIBUTES_FIELD;
        return field + "." + field + "." + path + "=";
    }

    private String stringScript(String path) {
        if (variantBlob) {
            return "def v = variant('attributes'); if (v != null) { def x = v.getString('" + path + "'); if (x != null) { emit(x); } }";
        }
        if (this == DOC_VALUES) {
            return "def vs = doc['"
                + OtelDocGenerator.ATTRIBUTES_FIELD
                + "._valueAndPath']; String p = '"
                + docValuePrefix(path)
                + "'; for (v in vs) { String s = String.valueOf(v); if (s.startsWith(p)) { "
                + "emit(s.substring(p.length())); break; } }";
        }
        return "def a = params._source.attributes; if (a != null) { def x = a['"
            + path
            + "']; "
            + "if (x != null) { emit(String.valueOf(x)); } }";
    }

    private static String escape(String script) {
        return script.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
