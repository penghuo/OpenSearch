/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Definition of Script for DerivedField.
 * It will be used to execute scripts defined against derived fields of any type
 *
 * @opensearch.internal
 */
public abstract class DerivedFieldScript {

    public static final String[] PARAMETERS = {};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("derived_field", Factory.class);
    private static final int MAX_BYTE_SIZE = 1024 * 1024; // Maximum allowed byte size (1 MB)

    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
        "doc",
        value -> value,
        "_source",
        value -> ((SourceLookup) value).loadSourceIfNeeded()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    /**
     * The segment this script is bound to. Retained so a script can read doc-values columns directly, not only through
     * {@link #leafLookup}.
     */
    private final LeafReaderContext leafContext;

    /**
     * The search-wide lookup, retained so Variant blob accessors can be shared with every other script on this thread.
     */
    private final SearchLookup searchLookup;

    /**
     * The field values emitted from the script.
     */
    private List<Object> emittedValues;

    private int totalByteSize;

    private int currentDocId = -1;

    /**
     * The accessor this script uses, resolved once and then held directly.
     *
     * <p>Resolution goes through {@link SearchLookup} so the underlying accessor is shared with every other script on the
     * thread, but the <em>result</em> is cached here because {@code variant()} is called once per document per
     * aggregation. An earlier version resolved through the shared map on every call, which added a boxed
     * {@code ConcurrentHashMap} lookup to the hot path and cost a measured 21 ns per document. This mirrors how
     * {@link #leafLookup} is resolved once in the constructor rather than per document.
     */
    private VariantFieldAccess cachedAccess;
    private String cachedAccessField;

    public DerivedFieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        this.leafContext = leafContext;
        this.searchLookup = lookup;
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
        this.emittedValues = new ArrayList<>();
        this.totalByteSize = 0;
    }

    /**
     * Returns an accessor for a {@code flat_object} field's Variant blob column, positioned on the current document.
     *
     * @return the accessor, or {@code null} if the field has no blob column in this segment, so a script can guard with a
     *         null check rather than having to know the mapping
     */
    public VariantFieldAccess variant(String field) {
        // cachedAccessField starts null, so the first call always resolves. A null result (no blob column in this
        // segment) is cached too, otherwise the absent-column case would pay the shared-map lookup on every document.
        if (field.equals(cachedAccessField) == false) {
            cachedAccess = searchLookup.variantFieldAccess(field, leafContext);
            cachedAccessField = field;
        }
        VariantFieldAccess access = cachedAccess;
        if (access != null) {
            access.setDocument(currentDocId);
        }
        return access;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Return the emitted values from the script execution.
     */
    public List<Object> getEmittedValues() {
        return emittedValues;
    }

    /**
     * Set the current document to run the script on next.
     * Clears the emittedValues as well since they should be scoped per document.
     */
    public void setDocument(int docid) {
        this.emittedValues = new ArrayList<>();
        this.totalByteSize = 0;
        // Recorded so variant() can position its accessor on the same document the script is executing against.
        this.currentDocId = docid;
        leafLookup.setDocument(docid);
    }

    public void addEmittedValue(Object o) {
        int byteSize = getObjectByteSize(o);
        int newTotalByteSize = totalByteSize + byteSize;
        if (newTotalByteSize <= MAX_BYTE_SIZE) {
            emittedValues.add(o);
            totalByteSize = newTotalByteSize;
        } else {
            throw new IllegalStateException("Exceeded maximum allowed byte size for emitted values");
        }
    }

    private int getObjectByteSize(Object obj) {
        if (obj instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8).length;
        } else if (obj instanceof Integer) {
            return Integer.BYTES;
        } else if (obj instanceof Long) {
            return Long.BYTES;
        } else if (obj instanceof Double) {
            return Double.BYTES;
        } else if (obj instanceof Float) {
            return Float.BYTES;
        } else if (obj instanceof Boolean) {
            return Byte.BYTES; // Assuming 1 byte for boolean
        } else if (obj instanceof Tuple) {
            // Assuming each element in the tuple is a double for GeoPoint case
            return Double.BYTES * 2;
        } else if (obj == null) {
            return 0;
        } else {
            throw new IllegalArgumentException("Unsupported object type passed in emit() - " + obj);
        }
    }

    public void execute() {}

    /**
     * A factory to construct {@link DerivedFieldScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        DerivedFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link DerivedFieldScript} factories for a specific index.
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
