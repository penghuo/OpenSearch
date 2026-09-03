/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

/**
 * Exposes {@code variant(field)} to derived field scripts.
 *
 * <p>Bound the same way {@link ScriptEmitValues} binds {@code emit}: painless constructs this with the executing script and
 * calls the instance method, which is what lets the script call {@code variant('attributes')} with no receiver.
 *
 * @opensearch.internal
 */
public final class ScriptVariantAccess {

    private final DerivedFieldScript derivedFieldScript;

    public ScriptVariantAccess(DerivedFieldScript derivedFieldScript) {
        this.derivedFieldScript = derivedFieldScript;
    }

    /**
     * @return an accessor for the field's Variant blob column, or {@code null} if the field has no such column
     */
    public VariantFieldAccess variant(String field) {
        return derivedFieldScript.variant(field);
    }
}
