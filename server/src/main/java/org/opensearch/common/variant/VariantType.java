/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

/**
 * The logical type of a Variant value.
 *
 * <p>Coarser than the encoding's primitive type ids: {@code int8} through {@code int64} all report {@link #LONG} here.
 * The exact stored width is still available from {@link Variant#primitiveTypeId()}, which the type-fidelity comparison
 * needs in order to show what each store preserved.
 *
 * @opensearch.internal
 */
public enum VariantType {
    NULL,
    BOOLEAN,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    BINARY,
    OBJECT,
    ARRAY
}
