/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

/**
 * The type a caller asks a {@link FlatObjectValueAccessor} to return a value as.
 *
 *
 * @opensearch.internal
 */
public enum ValueType {
    /** A 64-bit signed integer. */
    LONG,
    /** A 64-bit IEEE floating point value. */
    DOUBLE,
    /** A UTF-8 string. */
    STRING,
    /** A boolean. */
    BOOLEAN,
    /**
     * The value as stored, with no coercion.
     *
     * <p>For reconstruction, and for callers that need to see exactly what the store hands back rather than have it
     * normalised away.
     */
    RAW
}
