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
 * <p>Mirrors the {@code type} argument of the design's {@code get(path, type)} accessor.
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
     * <p>Useful for reconstruction and for the type-fidelity comparison, where the point is precisely to observe what
     * each store hands back rather than to normalise it away.
     */
    RAW
}
