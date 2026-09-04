/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

/**
 * Thrown when Variant bytes are malformed, truncated, or of an unsupported version.
 *
 * <p>A distinct exception type exists so that a corrupt blob surfaces as a recognisable error rather than as an
 * {@link ArrayIndexOutOfBoundsException} from somewhere deep in a decode. These values are read on the search hot path,
 * where an unchecked out-of-bounds failure is both harder to attribute and more likely to escape as a 500.
 *
 * @opensearch.internal
 */
public class VariantFormatException extends RuntimeException {

    public VariantFormatException(String message) {
        super(message);
    }
}
