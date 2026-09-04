/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.opensearch.common.annotation.InternalApi;

/**
 * Thrown when Variant bytes are malformed, truncated, or of an unsupported version.
 *
 * @opensearch.internal
 */
@InternalApi
public class VariantFormatException extends RuntimeException {

    public VariantFormatException(String message) {
        super(message);
    }
}
