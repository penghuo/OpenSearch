/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.parquet;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for exporting index doc_values as standalone Parquet files.
 * Requires a filesystem path where the Parquet files will be written.
 */
public class ParquetExportRequest extends BroadcastRequest<ParquetExportRequest> {

    private String path;

    public ParquetExportRequest(StreamInput in) throws IOException {
        super(in);
        this.path = in.readString();
    }

    public ParquetExportRequest(String... indices) {
        super(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (path == null || path.isEmpty()) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationError("path is required");
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(path);
    }

    public String getPath() {
        return path;
    }

    public ParquetExportRequest path(String path) {
        this.path = path;
        return this;
    }
}
