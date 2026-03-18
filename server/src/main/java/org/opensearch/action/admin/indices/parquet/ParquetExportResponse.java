/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.parquet;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for the Parquet export action, including the number of exported files and total bytes.
 */
public class ParquetExportResponse extends BroadcastResponse {

    private int fileCount;
    private long totalBytes;

    public ParquetExportResponse(StreamInput in) throws IOException {
        super(in);
        this.fileCount = in.readVInt();
        this.totalBytes = in.readVLong();
    }

    public ParquetExportResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        int fileCount,
        long totalBytes
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.fileCount = fileCount;
        this.totalBytes = totalBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(fileCount);
        out.writeVLong(totalBytes);
    }

    public int getFileCount() {
        return fileCount;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field("file_count", fileCount);
        builder.field("total_bytes", totalBytes);
    }
}
