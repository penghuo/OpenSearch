/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.parquet;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.IndexService;
import org.opensearch.index.codec.parquet.ParquetFileExporter;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Transport action that executes the Parquet export on each shard.
 * For each primary shard, reads doc_values and writes a standalone Parquet file
 * to the specified filesystem path.
 */
public class TransportParquetExportAction extends TransportBroadcastByNodeAction<
    ParquetExportRequest,
    ParquetExportResponse,
    TransportParquetExportAction.ShardExportResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportParquetExportAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ParquetExportAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ParquetExportRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    @Override
    protected ShardExportResult readShardResult(StreamInput in) throws IOException {
        return new ShardExportResult(in);
    }

    @Override
    protected ParquetExportResponse newResponse(
        ParquetExportRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardExportResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        int fileCount = 0;
        long totalBytes = 0;
        for (ShardExportResult result : results) {
            if (result.bytesWritten > 0) {
                fileCount++;
                totalBytes += result.bytesWritten;
            }
        }
        return new ParquetExportResponse(totalShards, successfulShards, failedShards, shardFailures, fileCount, totalBytes);
    }

    @Override
    protected ParquetExportRequest readRequestFrom(StreamInput in) throws IOException {
        return new ParquetExportRequest(in);
    }

    @Override
    protected ShardExportResult shardOperation(ParquetExportRequest request, ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

        String indexName = shardRouting.shardId().getIndex().getName();
        int shardId = shardRouting.shardId().id();
        Path outputFile = Paths.get(request.getPath(), indexName, "shard_" + shardId + ".parquet");

        try (var searcher = indexShard.acquireSearcher("parquet_export")) {
            ParquetFileExporter.ExportResult result = ParquetFileExporter.export(searcher, outputFile);
            return new ShardExportResult(result.totalBytes);
        }
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, ParquetExportRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShardsSatisfyingPredicate(concreteIndices, ShardRouting::primary);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ParquetExportRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ParquetExportRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    /**
     * Per-shard result holding the number of bytes written.
     */
    public static class ShardExportResult implements Writeable {
        final long bytesWritten;

        ShardExportResult(long bytesWritten) {
            this.bytesWritten = bytesWritten;
        }

        ShardExportResult(StreamInput in) throws IOException {
            this.bytesWritten = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(bytesWritten);
        }
    }
}
