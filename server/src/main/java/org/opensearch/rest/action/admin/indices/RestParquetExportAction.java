/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.parquet.ParquetExportAction;
import org.opensearch.action.admin.indices.parquet.ParquetExportRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for the _parquet_export endpoint.
 * Exports index doc_values as standalone Parquet files to a specified filesystem path.
 *
 * Usage: POST /my-index/_parquet_export?path=/shared/export/
 */
public class RestParquetExportAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/{index}/_parquet_export"));
    }

    @Override
    public String getName() {
        return "parquet_export_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String path = request.param("path");
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path parameter is required");
        }

        ParquetExportRequest exportRequest = new ParquetExportRequest(indices);
        exportRequest.path(path);
        exportRequest.indicesOptions(IndicesOptions.fromRequest(request, exportRequest.indicesOptions()));

        return channel -> client.execute(ParquetExportAction.INSTANCE, exportRequest, new RestToXContentListener<>(channel));
    }
}
