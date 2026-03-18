/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.parquet;

import org.opensearch.action.ActionType;

/**
 * Action type for exporting index doc_values as standalone Parquet files.
 */
public class ParquetExportAction extends ActionType<ParquetExportResponse> {

    public static final ParquetExportAction INSTANCE = new ParquetExportAction();
    public static final String NAME = "indices:admin/parquet_export";

    private ParquetExportAction() {
        super(NAME, ParquetExportResponse::new);
    }
}
