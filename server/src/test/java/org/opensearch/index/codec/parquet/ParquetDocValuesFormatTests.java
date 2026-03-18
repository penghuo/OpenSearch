/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Conformance tests for {@link ParquetDocValuesFormat}.
 * <p>
 * Extends Lucene's {@link BaseDocValuesFormatTestCase} which exercises every
 * {@code DocValues} type (NUMERIC, SORTED_NUMERIC, BINARY, SORTED, SORTED_SET)
 * through hundreds of randomised scenarios including merges, updates, missing
 * values, large value counts, and concurrent readers. Passing this suite proves
 * the Parquet-backed format is a drop-in replacement for any standard Lucene
 * doc-values implementation.
 *
 * @opensearch.experimental
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Parquet codec logs on purpose")
public class ParquetDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private final Codec codec = new Lucene104Codec() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return new ParquetDocValuesFormat();
        }
    };

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
