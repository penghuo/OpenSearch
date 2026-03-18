/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.parquet;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A {@link DocValuesFormat} that stores doc values using Apache Parquet encoding.
 * <p>
 * Uses {@code .pdvd} for the data file and {@code .pdvm} for the metadata file.
 * Values are encoded with Parquet's columnar encoding (delta-binary-packed for
 * integers, dictionary + RLE for sorted types) via the parquet-column library,
 * while Lucene's {@code IndexInput}/{@code IndexOutput} handle all I/O.
 *
 * @opensearch.experimental
 */
public class ParquetDocValuesFormat extends DocValuesFormat {

    /** Format name used in codec SPI registration. */
    public static final String FORMAT_NAME = "Parquet";

    /** File extension for the data file containing encoded Parquet pages. */
    public static final String DATA_EXTENSION = "pdvd";

    /** File extension for the metadata file containing field descriptors and page offsets. */
    public static final String META_EXTENSION = "pdvm";

    /** Codec name written into the data file header/footer. */
    public static final String DATA_CODEC = "ParquetDocValuesData";

    /** Codec name written into the metadata file header/footer. */
    public static final String META_CODEC = "ParquetDocValuesMeta";

    /** No-arg constructor required for SPI discovery. */
    public ParquetDocValuesFormat() {
        super(FORMAT_NAME);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ParquetDocValuesWriter(state, DATA_EXTENSION, META_EXTENSION, DATA_CODEC, META_CODEC);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ParquetDocValuesReader(state, DATA_EXTENSION, META_EXTENSION, DATA_CODEC, META_CODEC);
    }
}
