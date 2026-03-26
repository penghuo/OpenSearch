/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Wraps a {@link LeafReader} and provides access to the derived source.
 *
 * <p>This reader only intercepts stored field access ({@link #storedFields()} and
 * {@link #getSequentialStoredFieldsReader()}) to inject derived _source. All other
 * methods delegate directly to the wrapped reader, bypassing {@code FilterLeafReader}'s
 * virtual dispatch to enable JIT inlining on hot paths during query and aggregation phases.
 *
 * @opensearch.internal
 */
public class DerivedSourceLeafReader extends SequentialStoredFieldsLeafReader {

    private final CheckedFunction<Integer, BytesReference, IOException> sourceProvider;

    public DerivedSourceLeafReader(LeafReader in, CheckedFunction<Integer, BytesReference, IOException> sourceProvider) {
        super(in);
        this.sourceProvider = sourceProvider;
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return new DerivedSourceStoredFieldsReader.DerivedSourceStoredFields(in.storedFields(), sourceProvider);
    }

    @Override
    public StoredFieldsReader getSequentialStoredFieldsReader() throws IOException {
        if (in instanceof CodecReader) {
            final CodecReader reader = (CodecReader) in;
            final StoredFieldsReader sequentialReader = reader.getFieldsReader().getMergeInstance();
            return doGetSequentialStoredFieldsReader(new DerivedSourceStoredFieldsReader(sequentialReader, sourceProvider));
        } else if (in instanceof SequentialStoredFieldsLeafReader) {
            final SequentialStoredFieldsLeafReader reader = (SequentialStoredFieldsLeafReader) in;
            final StoredFieldsReader sequentialReader = reader.getSequentialStoredFieldsReader();
            return doGetSequentialStoredFieldsReader(new DerivedSourceStoredFieldsReader(sequentialReader, sourceProvider));
        } else {
            throw new IOException("requires a CodecReader or a SequentialStoredFieldsLeafReader, got " + in.getClass());
        }
    }

    // ----- Direct delegation overrides for hot-path methods -----
    // These bypass FilterLeafReader's virtual dispatch, allowing the JIT to inline
    // monomorphic calls at this concrete class level during query/scoring/aggregation.

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return in.getNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return in.getSortedDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return in.getSortedSetDocValues(field);
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return in.getNormValues(field);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        return in.getDocValuesSkipper(field);
    }

    @Override
    public Terms terms(String field) throws IOException {
        return in.terms(field);
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
        return in.getPointValues(field);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return in.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return in.getByteVectorValues(field);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return in.getFieldInfos();
    }

    @Override
    public Bits getLiveDocs() {
        return in.getLiveDocs();
    }

    @Override
    public int numDocs() {
        return in.numDocs();
    }

    @Override
    public int maxDoc() {
        return in.maxDoc();
    }
}
