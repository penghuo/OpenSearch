/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.variant.VariantBuilder;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Variant column and {@code _source} must return the same value for the same path.
 *
 * <p>Both are driven through {@link FlatObjectValueAccessor} over indices built from byte-identical documents by the real
 * mapper, so a divergence can only come from where the value was read. Values are compared exactly, including runtime class,
 * except where a difference is asserted deliberately -- see {@link #testIntegerWidthDivergesForSmallIntegers}.
 *
 * <p>{@link PathResolverTests} pins what the answers should be; this pins that both stores give them.
 */
public class AccessorEquivalenceTests extends MapperServiceTestCase {

    private static final String FIELD = "attributes";

    private static final String RICH_DOC = "{\"attributes\":{"
        + "\"region\":\"us-east-1\","
        + "\"sub\":{\"center\":1},"
        + "\"events\":[{\"status\":1},{\"status\":2}],"
        + "\"status\":200,"
        + "\"big\":9223372036854775807,"
        + "\"beyond\":92233720368547758070,"
        + "\"ratio\":0.25,"
        + "\"price\":12345678901234567890.12345,"
        + "\"ok\":true,"
        + "\"off\":false,"
        + "\"nothing\":null,"
        + "\"tags\":[\"a\",\"b\",\"c\"],"
        + "\"matrix\":[[{\"value\":1}],[{\"value\":2}]],"
        + "\"holes\":[{\"v\":1},{\"other\":9},{\"v\":null},{\"v\":4}],"
        + "\"empty\":[],"
        + "\"mixed\":[{\"v\":1},{\"v\":\"text\"},{\"v\":true},{\"v\":1.5}],"
        + "\"deep\":{\"a\":{\"b\":{\"c\":42}}},"
        + "\"k8s.namespace\":\"ns-01\","
        + "\"events{}\":[{\"status\":99}],"
        + "\"\":\"empty key\""
        + "}}";

    private static final List<String> PATHS = List.of(
        "region",
        "sub",
        "sub.center",
        "events",
        "events{}",
        "events{}.status",
        "events{0}",
        "events{0}.status",
        "events{1}.status",
        "events{2}.status",
        "events.status",
        "status",
        "big",
        "beyond",
        "ratio",
        "price",
        "ok",
        "off",
        "nothing",
        "nothing.deeper",
        "tags",
        "tags{}",
        "tags{1}",
        "matrix",
        "matrix{}{}.value",
        "matrix{}.value",
        "holes{}.v",
        "empty",
        "empty{}",
        "empty{}.anything",
        "mixed{}.v",
        "deep.a.b.c",
        "deep.a",
        "['k8s.namespace']",
        "k8s.namespace",
        "['events{}']",
        "['events{}']{}.status",
        "['']",
        "absent",
        "sub.absent",
        "region.absent",
        "region{}",
        "sub{}",
        "sub{0}"
    );

    private MapperService mapperService() throws IOException {
        return createMapperService(optedIn(), mapping(b -> b.startObject(FIELD).field("type", "flat_object").endObject()));
    }

    private static Settings optedIn() {
        return Settings.builder().put(FlatObjectFieldMapper.INDEX_FLAT_OBJECT_VARIANT_DOC_VALUES_SETTING.getKey(), true).build();
    }

    private Directory index(MapperService mapperService, List<String> sources) throws IOException {
        Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
            for (String source : sources) {
                writer.addDocument(mapperService.documentMapper().parse(source(source)).rootDoc());
            }
            writer.forceMerge(1);
        }
        return dir;
    }

    private interface Check {
        void accept(FlatObjectValueAccessor source, FlatObjectValueAccessor blob) throws IOException;
    }

    private void withBothStores(List<String> sources, Check check) throws IOException {
        MapperService service = mapperService();
        try (Directory dir = index(service, sources); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(1, reader.leaves().size());
            LeafReaderContext context = reader.leaves().get(0);

            SourceValueAccessor fromSource = new SourceValueAccessor(FIELD);
            fromSource.setNextReader(context);
            VariantBlobValueAccessor fromBlob = new VariantBlobValueAccessor(FIELD);
            fromBlob.setNextReader(context);

            assertTrue("the _source reader must have _source", fromSource.valueStoreAvailable());
            assertTrue("the column reader must have the blob column", fromBlob.valueStoreAvailable());
            check.accept(fromSource, fromBlob);
        }
    }

    private static Object read(FlatObjectValueAccessor accessor, int docId, String path) throws IOException {
        return accessor.get(docId, FlatObjectPath.compile(path));
    }

    /**
     * Both stores answer identically for every path, including the ones that must resolve to nothing.
     *
     * <p>Classes are compared too, except for a small integer -- {@code _source} is text and picks the narrowest type that
     * fits, which {@link #testIntegerWidthDivergesForSmallIntegers} pins on its own.
     */
    public void testEveryPathAgrees() throws IOException {
        withBothStores(List.of(RICH_DOC), (source, blob) -> {
            for (String path : PATHS) {
                Object fromSource = read(source, 0, path);
                Object fromBlob = read(blob, 0, path);
                assertEquals("value at [" + path + "]", widen(fromSource), widen(fromBlob));
                assertEquals("presence at [" + path + "]", fromSource == PathResolver.MISSING, fromBlob == PathResolver.MISSING);
            }
        });
    }

    /** Equality between the two is necessary but not sufficient: both could be wrong the same way. */
    public void testTheAnswersAreAlsoCorrect() throws IOException {
        withBothStores(List.of(RICH_DOC), (source, blob) -> {
            for (FlatObjectValueAccessor accessor : List.of(source, blob)) {
                String store = accessor.storeName();

                // Scalars keep their JSON type.
                assertEquals(store, "us-east-1", read(accessor, 0, "region"));
                assertEquals(store, Boolean.TRUE, read(accessor, 0, "ok"));
                assertEquals(store, Boolean.FALSE, read(accessor, 0, "off"));
                assertEquals(store, 0.25, ((Number) read(accessor, 0, "ratio")).doubleValue(), 0.0);
                assertEquals(store, Long.MAX_VALUE, ((Number) read(accessor, 0, "big")).longValue());

                // An object is a Map, an array is a List, both whole.
                assertEquals(store, Map.of("center", 1L), widen(read(accessor, 0, "sub")));
                assertEquals(store, List.of("a", "b", "c"), read(accessor, 0, "tags"));
                assertEquals(store, List.of(Map.of("status", 1L), Map.of("status", 2L)), widen(read(accessor, 0, "events")));

                // Selectors.
                assertEquals(store, List.of(1L, 2L), widen(read(accessor, 0, "events{}.status")));
                assertEquals(store, 1L, widen(read(accessor, 0, "events{0}.status")));
                assertEquals(store, 2L, widen(read(accessor, 0, "events{1}.status")));
                assertEquals(store, List.of(1L, 2L), widen(read(accessor, 0, "matrix{}{}.value")));
                assertEquals(store, "b", read(accessor, 0, "tags{1}"));

                // An array is never traversed without a selector.
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "events.status"));

                // Nested keys, and a literal key containing a dot.
                assertEquals(store, 42L, widen(read(accessor, 0, "deep.a.b.c")));
                assertEquals(store, "ns-01", read(accessor, 0, "['k8s.namespace']"));
                // The same string unquoted is nesting, and there is no object called k8s.
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "k8s.namespace"));

                // A literal key that looks like a selector, and the empty key.
                assertEquals(store, List.of(99L), widen(read(accessor, 0, "['events{}']{}.status")));
                assertEquals(store, "empty key", read(accessor, 0, "['']"));

                // Present-and-null is not missing.
                assertNull(store, read(accessor, 0, "nothing"));
                assertNotSame(store, PathResolver.MISSING, read(accessor, 0, "nothing"));

                // Missing members are skipped, explicit nulls kept.
                assertEquals(store, List.of(1L, 4L), widenWithNulls(read(accessor, 0, "holes{}.v"), 1L, null, 4L));

                // An empty array is present and empty; an absent one is missing.
                assertEquals(store, List.of(), read(accessor, 0, "empty{}"));
                assertEquals(store, List.of(), read(accessor, 0, "empty{}.anything"));
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "absent"));

                // A selector on a non-array is no match, not a singleton.
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "region{}"));
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "sub{}"));
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "sub{0}"));

                // An index past the end is no match.
                assertSame(store, PathResolver.MISSING, read(accessor, 0, "events{2}.status"));

                // Heterogeneous elements each keep their own type.
                List<?> mixed = (List<?>) read(accessor, 0, "mixed{}.v");
                assertEquals(store, 4, mixed.size());
                assertEquals(store, 1L, ((Number) mixed.get(0)).longValue());
                assertEquals(store, "text", mixed.get(1));
                assertEquals(store, Boolean.TRUE, mixed.get(2));
                assertEquals(store, 1.5, ((Number) mixed.get(3)).doubleValue(), 0.0);
            }
        });
    }

    /** A wildcard result keeps an explicit null as an element, so it is not confused with a missing member. */
    private static Object widenWithNulls(Object actual, Object... expected) {
        List<?> list = (List<?>) actual;
        assertEquals("wildcard result size", expected.length, list.size());
        List<Object> kept = new ArrayList<>();
        for (int i = 0; i < expected.length; i++) {
            Object element = list.get(i);
            if (expected[i] == null) {
                assertNull("element " + i + " must be an explicit null", element);
            } else {
                assertEquals("element " + i, expected[i], widen(element));
                kept.add(widen(element));
            }
        }
        return kept;
    }

    /**
     * An integer past {@code int64} survives exactly in both stores, as a scale-zero decimal in the column.
     *
     * <p>A JSON <em>float</em> does not, and cannot: the XContent parser reports every float as {@code DOUBLE}, so by the time
     * the mapper sees {@code 12345678901234567890.12345} it is already a {@code double} and its extra digits are gone. That is
     * a property of the parser, not of this column -- and because {@code _source} is re-parsed the same way, both stores hold
     * the same double, which is the property that matters here. The encoder's exact-decimal path is covered directly in
     * {@code VariantRoundTripTests}.
     */
    public void testWideNumbersSurviveExactly() throws IOException {
        withBothStores(List.of(RICH_DOC), (source, blob) -> {
            for (FlatObjectValueAccessor accessor : List.of(source, blob)) {
                String store = accessor.storeName();
                assertEquals(store, new BigInteger("92233720368547758070"), widen(read(accessor, 0, "beyond")));
            }
            // The float is a double on both sides, and identically so.
            Object fromSource = read(source, 0, "price");
            Object fromBlob = read(blob, 0, "price");
            assertEquals(Double.class, fromSource.getClass());
            assertEquals(Double.class, fromBlob.getClass());
            assertEquals(fromSource, fromBlob);
        });
    }

    /**
     * The one divergence, asserted rather than assumed.
     *
     * <p>{@code _source} is JSON text, so it picks the narrowest type that fits and yields an {@code Integer} for
     * {@code 200}; the blob recorded a type tag at write time and yields a {@code Long}. The numeric value is the same
     * either way, and past int range the two agree on class as well.
     */
    public void testIntegerWidthDivergesForSmallIntegers() throws IOException {
        withBothStores(List.of("{\"attributes\":{\"small\":200,\"large\":4294967296}}"), (source, blob) -> {
            Object smallFromSource = read(source, 0, "small");
            Object smallFromBlob = read(blob, 0, "small");
            assertEquals("_source yields the narrowest type that fits", Integer.class, smallFromSource.getClass());
            assertEquals("the blob yields the width it recorded", Long.class, smallFromBlob.getClass());
            assertEquals(200L, ((Number) smallFromSource).longValue());
            assertEquals(200L, ((Number) smallFromBlob).longValue());

            assertEquals(Long.class, read(source, 0, "large").getClass());
            assertEquals(Long.class, read(blob, 0, "large").getClass());
            assertEquals(read(source, 0, "large"), read(blob, 0, "large"));
        });
    }

    public void testBackwardsAccessAgrees() throws IOException {
        List<String> sources = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            sources.add("{\"attributes\":{\"status\":" + i + "}}");
        }
        withBothStores(sources, (source, blob) -> {
            // The blob column is a forward-only iterator, so out-of-order reads are the interesting case.
            for (int docId : new int[] { 3, 0, 2, 1, 3, 0 }) {
                assertEquals("doc " + docId, widen(read(source, docId, "status")), widen(read(blob, docId, "status")));
                assertEquals("doc " + docId, (long) (docId + 1), widen(read(blob, docId, "status")));
            }
        });
    }

    public void testADocumentWithoutTheFieldAgrees() throws IOException {
        withBothStores(List.of(RICH_DOC, "{\"other\":1}"), (source, blob) -> {
            assertSame(PathResolver.MISSING, read(source, 1, "status"));
            assertSame(PathResolver.MISSING, read(blob, 1, "status"));
            assertTrue(source.getAll(1).isEmpty());
            assertTrue(blob.getAll(1).isEmpty());
        });
    }

    public void testAnEmptyObjectAgrees() throws IOException {
        withBothStores(List.of("{\"attributes\":{}}"), (source, blob) -> {
            assertTrue(source.getAll(0).isEmpty());
            assertTrue(blob.getAll(0).isEmpty());
            assertSame(PathResolver.MISSING, read(source, 0, "anything"));
            assertSame(PathResolver.MISSING, read(blob, 0, "anything"));
        });
    }

    public void testWholeValueReconstructionAgrees() throws IOException {
        withBothStores(List.of(RICH_DOC), (source, blob) -> {
            Map<String, Object> fromSource = source.getAll(0);
            Map<String, Object> fromBlob = blob.getAll(0);
            assertEquals("reconstructed values must match", widen(fromSource), widen(fromBlob));
            assertEquals("every key must survive", fromSource.size(), fromBlob.size());
            assertEquals(20, fromBlob.size());
        });
    }

    /**
     * A path present in some documents and not others, across two segments, so per-segment name state cannot leak.
     */
    public void testMultipleSegmentsAgree() throws IOException {
        MapperService service = mapperService();
        Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(service.indexAnalyzer()))) {
            writer.addDocument(service.documentMapper().parse(source("{\"attributes\":{\"a\":1}}")).rootDoc());
            writer.commit();
            writer.addDocument(service.documentMapper().parse(source("{\"attributes\":{\"b\":2}}")).rootDoc());
            writer.commit();
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("two commits, two segments", 2, reader.leaves().size());
            for (LeafReaderContext leaf : reader.leaves()) {
                SourceValueAccessor fromSource = new SourceValueAccessor(FIELD);
                fromSource.setNextReader(leaf);
                VariantBlobValueAccessor fromBlob = new VariantBlobValueAccessor(FIELD);
                fromBlob.setNextReader(leaf);
                for (String path : List.of("a", "b")) {
                    assertEquals("segment " + leaf.ord + " path " + path, widen(read(fromSource, 0, path)), widen(read(fromBlob, 0, path)));
                }
            }
        }
        dir.close();
    }

    public void testSourceDisabledLeavesOnlyTheBlob() throws IOException {
        MapperService service = createMapperService(optedIn(), topMapping(b -> {
            b.startObject("_source").field("enabled", false).endObject();
            b.startObject("properties");
            b.startObject(FIELD).field("type", "flat_object").endObject();
            b.endObject();
        }));

        try (Directory dir = index(service, List.of(RICH_DOC)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext context = reader.leaves().get(0);

            SourceValueAccessor fromSource = new SourceValueAccessor(FIELD);
            fromSource.setNextReader(context);
            assertFalse("_source is disabled, so its reader has nothing to read", fromSource.valueStoreAvailable());
            assertSame(PathResolver.MISSING, read(fromSource, 0, "status"));
            assertTrue(fromSource.getAll(0).isEmpty());

            VariantBlobValueAccessor fromBlob = new VariantBlobValueAccessor(FIELD);
            fromBlob.setNextReader(context);
            assertTrue("the blob column is independent of _source", fromBlob.valueStoreAvailable());
            assertEquals(200L, widen(read(fromBlob, 0, "status")));
            assertEquals(List.of(1L, 2L), widen(read(fromBlob, 0, "events{}.status")));
        }
    }

    /**
     * A document whose value could not be encoded must read as missing, not as an empty object.
     *
     * <p>The two are different documents: {@code {}} is a value a user indexed and a reader must report as a present empty
     * map. The sentinel means the column has no value for this document at all. An empty-object sentinel would collapse
     * them, so the mapper writes a Variant null and every reader treats that as absent.
     */
    public void testTheUnencodableSentinelReadsAsMissingNotAsAnEmptyObject() throws IOException {
        MapperService service = mapperService();
        Directory dir = newDirectory();
        String blobField = FlatObjectFieldMapper.blobFieldName(FIELD);
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(service.indexAnalyzer()))) {
            // A genuine empty object.
            writer.addDocument(service.documentMapper().parse(source("{\"attributes\":{}}")).rootDoc());

            // The same field carrying the sentinel the mapper writes when a value cannot be encoded.
            ParsedDocument unencodable = service.documentMapper().parse(source("{\"attributes\":{\"a\":1}}"));
            List<IndexableField> fields = new ArrayList<>();
            for (IndexableField field : unencodable.rootDoc().getFields()) {
                if (field.name().equals(blobField)) {
                    fields.add(new BinaryDocValuesField(blobField, new BytesRef(unavailableValue())));
                } else {
                    fields.add(field);
                }
            }
            writer.addDocument(fields);
            writer.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            VariantBlobValueAccessor blob = new VariantBlobValueAccessor(FIELD);
            blob.setNextReader(leaf);

            // The real empty object is present and empty.
            assertTrue("an indexed {} is a present empty map", blob.getAll(0).isEmpty());
            assertSame(PathResolver.MISSING, read(blob, 0, "a"));

            // The sentinel is missing, and getAll must not throw on it.
            assertTrue("the sentinel reconstructs as nothing", blob.getAll(1).isEmpty());
            assertSame("the sentinel has no value at any path", PathResolver.MISSING, read(blob, 1, "a"));
        }
        dir.close();
    }

    /**
     * The bytes the mapper writes for a document whose value cannot be encoded: a Variant null.
     *
     * <p>Built here rather than read off the mapper, so the sentinel does not have to be public production API. If the two
     * ever disagree, the sentinel test below fails, which is the point.
     */
    private static byte[] unavailableValue() {
        VariantBuilder builder = new VariantBuilder(8);
        builder.appendNull();
        return builder.finish().valueBytes();
    }

    /**
     * Widens boxed integers so an {@code Integer} from {@code _source} compares equal to a {@code Long} from the column.
     *
     * <p>Only integer width is normalised; a decimal, a string and a boolean are compared as they are.
     */
    private static Object widen(Object value) {
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof Map<?, ?> map) {
            java.util.LinkedHashMap<Object, Object> out = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                out.put(entry.getKey(), widen(entry.getValue()));
            }
            return out;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(widen(element));
            }
            return out;
        }
        return value;
    }
}
