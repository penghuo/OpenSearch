/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.variant.VariantFormatException;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.test.flatobject.CorpusConfig;
import org.opensearch.test.flatobject.OtelDocGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tests C1 and C2: Solution A and Solution B must return the same values for the same documents.
 *
 * <p>Both arms are driven through the same {@link FlatObjectValueAccessor} interface over indices built from byte-identical
 * documents by the real mapper, so a divergence can only come from the value store.
 *
 * <p>Typed reads are compared exactly, including the runtime class. Untyped {@code RAW} reads are compared numerically and
 * their class difference is asserted separately, because that difference <em>is</em> the type-fidelity finding rather than
 * a bug: see {@link #testRawIntegerWidthDivergence}.
 */
public class AccessorEquivalenceTests extends MapperServiceTestCase {

    private static final String FIELD = "attributes";

    /** Every value kind from C1.1, plus a literal dotted key and a deliberately deep path. */
    private static final String RICH_DOC = "{\"attributes\":{"
        + "\"status\":200,"
        + "\"big\":9223372036854775807,"
        + "\"ratio\":0.25,"
        + "\"level\":\"info\","
        + "\"ok\":true,"
        + "\"off\":false,"
        + "\"nothing\":null,"
        + "\"tags\":[\"a\",\"b\",\"c\"],"
        + "\"numbers\":[1,2,3],"
        + "\"nested\":{\"deep\":{\"value\":42}},"
        + "\"k8s.namespace\":\"ns-01\","
        + "\"numeric_string\":\"200\","
        + "\"leading_zero\":\"007\""
        + "}}";

    private static final List<String> PATHS = List.of(
        "status",
        "big",
        "ratio",
        "level",
        "ok",
        "off",
        "nothing",
        "tags",
        "numbers",
        "nested",
        "nested.deep",
        "nested.deep.value",
        "k8s.namespace",
        "numeric_string",
        "leading_zero",
        "absent",
        "nested.absent",
        "status.absent"
    );

    /**
     * Built with an explicit mapping rather than the {@code fieldMapping} helper, which would name the field {@code field}
     * and no longer match the documents below.
     */
    private MapperService mapperService(boolean variantBlob) throws IOException {
        // The parameter is gone: every flat_object writes the blob columns now. The boolean is kept so the A/B harness below
        // still reads as two arms, but both indices are byte-identical -- which is itself the point, since the _source reader
        // must be unaffected by the column's presence.
        return createMapperService(mapping(b -> b.startObject(FIELD).field("type", "flat_object").endObject()));
    }

    /**
     * Indexes documents through the real mapper into a single segment, so document ids follow insertion order.
     */
    private Directory index(MapperService mapperService, List<String> sources) throws IOException {
        Directory dir = newDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(mapperService.indexAnalyzer()))) {
            for (String source : sources) {
                ParsedDocument parsed = mapperService.documentMapper().parse(source(source));
                writer.addDocument(parsed.rootDoc());
            }
            writer.forceMerge(1);
        }
        return dir;
    }

    /**
     * Runs a check with both arms bound to equivalent indices over the same documents.
     */
    private void withBothArms(List<String> sources, CheckedBiConsumer check) throws IOException {
        MapperService serviceA = mapperService(false);
        MapperService serviceB = mapperService(true);
        try (
            Directory dirA = index(serviceA, sources);
            Directory dirB = index(serviceB, sources);
            DirectoryReader readerA = DirectoryReader.open(dirA);
            DirectoryReader readerB = DirectoryReader.open(dirB)
        ) {
            assertEquals(1, readerA.leaves().size());
            assertEquals(1, readerB.leaves().size());
            LeafReaderContext contextA = readerA.leaves().get(0);
            LeafReaderContext contextB = readerB.leaves().get(0);

            SourceValueAccessor accessorA = new SourceValueAccessor(FIELD);
            accessorA.setNextReader(contextA);
            VariantBlobValueAccessor accessorB = new VariantBlobValueAccessor(FIELD);
            accessorB.setNextReader(contextB);

            assertTrue("arm A must have _source", accessorA.valueStoreAvailable());
            assertTrue("arm B must have the blob column", accessorB.valueStoreAvailable());
            check.accept(accessorA, accessorB);
        }
    }

    private interface CheckedBiConsumer {
        void accept(FlatObjectValueAccessor a, FlatObjectValueAccessor b) throws IOException;
    }

    // ------------------------------------------------- the columnar read path

    /**
     * Drives the fielddata reader as a third arm: every value it yields for a path must be what the {@code _source} reader
     * yields for the same path, without a script anywhere.
     *
     * <p>This is the whole point of the phase. The two accessors agree by construction because they share path resolution
     * and coercion; the fielddata reader shares neither -- it resolves by field id and never sees a key name -- so agreement
     * here is real evidence rather than a tautology.
     */
    public void testFielddataAgreesWithSourceOnEveryPath() throws IOException {
        MapperService service = mapperService(true);
        try (Directory dir = index(service, List.of(RICH_DOC)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            SourceValueAccessor source = new SourceValueAccessor(FIELD);
            source.setNextReader(leaf);

            for (String path : PATHS) {
                // An array is where the two contracts legitimately part: the accessor's get() is single-valued and refuses a
                // container, while fielddata expands it the way a declared field does. Asserted separately in
                // testFielddataExpandsArraysAscending; here it only has to not be mistaken for a disagreement.
                boolean multiValued = source.get(0, path, ValueType.RAW) instanceof List;
                assertDoubleValues(service, leaf, source, path, multiValued);
                assertLongValues(service, leaf, source, path, multiValued);
                assertStringValues(service, leaf, source, path, multiValued);
            }
        }
    }

    /** An array at a path contributes every element, ascending, exactly as a declared numeric field would. */
    public void testFielddataExpandsArraysAscending() throws IOException {
        MapperService service = mapperService(true);
        List<String> docs = List.of(
            "{\"attributes\":{\"ports\":[443,80]}}",
            "{\"attributes\":{\"ports\":[[8080,22],1]}}",
            "{\"attributes\":{\"ports\":[80,{\"nested\":1},\"NaN\",8443]}}",
            "{\"attributes\":{\"ports\":7}}"
        );
        try (Directory dir = index(service, docs); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            SortedNumericDocValues values = longValues(service, leaf, "ports");

            assertEquals(List.of(80L, 443L), drainLongs(values, 0));
            assertEquals("nested arrays flatten, as they do for a declared field", List.of(1L, 22L, 8080L), drainLongs(values, 1));
            assertEquals("an object and a word are skipped, the numbers are kept", List.of(80L, 8443L), drainLongs(values, 2));
            assertEquals("a single value needs no array", List.of(7L), drainLongs(values, 3));
        }
    }

    /** A key absent from the whole segment must serve nothing, without decoding a document. */
    public void testFielddataSkipsASegmentThatCannotMatch() throws IOException {
        MapperService service = mapperService(true);
        try (Directory dir = index(service, List.of(RICH_DOC)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            SortedNumericDocValues values = longValues(service, leaf, "no_such_key_anywhere");
            assertFalse("a key no document has must yield no value", values.advanceExact(0));
        }
    }

    private List<Long> drainLongs(SortedNumericDocValues values, int docId) throws IOException {
        if (values.advanceExact(docId) == false) {
            return List.of();
        }
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < values.docValueCount(); i++) {
            out.add(values.nextValue());
        }
        return out;
    }

    private SortedNumericDocValues longValues(MapperService service, LeafReaderContext leaf, String path) {
        return fielddata(service, path).load(leaf).getLongValues();
    }

    private org.opensearch.index.fielddata.IndexNumericFieldData fielddata(MapperService service, String path) {
        MappedFieldType keyed = service.fieldType(FIELD + "." + path);
        assertNotNull("no field type for [" + path + "]", keyed);
        assertTrue("a keyed flat_object path must be aggregatable", keyed.isAggregatable());
        return (org.opensearch.index.fielddata.IndexNumericFieldData) keyed.fielddataBuilder("test", () -> null).build(null, null);
    }

    private void assertDoubleValues(
        MapperService service,
        LeafReaderContext leaf,
        SourceValueAccessor source,
        String path,
        boolean multiValued
    ) throws IOException {
        Object expected = source.get(0, path, ValueType.DOUBLE);
        SortedNumericDoubleValues values = fielddata(service, path).load(leaf).getDoubleValues();
        if (values.advanceExact(0) == false) {
            assertNull("fielddata has no double at [" + path + "] but _source does", expected);
            return;
        }
        if (multiValued) {
            // Every element readable as this type, and nothing to compare against a single-valued read.
            assertTrue("an expanded array must yield at least one double at [" + path + "]", values.docValueCount() >= 1);
            return;
        }
        assertNotNull("fielddata has a double at [" + path + "] but _source does not", expected);
        assertEquals("double count at [" + path + "]", 1, values.docValueCount());
        assertEquals("double at [" + path + "]", (Double) expected, values.nextValue(), 0.0);
    }

    private void assertLongValues(
        MapperService service,
        LeafReaderContext leaf,
        SourceValueAccessor source,
        String path,
        boolean multiValued
    ) throws IOException {
        Object expected = source.get(0, path, ValueType.LONG);
        SortedNumericDocValues values = fielddata(service, path).load(leaf).getLongValues();
        if (values.advanceExact(0) == false) {
            assertNull("fielddata has no long at [" + path + "] but _source does", expected);
            return;
        }
        if (multiValued) {
            // Every element readable as this type, and nothing to compare against a single-valued read.
            assertTrue("an expanded array must yield at least one long at [" + path + "]", values.docValueCount() >= 1);
            return;
        }
        assertNotNull("fielddata has a long at [" + path + "] but _source does not", expected);
        assertEquals("long count at [" + path + "]", 1, values.docValueCount());
        assertEquals("long at [" + path + "]", expected, values.nextValue());
    }

    private void assertStringValues(
        MapperService service,
        LeafReaderContext leaf,
        SourceValueAccessor source,
        String path,
        boolean multiValued
    ) throws IOException {
        Object expected = source.get(0, path, ValueType.STRING);
        SortedBinaryDocValues values = fielddata(service, path).load(leaf).getBytesValues();
        if (values.advanceExact(0) == false) {
            assertNull("fielddata has no string at [" + path + "] but _source does", expected);
            return;
        }
        if (multiValued) {
            // Every element readable as this type, and nothing to compare against a single-valued read.
            assertTrue("an expanded array must yield at least one string at [" + path + "]", values.docValueCount() >= 1);
            return;
        }
        assertNotNull("fielddata has a string at [" + path + "] but _source does not", expected);
        assertEquals("string count at [" + path + "]", 1, values.docValueCount());
        assertEquals("string at [" + path + "]", expected, values.nextValue().utf8ToString());
    }

    // ------------------------------------------------------------------- C1.1

    public void testTypedReadsAgreeForEveryPathAndType() throws IOException {
        withBothArms(List.of(RICH_DOC), (a, b) -> {
            for (String path : PATHS) {
                for (ValueType type : new ValueType[] { ValueType.LONG, ValueType.DOUBLE, ValueType.STRING, ValueType.BOOLEAN }) {
                    Object fromA = a.get(0, path, type);
                    Object fromB = b.get(0, path, type);
                    assertEquals("value at [" + path + "] as " + type, fromA, fromB);
                    if (fromA != null) {
                        assertEquals("class at [" + path + "] as " + type, fromA.getClass(), fromB.getClass());
                    }
                }
            }
            assertEquals("both arms must exclude the same values", a.coercionFailures(), b.coercionFailures());
            assertTrue("the mixed cases should have produced some exclusions", a.coercionFailures() > 0);
        });
    }

    public void testKnownValuesAreCorrectNotJustEqual() throws IOException {
        // Equality between the arms is necessary but not sufficient; both could be wrong the same way.
        withBothArms(List.of(RICH_DOC), (a, b) -> {
            for (FlatObjectValueAccessor accessor : List.of(a, b)) {
                String store = accessor.storeName();
                assertEquals(store, 200L, accessor.get(0, "status", ValueType.LONG));
                assertEquals(store, Long.MAX_VALUE, accessor.get(0, "big", ValueType.LONG));
                assertEquals(store, 0.25, accessor.get(0, "ratio", ValueType.DOUBLE));
                assertEquals(store, "info", accessor.get(0, "level", ValueType.STRING));
                assertEquals(store, Boolean.TRUE, accessor.get(0, "ok", ValueType.BOOLEAN));
                assertEquals(store, Boolean.FALSE, accessor.get(0, "off", ValueType.BOOLEAN));
                assertNull(store, accessor.get(0, "nothing", ValueType.STRING));
                assertEquals(store, 42L, accessor.get(0, "nested.deep.value", ValueType.LONG));
                assertEquals(store, "ns-01", accessor.get(0, "k8s.namespace", ValueType.STRING));
                assertEquals(store, "200", accessor.get(0, "numeric_string", ValueType.STRING));
                assertEquals(store, "007", accessor.get(0, "leading_zero", ValueType.STRING));
                assertNull(store, accessor.get(0, "absent", ValueType.STRING));
            }
        });
    }

    // ------------------------------------------------------------------- C1.2

    public void testWholeValueReconstructionAgrees() throws IOException {
        withBothArms(List.of(RICH_DOC), (a, b) -> {
            Map<String, Object> fromA = a.getAll(0);
            Map<String, Object> fromB = b.getAll(0);
            assertEquals("reconstructed values must match", widen(fromA), widen(fromB));
            assertEquals("all 13 keys must survive", 13, fromA.size());
            assertEquals(13, fromB.size());
        });
    }

    public void testReconstructionMatchesTheOriginalDocument() throws IOException {
        CorpusConfig config = CorpusConfig.builder("equiv").docCount(20).attrKeys(10).maxDepth(3).build();
        OtelDocGenerator generator = new OtelDocGenerator(config);
        List<String> sources = new ArrayList<>();
        for (int i = 0; i < config.docCount(); i++) {
            sources.add("{\"attributes\":" + toJson(generator.attributesAsMap(i)) + "}");
        }
        withBothArms(sources, (a, b) -> {
            for (int docId = 0; docId < sources.size(); docId++) {
                Object expected = widen(generator.attributesAsMap(docId));
                assertEquals("arm A doc " + docId, expected, widen(a.getAll(docId)));
                assertEquals("arm B doc " + docId, expected, widen(b.getAll(docId)));
            }
        });
    }

    // ------------------------------------------------- generated corpus sweep

    public void testGeneratedCorpusAgreesOnEveryStableKey() throws IOException {
        CorpusConfig config = CorpusConfig.builder("sweep").docCount(50).attrKeys(12).maxDepth(3).seed(randomLong()).build();
        OtelDocGenerator generator = new OtelDocGenerator(config);
        List<String> sources = new ArrayList<>();
        for (int i = 0; i < config.docCount(); i++) {
            sources.add("{\"attributes\":" + toJson(generator.attributesAsMap(i)) + "}");
        }
        withBothArms(sources, (a, b) -> {
            for (int docId = 0; docId < sources.size(); docId++) {
                for (String key : OtelDocGenerator.stableKeys()) {
                    for (ValueType type : ValueType.values()) {
                        Object fromA = a.get(docId, key, type);
                        Object fromB = b.get(docId, key, type);
                        if (type == ValueType.RAW) {
                            assertEquals("raw doc " + docId + " key " + key, widen(fromA), widen(fromB));
                        } else {
                            assertEquals("doc " + docId + " key " + key + " as " + type, fromA, fromB);
                        }
                    }
                }
            }
            assertEquals(a.coercionFailures(), b.coercionFailures());
        });
    }

    public void testBackwardsDocumentAccessAgrees() throws IOException {
        List<String> sources = List.of(
            "{\"attributes\":{\"status\":1}}",
            "{\"attributes\":{\"status\":2}}",
            "{\"attributes\":{\"status\":3}}",
            "{\"attributes\":{\"status\":4}}"
        );
        withBothArms(sources, (a, b) -> {
            // The blob column is a forward-only iterator, so out-of-order reads are the interesting case.
            for (int docId : new int[] { 3, 0, 2, 1, 3, 0 }) {
                assertEquals("doc " + docId, a.get(docId, "status", ValueType.LONG), b.get(docId, "status", ValueType.LONG));
                assertEquals("doc " + docId + " value", (long) (docId + 1), b.get(docId, "status", ValueType.LONG));
            }
        });
    }

    public void testDocumentWithoutTheFieldAgrees() throws IOException {
        List<String> sources = List.of(RICH_DOC, "{\"other\":1}");
        withBothArms(sources, (a, b) -> {
            assertNull(a.get(1, "status", ValueType.LONG));
            assertNull(b.get(1, "status", ValueType.LONG));
            assertTrue(a.getAll(1).isEmpty());
            assertTrue(b.getAll(1).isEmpty());
        });
    }

    public void testEmptyObjectAgrees() throws IOException {
        withBothArms(List.of("{\"attributes\":{}}"), (a, b) -> {
            assertTrue(a.getAll(0).isEmpty());
            assertTrue(b.getAll(0).isEmpty());
            assertNull(a.get(0, "anything", ValueType.STRING));
            assertNull(b.get(0, "anything", ValueType.STRING));
        });
    }

    // ------------------------------------------------------------------- C1.6

    public void testMixedTypePathAgrees() throws IOException {
        List<String> sources = List.of("{\"attributes\":{\"code\":200}}", "{\"attributes\":{\"code\":\"OK\"}}");
        withBothArms(sources, (a, b) -> {
            assertEquals(200L, a.get(0, "code", ValueType.LONG));
            assertEquals(200L, b.get(0, "code", ValueType.LONG));
            // "OK" is not a number in either arm, and both must count the exclusion.
            assertNull(a.get(1, "code", ValueType.LONG));
            assertNull(b.get(1, "code", ValueType.LONG));
            assertEquals("identical exclusion counts", a.coercionFailures(), b.coercionFailures());
            assertEquals(1L, a.coercionFailures());
            // Read as strings, both values are representable in both arms.
            assertEquals("200", a.get(0, "code", ValueType.STRING));
            assertEquals("200", b.get(0, "code", ValueType.STRING));
            assertEquals("OK", a.get(1, "code", ValueType.STRING));
            assertEquals("OK", b.get(1, "code", ValueType.STRING));
        });
    }

    // ------------------------------------------------------ C2: type fidelity

    /**
     * The type-fidelity finding, asserted rather than assumed.
     *
     * <p>The design predicts Solution A loses type information because {@code _source} is JSON text. What actually happens
     * is narrower than that: the numeric <em>value</em> survives in both arms, and only the integer <em>width</em>
     * differs. Reading {@code 200} back from {@code _source} yields an {@code Integer}, because nothing in the text says
     * otherwise, whereas the blob recorded a type tag at write time and yields a {@code Long}.
     *
     * <p>This is asserted as current behaviour so that any future change to it fails loudly, and so the results write-up
     * can cite a test rather than a claim.
     */
    public void testRawIntegerWidthDivergence() throws IOException {
        withBothArms(List.of("{\"attributes\":{\"small\":200,\"large\":4294967296}}"), (a, b) -> {
            Object smallA = a.get(0, "small", ValueType.RAW);
            Object smallB = b.get(0, "small", ValueType.RAW);
            assertEquals("_source yields the narrowest type that fits", Integer.class, smallA.getClass());
            assertEquals("the blob yields the width it recorded", Long.class, smallB.getClass());
            assertEquals("but the value is the same", 200L, ((Number) smallA).longValue());
            assertEquals(200L, ((Number) smallB).longValue());

            // Beyond int range there is nothing narrower to choose, so the arms agree even on class.
            Object largeA = a.get(0, "large", ValueType.RAW);
            Object largeB = b.get(0, "large", ValueType.RAW);
            assertEquals(Long.class, largeA.getClass());
            assertEquals(Long.class, largeB.getClass());
            assertEquals(largeA, largeB);

            // Asked for a type, both arms agree exactly. The divergence only exists for untyped reads.
            assertEquals(a.get(0, "small", ValueType.LONG), b.get(0, "small", ValueType.LONG));
        });
    }

    /**
     * C2.1: the three JSON spellings of two hundred, and what each arm makes of them.
     */
    public void testNumberSpellingsAgree() throws IOException {
        withBothArms(List.of("{\"attributes\":{\"i\":200,\"d\":200.0,\"e\":2e2,\"s\":\"200\"}}"), (a, b) -> {
            for (String path : List.of("i", "d", "e", "s")) {
                assertEquals("as long at [" + path + "]", a.get(0, path, ValueType.LONG), b.get(0, path, ValueType.LONG));
                assertEquals("as double at [" + path + "]", a.get(0, path, ValueType.DOUBLE), b.get(0, path, ValueType.DOUBLE));
                assertEquals("as string at [" + path + "]", a.get(0, path, ValueType.STRING), b.get(0, path, ValueType.STRING));
            }
            // 200.0 and 2e2 are the same double once parsed; neither arm can tell them apart, because the text does not.
            assertEquals(a.get(0, "d", ValueType.RAW), a.get(0, "e", ValueType.RAW));
            assertEquals(b.get(0, "d", ValueType.RAW), b.get(0, "e", ValueType.RAW));
            // The string stays distinct from the number in both arms, which is the type-conflict case that matters.
            assertEquals("200", a.get(0, "s", ValueType.RAW));
            assertEquals("200", b.get(0, "s", ValueType.RAW));
        });
    }

    /**
     * C2.2: extreme and awkward numeric values.
     */
    public void testNumericEdgeCasesAgree() throws IOException {
        String doc = "{\"attributes\":{"
            + "\"max\":9223372036854775807,"
            + "\"min\":-9223372036854775808,"
            + "\"beyond\":92233720368547758070,"
            + "\"negzero\":-0.0,"
            + "\"tiny\":4.9E-324,"
            + "\"huge\":1.7976931348623157E308"
            + "}}";
        withBothArms(List.of(doc), (a, b) -> {
            assertEquals(Long.MAX_VALUE, a.get(0, "max", ValueType.LONG));
            assertEquals(Long.MAX_VALUE, b.get(0, "max", ValueType.LONG));
            assertEquals(Long.MIN_VALUE, a.get(0, "min", ValueType.LONG));
            assertEquals(Long.MIN_VALUE, b.get(0, "min", ValueType.LONG));

            // Past int64 both arms hold the value exactly and both refuse to narrow it to a long.
            assertEquals(widen(a.get(0, "beyond", ValueType.RAW)), widen(b.get(0, "beyond", ValueType.RAW)));
            assertNull(a.get(0, "beyond", ValueType.LONG));
            assertNull(b.get(0, "beyond", ValueType.LONG));

            // Negative zero keeps its sign in both arms.
            double negZeroA = (Double) a.get(0, "negzero", ValueType.DOUBLE);
            double negZeroB = (Double) b.get(0, "negzero", ValueType.DOUBLE);
            assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(negZeroA));
            assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(negZeroB));

            assertEquals(a.get(0, "tiny", ValueType.DOUBLE), b.get(0, "tiny", ValueType.DOUBLE));
            assertEquals(a.get(0, "huge", ValueType.DOUBLE), b.get(0, "huge", ValueType.DOUBLE));
        });
    }

    // ------------------------------------------------------------------- C3.1

    /**
     * C3.1: with {@code _source} disabled the blob still answers and {@code _source} cannot.
     */
    public void testSourceDisabledLeavesOnlyTheBlob() throws IOException {
        MapperService service = createMapperService(topMapping(b -> {
            b.startObject("_source").field("enabled", false).endObject();
            b.startObject("properties");
            b.startObject(FIELD).field("type", "flat_object").endObject();
            b.endObject();
        }));

        try (Directory dir = index(service, List.of(RICH_DOC)); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext context = reader.leaves().get(0);

            SourceValueAccessor accessorA = new SourceValueAccessor(FIELD);
            accessorA.setNextReader(context);
            assertFalse("_source is disabled, so arm A has nothing to read", accessorA.valueStoreAvailable());
            assertNull(accessorA.get(0, "status", ValueType.LONG));
            assertTrue(accessorA.getAll(0).isEmpty());

            VariantBlobValueAccessor accessorB = new VariantBlobValueAccessor(FIELD);
            accessorB.setNextReader(context);
            assertTrue("the blob column is independent of _source", accessorB.valueStoreAvailable());
            assertEquals(200L, accessorB.get(0, "status", ValueType.LONG));
            assertEquals("ns-01", accessorB.get(0, "k8s.namespace", ValueType.STRING));
            assertEquals(13, accessorB.getAll(0).size());
        }
    }

    // ------------------------------------------------- partial-path retrieval

    /**
     * Selecting by a <em>prefix</em> of a path must return the whole subtree beneath it, not just leaves.
     *
     * <p>This is what {@code fields} retrieval on a {@code flat_object} needs: given
     * {@code attributes.resource.host} as a leaf, a request for {@code attributes.resource} should yield the object.
     * Both arms must agree, and arm B must be able to serve it from the blob alone — that is the prerequisite for ever
     * dropping the field from {@code _source}.
     */
    public void testPartialPathReturnsSubtree() throws IOException {
        String doc = "{\"attributes\":{"
            + "\"status\":200,"
            + "\"resource\":{\"host\":\"h1\",\"region\":\"us-west-2\",\"tags\":[\"a\",\"b\"]},"
            + "\"resource.direct\":\"literal\""
            + "}}";
        withBothArms(List.of(doc), (a, b) -> {
            // Full path to a leaf.
            assertEquals("h1", a.get(0, "resource.host", ValueType.STRING));
            assertEquals("h1", b.get(0, "resource.host", ValueType.STRING));
            assertEquals("us-west-2", b.get(0, "resource.region", ValueType.STRING));

            // Prefix of a path: the whole subtree.
            Object subtreeA = a.get(0, "resource", ValueType.RAW);
            Object subtreeB = b.get(0, "resource", ValueType.RAW);
            assertTrue("arm A returns an object for a prefix", subtreeA instanceof Map);
            assertTrue("arm B returns an object for a prefix", subtreeB instanceof Map);
            assertEquals(widen(subtreeA), widen(subtreeB));
            assertEquals(3, ((Map<?, ?>) subtreeB).size());
            assertEquals("h1", ((Map<?, ?>) subtreeB).get("host"));
            assertEquals(List.of("a", "b"), ((Map<?, ?>) subtreeB).get("tags"));

            // A nested array beneath the prefix.
            assertEquals(widen(a.get(0, "resource.tags", ValueType.RAW)), widen(b.get(0, "resource.tags", ValueType.RAW)));

            // The literal dotted key must still win over the nested reading of the same text.
            assertEquals("literal", a.get(0, "resource.direct", ValueType.STRING));
            assertEquals("literal", b.get(0, "resource.direct", ValueType.STRING));

            // Asking for a subtree as a scalar fails identically in both arms rather than returning nonsense.
            assertNull(a.get(0, "resource", ValueType.LONG));
            assertNull(b.get(0, "resource", ValueType.LONG));
            assertEquals(a.coercionFailures(), b.coercionFailures());
        });
    }

    // ------------------------------------------------ field-id relabelling

    /**
     * Documents whose keys deliberately do not sort in insertion order, at every nesting position that carries field ids.
     *
     * <p>Relabelling is a permutation applied to bytes already written, so the failure mode is not an exception -- it is
     * plausible values returned under the wrong keys. Any document whose keys happen to arrive already sorted would pass
     * with an identity permutation and prove nothing, so every object here is in reverse or scrambled order, including
     * objects nested inside other objects and inside arrays, whose ids are drawn from the same document-wide dictionary.
     */
    private static final List<String> UNSORTED_KEY_DOCS = List.of(
        RICH_DOC,
        "{\"attributes\":{\"zebra\":1,\"yak\":2,\"xray\":3,\"apple\":4}}",
        // A nested object first meets its keys after the outer ones, so its ids are the high end of the dictionary while
        // its names sort to the low end.
        "{\"attributes\":{\"zulu\":{\"delta\":1,\"charlie\":2,\"bravo\":3},\"alpha\":9}}",
        // Objects inside an array, sharing the document's dictionary with the object around them.
        "{\"attributes\":{\"rows\":[{\"zzz\":1,\"aaa\":2},{\"mmm\":3,\"bbb\":4}],\"middle\":5}}",
        // A key that is a prefix of another, where an off-by-one in the permutation is easiest to hide.
        "{\"attributes\":{\"statuses\":2,\"status\":1,\"stat\":0}}",
        // Non-ASCII, where sorting by UTF-8 bytes and sorting by UTF-16 code units disagree.
        "{\"attributes\":{\"￿high\":1,\"😀emoji\":2,\"plain\":3}}",
        "{\"attributes\":{\"only\":\"one\"}}",
        "{\"attributes\":{}}"
    );

    /**
     * Field ids ordered by name must read back exactly as {@code _source} does.
     *
     * <p>The risk this carries is a permutation bug. Field ids index the document's ordinal list, which indexes the
     * segment's names, so an off-by-one anywhere returns a plausible value under the wrong key rather than failing. Hence
     * values, whole-value reconstruction and coercion-failure counts are all compared, over documents whose keys
     * deliberately do not sort in insertion order.
     */
    public void testOrderedFieldIdsMatchSource() throws IOException {
        MapperService serviceA = mapperService(false);
        MapperService serviceB = mapperService(true);

        try (
            Directory dirA = index(serviceA, UNSORTED_KEY_DOCS);
            Directory dirB = index(serviceB, UNSORTED_KEY_DOCS);
            DirectoryReader readerA = DirectoryReader.open(dirA);
            DirectoryReader readerB = DirectoryReader.open(dirB)
        ) {
            SourceValueAccessor a = new SourceValueAccessor(FIELD);
            a.setNextReader(readerA.leaves().get(0));
            VariantBlobValueAccessor b = new VariantBlobValueAccessor(FIELD);
            b.setNextReader(readerB.leaves().get(0));

            List<String> paths = new ArrayList<>(PATHS);
            paths.addAll(
                List.of(
                    "zebra",
                    "yak",
                    "xray",
                    "apple",
                    "zulu",
                    "zulu.delta",
                    "zulu.charlie",
                    "zulu.bravo",
                    "alpha",
                    "rows",
                    "middle",
                    "statuses",
                    "status",
                    "stat",
                    "\uFFFFhigh",
                    "\uD83D\uDE00emoji",
                    "plain",
                    "only"
                )
            );

            for (int docId = 0; docId < UNSORTED_KEY_DOCS.size(); docId++) {
                for (String path : paths) {
                    for (ValueType type : new ValueType[] {
                        ValueType.LONG,
                        ValueType.DOUBLE,
                        ValueType.STRING,
                        ValueType.BOOLEAN,
                        ValueType.RAW }) {
                        String where = "doc " + docId + " path [" + path + "] as " + type;
                        assertEquals(where, widen(a.get(docId, path, type)), widen(b.get(docId, path, type)));
                    }
                }
                // Whole-value reconstruction catches a permutation that happens to be self-consistent for the paths probed.
                assertEquals("doc " + docId + " whole value", widen(a.getAll(docId)), widen(b.getAll(docId)));
            }
            assertEquals(a.coercionFailures(), b.coercionFailures());
        }
    }

    /**
     * Exactly two doc-values columns must be written, which is the entire storage claim.
     *
     * <p>Asserted from the written index rather than from the mapping, because the two are only equal if the write path does
     * what it says. A test that only compared values would pass just as well if a redundant third column were still being
     * written and merely ignored.
     */
    public void testOnlyTwoColumnsAreWritten() throws IOException {
        try (Directory dir = index(mapperService(true), UNSORTED_KEY_DOCS); DirectoryReader reader = DirectoryReader.open(dir)) {
            FieldInfos infos = reader.leaves().get(0).reader().getFieldInfos();
            assertNotNull("the value column", infos.fieldInfo(FIELD + "._blob"));
            assertNotNull("the name column", infos.fieldInfo(FIELD + "._blobnames"));
            assertNull("ordering the field ids makes a third column unnecessary", infos.fieldInfo(FIELD + "._blobmeta"));
        }
    }

    /**
     * A segment carrying the superseded metadata column must be refused rather than read.
     *
     * <p>Two earlier prototype layouts put a column under that name. Reading such a segment now would treat its field ids as
     * though they were already in name order, which returns values under the wrong keys and raises nothing -- the worst
     * failure available. Nothing writes that column any more, so the column is built by hand here.
     */
    public void testSegmentWithSupersededMetaColumnIsRefused() throws IOException {
        MapperService service = mapperService(true);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(service.indexAnalyzer()))) {
                ParsedDocument parsed = service.documentMapper().parse(source("{\"attributes\":{\"zebra\":1,\"apple\":2}}"));
                parsed.rootDoc().add(new BinaryDocValuesField(FIELD + "._blobmeta", new BytesRef(new byte[] { 1, 1, 0 })));
                writer.addDocument(parsed.rootDoc());
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                VariantBlobValueAccessor accessor = new VariantBlobValueAccessor(FIELD);
                VariantFormatException e = expectThrows(VariantFormatException.class, () -> accessor.setNextReader(reader.leaves().get(0)));
                assertTrue(e.getMessage(), e.getMessage().contains("_blobmeta"));
            }
        }
    }

    /**
     * Relabelled ids must be ascending within every object, which is what makes the reader's binary search valid.
     *
     * <p>The spec requires an object's field ids to be ordered by their key <em>strings</em>; ordering them by name across
     * the whole document additionally makes them numerically ascending. That is not decoration -- {@code objectGet} binary
     * searches by comparing resolved names, and with a name column those names come from the ordinal list in id order, so
     * the two orderings have to be the same one.
     */
    public void testRelabelledIdsAreAscendingWithinEachObject() throws IOException {
        MapperService service = mapperService(true);
        try (Directory dir = index(service, UNSORTED_KEY_DOCS); DirectoryReader reader = DirectoryReader.open(dir)) {
            BinaryDocValues blob = DocValues.getBinary(reader.leaves().get(0).reader(), FIELD + "._blob");
            SortedSetDocValues names = DocValues.getSortedSet(reader.leaves().get(0).reader(), FIELD + "._blobnames");
            for (int docId = 0; docId < UNSORTED_KEY_DOCS.size(); docId++) {
                assertTrue("doc " + docId + " must have a blob", blob.advanceExact(docId));
                BytesRef value = blob.binaryValue();
                int count = names.advanceExact(docId) ? names.docValueCount() : 0;
                for (int i = 0; i < count; i++) {
                    names.nextOrd();
                }
                assertAscendingFieldIds("doc " + docId, value.bytes, value.offset, value.offset + value.length);
            }
        }
    }

    /** Walks the value tree asserting every object's field ids are strictly ascending. */
    private static void assertAscendingFieldIds(String where, byte[] value, int pos, int end) {
        int basic = value[pos] & 0x03;
        int header = (value[pos] & 0xFF) >>> 2;
        if (basic == 2) { // object
            boolean isLarge = ((header >>> 4) & 0x01) != 0;
            int fieldIdSize = ((header >>> 2) & 0x03) + 1;
            int fieldOffsetSize = (header & 0x03) + 1;
            int at = pos + 1;
            int countWidth = isLarge ? 4 : 1;
            int numElements = readLE(value, at, countWidth);
            at += countWidth;
            int fieldIdsStart = at;
            int fieldOffsetsStart = fieldIdsStart + numElements * fieldIdSize;
            int valuesStart = fieldOffsetsStart + (numElements + 1) * fieldOffsetSize;
            int previous = -1;
            for (int i = 0; i < numElements; i++) {
                int fieldId = readLE(value, fieldIdsStart + i * fieldIdSize, fieldIdSize);
                assertTrue(where + ": field id " + fieldId + " at index " + i + " does not follow " + previous, fieldId > previous);
                previous = fieldId;
            }
            for (int i = 0; i < numElements; i++) {
                int offset = readLE(value, fieldOffsetsStart + i * fieldOffsetSize, fieldOffsetSize);
                assertAscendingFieldIds(where, value, valuesStart + offset, end);
            }
        } else if (basic == 3) { // array
            boolean isLarge = ((header >>> 2) & 0x01) != 0;
            int fieldOffsetSize = (header & 0x03) + 1;
            int at = pos + 1;
            int countWidth = isLarge ? 4 : 1;
            int numElements = readLE(value, at, countWidth);
            at += countWidth;
            int valuesStart = at + (numElements + 1) * fieldOffsetSize;
            for (int i = 0; i < numElements; i++) {
                assertAscendingFieldIds(where, value, valuesStart + readLE(value, at + i * fieldOffsetSize, fieldOffsetSize), end);
            }
        }
    }

    private static int readLE(byte[] source, int offset, int width) {
        int value = 0;
        for (int i = 0; i < width; i++) {
            value |= (source[offset + i] & 0xFF) << (8 * i);
        }
        return value;
    }

    /**
     * Documents too wide to patch in place must still get ordered field ids, with no extra column.
     *
     * <p>Relabelling patches ids at the width already written, which only works while every permuted id still fits. Above
     * that the writer re-encodes with the dictionary supplied in name order instead -- the way a Parquet writer produces
     * sorted ids -- so the result is the same layout with no key-count limit. Both sides of the boundary are exercised,
     * since an off-by-one there would silently truncate an id and return a wrong key rather than fail.
     */
    public void testWideDocumentsAreReencodedNotRanked() throws IOException {
        List<String> sources = new ArrayList<>();
        int[] keyCounts = { 255, 256, 257, 600, 1000 };
        for (int keys : keyCounts) {
            StringBuilder json = new StringBuilder("{\"attributes\":{");
            // Names formatted so lexicographic order is the reverse of insertion order, forcing a full permutation. One
            // nested object early on, which is the shape that makes in-place patching unsafe: it gets a narrow field-id
            // width from its low insertion-order ids, and its names can sort anywhere.
            for (int i = 0; i < keys; i++) {
                json.append(i > 0 ? "," : "").append('"').append('k').append(String.format(Locale.ROOT, "%04d", keys - i)).append('"');
                json.append(':');
                if (i == 1) {
                    json.append("{\"zz\":1,\"aa\":2}");
                } else {
                    json.append(i);
                }
            }
            sources.add(json.append("}}").toString());
        }

        MapperService serviceA = mapperService(false);
        MapperService serviceB = mapperService(true);
        try (
            Directory dirA = index(serviceA, sources);
            Directory dirB = index(serviceB, sources);
            DirectoryReader readerA = DirectoryReader.open(dirA);
            DirectoryReader readerB = DirectoryReader.open(dirB)
        ) {
            LeafReaderContext leafB = readerB.leaves().get(0);
            assertNull(
                "ordered field ids make the rank column unnecessary at every width",
                leafB.reader().getFieldInfos().fieldInfo(FIELD + "._blobmeta")
            );

            SourceValueAccessor a = new SourceValueAccessor(FIELD);
            a.setNextReader(readerA.leaves().get(0));
            VariantBlobValueAccessor b = new VariantBlobValueAccessor(FIELD);
            b.setNextReader(leafB);
            for (int docId = 0; docId < sources.size(); docId++) {
                for (int i = 0; i < keyCounts[docId]; i++) {
                    String path = "k" + String.format(Locale.ROOT, "%04d", keyCounts[docId] - i);
                    String where = "doc " + docId + " [" + path + "]";
                    if (i == 1) {
                        assertEquals(where + " nested", 1L, b.get(docId, path + ".zz", ValueType.LONG));
                        assertEquals(where + " nested", 2L, b.get(docId, path + ".aa", ValueType.LONG));
                    } else {
                        assertEquals(where, (long) i, b.get(docId, path, ValueType.LONG));
                    }
                }
                assertEquals("doc " + docId + " whole value", widen(a.getAll(docId)), widen(b.getAll(docId)));
            }
        }
    }

    /**
     * Re-encoding must not alter a single value, including the ones a round trip through a Java type would change.
     *
     * <p>Scalars are copied verbatim rather than re-appended precisely so that an {@code int16} holding a small number stays
     * {@code int16} and a decimal keeps its scale. Asserted against the narrow path, which does not re-encode at all, so any
     * difference is the re-encoding's fault.
     */
    public void testReencodingPreservesEveryValue() throws IOException {
        // Wide enough to force re-encoding, with the value kinds whose exact form a decode/encode round trip could lose.
        StringBuilder json = new StringBuilder("{\"attributes\":{");
        json.append("\"zzz_int16\":32767,\"zzz_big\":9223372036854775807,\"zzz_ratio\":0.25,");
        json.append("\"zzz_neg\":-128,\"zzz_bool\":true,\"zzz_null\":null,");
        json.append("\"zzz_arr\":[1,2,3],\"zzz_obj\":{\"b\":1,\"a\":2},\"zzz_str\":\"0071\"");
        for (int i = 0; i < 300; i++) {
            json.append(",\"k").append(String.format(Locale.ROOT, "%04d", 300 - i)).append("\":").append(i);
        }
        String wide = json.append("}}").toString();
        String narrow = wide;

        MapperService blob = mapperService(true);
        MapperService source = mapperService(false);
        try (
            Directory dirS = index(source, List.of(narrow));
            Directory dirB = index(blob, List.of(wide));
            DirectoryReader readerS = DirectoryReader.open(dirS);
            DirectoryReader readerB = DirectoryReader.open(dirB)
        ) {
            SourceValueAccessor s = new SourceValueAccessor(FIELD);
            s.setNextReader(readerS.leaves().get(0));
            VariantBlobValueAccessor b = new VariantBlobValueAccessor(FIELD);
            b.setNextReader(readerB.leaves().get(0));
            for (String path : List.of(
                "zzz_int16",
                "zzz_big",
                "zzz_ratio",
                "zzz_neg",
                "zzz_bool",
                "zzz_null",
                "zzz_arr",
                "zzz_obj",
                "zzz_obj.a",
                "zzz_obj.b",
                "zzz_str"
            )) {
                for (ValueType type : new ValueType[] { ValueType.LONG, ValueType.DOUBLE, ValueType.STRING, ValueType.RAW }) {
                    assertEquals("re-encoded [" + path + "] as " + type, widen(s.get(0, path, type)), widen(b.get(0, path, type)));
                }
            }
            assertEquals(widen(s.getAll(0)), widen(b.getAll(0)));
        }
    }

    /** Narrow and wide documents in one segment must both read, in either direction, though they took different paths. */
    public void testMixedNarrowAndWideDocumentsInOneSegment() throws IOException {
        StringBuilder wide = new StringBuilder("{\"attributes\":{");
        for (int i = 0; i < 300; i++) {
            wide.append(i > 0 ? "," : "").append("\"w").append(String.format(Locale.ROOT, "%04d", 300 - i)).append("\":").append(i);
        }
        wide.append("}}");

        List<String> sources = List.of(
            "{\"attributes\":{\"zebra\":1,\"apple\":2}}",
            wide.toString(),
            "{\"attributes\":{\"yak\":3,\"bee\":4}}"
        );
        withBothArms(sources, (a, b) -> {
            // Forwards.
            for (int docId = 0; docId < sources.size(); docId++) {
                assertEquals("doc " + docId, widen(a.getAll(docId)), widen(b.getAll(docId)));
            }
            // Backwards, which restarts the iterators and must not lose the per-document choice of form.
            for (int docId = sources.size() - 1; docId >= 0; docId--) {
                assertEquals("doc " + docId + " backwards", widen(a.getAll(docId)), widen(b.getAll(docId)));
            }
            assertEquals(1L, b.get(0, "zebra", ValueType.LONG));
            assertEquals(299L, b.get(1, "w0001", ValueType.LONG));
            assertEquals(4L, b.get(2, "bee", ValueType.LONG));
        });
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Recursively widens {@link Integer} to {@link Long}, so a comparison is about values rather than about which boxed
     * type each store happened to produce.
     */
    private static Object widen(Object value) {
        if (value instanceof Integer integer) {
            return integer.longValue();
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(k, widen(v)));
            return result;
        }
        if (value instanceof List<?> list) {
            List<Object> result = new ArrayList<>(list.size());
            list.forEach(element -> result.add(widen(element)));
            return result;
        }
        return value;
    }

    private static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder json = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (first == false) {
                    json.append(',');
                }
                first = false;
                json.append(quote(String.valueOf(entry.getKey()))).append(':').append(toJson(entry.getValue()));
            }
            return json.append('}').toString();
        }
        if (value instanceof List<?> list) {
            StringBuilder json = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append(toJson(list.get(i)));
            }
            return json.append(']').toString();
        }
        if (value instanceof String text) {
            return quote(text);
        }
        return String.valueOf(value);
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }
}
