/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.variant;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.compress.LZ4;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.flatobject.CorpusConfig;
import org.opensearch.test.flatobject.OtelDocGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.Deflater;

/**
 * Audits the encoder against the Apache Parquet Variant specification, and accounts for where every byte of an encoded
 * document goes.
 *
 * <p>Written to settle a specific challenge: published figures put Variant at 22-31% smaller than the equivalent JSON
 * string, while the measured blob column came out only ~4% smaller. Either the encoder is not emitting the compact forms
 * the spec offers, or the published comparison is measuring a different layout. This test decides which, by (1) asserting
 * every container and scalar uses the narrowest encoding the spec permits, and (2) reporting the byte composition and what
 * the same bytes would cost under the layout Parquet actually uses.
 *
 * <p>The audit encodes through {@link VariantJson#encode}, which drives the same {@link VariantBuilder} calls the mapper's
 * own parse walk does, so the bytes counted here are the bytes the index holds.
 */
public class VariantEncodingAuditTests extends OpenSearchTestCase {

    /**
     * Documents sampled from the benchmark corpus. Large enough that the key-frequency distribution and the block
     * compression models both see a representative mix, small enough to run as a unit test.
     */
    private static final int SAMPLE_DOCS = 20_000;

    /** Lucene's stored-fields block size, so the compression model compresses the same span of documents `_source` does. */
    private static final int BLOCK_BYTES = 16 * 1024;

    // ------------------------------------------------------------------ part 1

    /**
     * Every value must use the narrowest form the spec offers: integers narrowed to int8/int16/int32/int64, strings under
     * 64 bytes folded into the header byte, and each container's field-id and field-offset widths the minimum that can
     * address it.
     *
     * <p>A non-minimal encoder would round-trip its own output perfectly, so nothing else in the suite would catch this.
     */
    public void testEveryValueUsesTheNarrowestSpecForm() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("E10M"));
        Tally tally = new Tally();
        for (int doc = 0; doc < SAMPLE_DOCS; doc++) {
            Variant variant = encode(attributesJson(generator, doc));
            walk(variant.valueArray(), variant.offset(), tally);
        }

        assertEquals("integers wider than necessary: " + tally.nonMinimalIntegers, 0, tally.nonMinimalIntegers);
        assertEquals("short strings encoded in long form: " + tally.nonMinimalStrings, 0, tally.nonMinimalStrings);
        assertEquals("containers with a wider field-id width than needed: " + tally.nonMinimalFieldIds, 0, tally.nonMinimalFieldIds);
        assertEquals("containers with a wider offset width than needed: " + tally.nonMinimalOffsets, 0, tally.nonMinimalOffsets);
        assertEquals("element counts written in 4-byte form unnecessarily: " + tally.nonMinimalCounts, 0, tally.nonMinimalCounts);

        logger.info("--- encoding forms used over {} documents ---", SAMPLE_DOCS);
        for (Map.Entry<String, Long> entry : tally.forms.entrySet()) {
            logger.info(String.format(Locale.ROOT, "  %-22s %12d", entry.getKey(), entry.getValue()));
        }
    }

    // ------------------------------------------------------------------ part 2

    /**
     * Accounts for every byte of the encoded documents and reports what the same data would cost under the two-column
     * layout Parquet uses, where {@code metadata} and {@code value} are separate, independently compressed columns.
     */
    public void testByteCompositionAndAlternativeLayouts() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("E10M"));

        Tally tally = new Tally();
        long jsonBytes = 0;
        long metadataBytes = 0;
        long valueBytes = 0;
        // Extra field-id bytes each value tree would need if field ids indexed one dictionary shared by the whole segment
        // instead of a per-document one. With ~15 keys per document a local id fits in one byte; a 1000-key shared
        // dictionary needs two. Sharing the dictionary is not free, and this is the price.
        long sharedDictFieldIdGrowth = 0;

        List<byte[]> jsonColumn = new ArrayList<>();
        List<byte[]> gluedColumn = new ArrayList<>();
        List<byte[]> metadataColumn = new ArrayList<>();
        List<byte[]> valueColumn = new ArrayList<>();

        Set<String> globalKeys = new HashSet<>();
        Map<String, Integer> keyOccurrences = new HashMap<>();

        for (int doc = 0; doc < SAMPLE_DOCS; doc++) {
            byte[] json = attributesJson(generator, doc);
            Variant variant = encode(json);
            byte[] metadata = variant.metadataBytes();
            byte[] value = variant.valueBytes();

            jsonBytes += json.length;
            metadataBytes += metadata.length;
            valueBytes += value.length;

            MetadataParts parts = splitMetadata(metadata);
            tally.metaHeaderBytes += parts.headerBytes;
            tally.metaOffsetBytes += parts.offsetBytes;
            tally.metaKeyBytes += parts.keyBytes;
            tally.dictionaryEntries += parts.count;
            for (String key : parts.keys) {
                globalKeys.add(key);
                keyOccurrences.merge(key, 1, Integer::sum);
            }

            walk(value, 0, tally);

            jsonColumn.add(json);
            metadataColumn.add(metadata);
            valueColumn.add(value);
            byte[] glued = new byte[4 + metadata.length + value.length];
            VariantEncoding.writeUnsigned(glued, 0, metadata.length, 4);
            System.arraycopy(metadata, 0, glued, 4, metadata.length);
            System.arraycopy(value, 0, glued, 4 + metadata.length, value.length);
            gluedColumn.add(glued);
        }

        // Re-encode against one dictionary holding every key in the sample, which is the layout Parquet's separate
        // `metadata` column amounts to. Done as a second pass because the global key set is only known after the first.
        List<String> sharedDictionary = new ArrayList<>(new TreeSet<>(globalKeys));
        List<byte[]> sharedValueColumn = new ArrayList<>();
        long sharedValueBytes = 0;
        for (int doc = 0; doc < SAMPLE_DOCS; doc++) {
            byte[] value = encode(attributesJson(generator, doc), sharedDictionary).valueBytes();
            sharedValueColumn.add(value);
            sharedValueBytes += value.length;
        }
        int globalIdWidth = VariantEncoding.minUnsignedWidth(sharedDictionary.size() - 1);
        for (Map.Entry<Integer, Long> entry : tally.objectMembersByIdWidth.entrySet()) {
            sharedDictFieldIdGrowth += entry.getValue() * Math.max(0, globalIdWidth - entry.getKey());
        }

        double n = SAMPLE_DOCS;
        long blobBytes = 4 * SAMPLE_DOCS + metadataBytes + valueBytes;

        logger.info("=== Variant encoding audit: {} documents from the E10M corpus ===", SAMPLE_DOCS);
        logger.info("");
        logger.info("attributes as JSON text          {} B/doc", round(jsonBytes / n));
        logger.info("attributes as a Variant blob     {} B/doc   ({} of JSON)", round(blobBytes / n), ratio(blobBytes, jsonBytes));
        logger.info("");
        logger.info("--- where the blob's bytes go ---");
        logger.info("  metadata (key dictionary)      {} B/doc   {}", round(metadataBytes / n), share(metadataBytes, blobBytes));
        logger.info("    key name bytes               {} B/doc   {}", round(tally.metaKeyBytes / n), share(tally.metaKeyBytes, blobBytes));
        logger.info(
            "    dictionary offsets             {} B/doc   {}",
            round(tally.metaOffsetBytes / n),
            share(tally.metaOffsetBytes, blobBytes)
        );
        logger.info(
            "    header                         {} B/doc   {}",
            round(tally.metaHeaderBytes / n),
            share(tally.metaHeaderBytes, blobBytes)
        );
        logger.info("  value tree                     {} B/doc   {}", round(valueBytes / n), share(valueBytes, blobBytes));
        logger.info(
            "    scalar payloads                {} B/doc   {}",
            round(tally.scalarPayloadBytes / n),
            share(tally.scalarPayloadBytes, blobBytes)
        );
        logger.info("    type tags                    {} B/doc   {}", round(tally.typeTagBytes / n), share(tally.typeTagBytes, blobBytes));
        logger.info("    field ids                    {} B/doc   {}", round(tally.fieldIdBytes / n), share(tally.fieldIdBytes, blobBytes));
        logger.info(
            "    field offsets                  {} B/doc   {}",
            round(tally.fieldOffsetBytes / n),
            share(tally.fieldOffsetBytes, blobBytes)
        );
        logger.info(
            "    container headers + counts     {} B/doc   {}",
            round(tally.containerHeaderBytes / n),
            share(tally.containerHeaderBytes, blobBytes)
        );
        logger.info("  framing (metadata length)      {} B/doc   {}", round(4), share(4L * SAMPLE_DOCS, blobBytes));
        logger.info("");
        logger.info("distinct keys in the sample      {}", globalKeys.size());
        logger.info("dictionary entries per document  {}", round(tally.dictionaryEntries / n));
        logger.info(
            "mean key name length             {} B",
            round(tally.dictionaryEntries == 0 ? 0 : (double) tally.metaKeyBytes / tally.dictionaryEntries)
        );
        logger.info("times each key name is stored    {} (mean over distinct keys)", round(meanOccurrences(keyOccurrences)));

        // ---- layout models -------------------------------------------------
        long sharedDictOnce = sharedDictionaryBytes(globalKeys);
        // The field-id widening accounts for nearly all of the growth, but not quite all of it: widening a nested
        // object's header enlarges its parent's values region, which can push a parent whose region sat just under 256
        // bytes up to a two-byte field-offset width. That knock-on is real, so bound it rather than assert it away.
        long predicted = valueBytes + sharedDictFieldIdGrowth;
        assertTrue(
            "a shared dictionary cannot shrink the value trees: predicted " + predicted + ", measured " + sharedValueBytes,
            sharedValueBytes >= predicted
        );
        assertTrue(
            "knock-on offset widening should be marginal, but added " + (sharedValueBytes - predicted) + " bytes on top of " + predicted,
            (double) (sharedValueBytes - predicted) / predicted < 0.001
        );

        logger.info("");
        logger.info("--- layout models, uncompressed ---");
        logger.info("  M0  JSON text                                   {} B/doc   baseline", round(jsonBytes / n));
        logger.info("  M1  blob as implemented (metadata glued in)     {} B/doc   {}", round(blobBytes / n), ratio(blobBytes, jsonBytes));
        logger.info("  M2  value column only, per-doc dictionary       {} B/doc   {}", round(valueBytes / n), ratio(valueBytes, jsonBytes));
        logger.info(
            "  M3  value column, segment-shared dictionary     {} B/doc   {}   (dictionary costs {} B once)",
            round(sharedValueBytes / n),
            ratio(sharedValueBytes, jsonBytes),
            sharedDictOnce
        );

        // ---- compression models --------------------------------------------
        logger.info("");
        logger.info("--- layout models, block-compressed at {} KB (as `_source` is) ---", BLOCK_BYTES / 1024);
        long jsonLz4 = blockCompress(jsonColumn, false);
        long gluedLz4 = blockCompress(gluedColumn, false);
        long metaLz4 = blockCompress(metadataColumn, false);
        long valueLz4 = blockCompress(valueColumn, false);
        long sharedLz4 = blockCompress(sharedValueColumn, false);
        long jsonZip = blockCompress(jsonColumn, true);
        long gluedZip = blockCompress(gluedColumn, true);
        long metaZip = blockCompress(metadataColumn, true);
        long valueZip = blockCompress(valueColumn, true);
        long sharedZip = blockCompress(sharedValueColumn, true);

        logger.info(row("layout", "LZ4 B/doc", "vs JSON", "deflate B/doc", "vs JSON"));
        logger.info(row("JSON text", round(jsonLz4 / n), "baseline", round(jsonZip / n), "baseline"));
        logger.info(row("M1 glued", round(gluedLz4 / n), ratio(gluedLz4, jsonLz4), round(gluedZip / n), ratio(gluedZip, jsonZip)));
        logger.info(
            row(
                "M4 two columns",
                round((metaLz4 + valueLz4) / n),
                ratio(metaLz4 + valueLz4, jsonLz4),
                round((metaZip + valueZip) / n),
                ratio(metaZip + valueZip, jsonZip)
            )
        );
        logger.info(
            row(
                "   metadata col",
                round(metaLz4 / n),
                share(metaLz4, metaLz4 + valueLz4),
                round(metaZip / n),
                share(metaZip, metaZip + valueZip)
            )
        );
        logger.info(
            row(
                "   value col",
                round(valueLz4 / n),
                share(valueLz4, metaLz4 + valueLz4),
                round(valueZip / n),
                share(valueZip, metaZip + valueZip)
            )
        );
        logger.info(
            row("M5 shared dict", round(sharedLz4 / n), ratio(sharedLz4, jsonLz4), round(sharedZip / n), ratio(sharedZip, jsonZip))
        );
        logger.info("");
        logger.info(
            "M5 carries the dictionary once per segment ({} B), so its per-document cost is the value column alone.",
            sharedDictOnce
        );

        // The audit's whole purpose is these two comparisons, so pin their direction rather than only printing them.
        assertTrue(
            "the metadata column must compress far better than the value column, since key names repeat across documents",
            (double) metaLz4 / metadataBytes < (double) valueLz4 / valueBytes
        );
        assertTrue(
            "sharing the dictionary must beat repeating it per document, in both compressors",
            sharedLz4 < metaLz4 + valueLz4 && sharedZip < metaZip + valueZip
        );
    }

    /**
     * How much a deduplicated metadata column can actually save, which depends entirely on how many documents share a key
     * set.
     *
     * <p>Storing metadata as {@code SortedDocValues} keeps one copy per <em>distinct byte string</em> per segment. So the
     * saving is governed by the number of distinct metadata values, not by the number of documents. This measures that
     * directly, and separately measures what canonicalising the dictionary order would add: two documents with the same
     * keys in a different order encode to different bytes today, because ids are assigned in insertion order.
     */
    public void testMetadataDeduplicationPotential() {
        OtelDocGenerator generator = new OtelDocGenerator(CorpusConfig.preset("E10M"));

        Set<String> distinctAsEncoded = new HashSet<>();
        Set<String> distinctIfCanonical = new HashSet<>();
        long metadataBytes = 0;
        long distinctBytes = 0;

        for (int doc = 0; doc < SAMPLE_DOCS; doc++) {
            Map<String, Object> attributes = generator.attributesAsMap(doc);
            byte[] metadata = encode(attributesJson(generator, doc)).metadataBytes();
            metadataBytes += metadata.length;

            // Identity of the bytes actually written.
            String asEncoded = new String(metadata, StandardCharsets.ISO_8859_1);
            if (distinctAsEncoded.add(asEncoded)) {
                distinctBytes += metadata.length;
            }
            // Identity a canonical (sorted) dictionary would produce: the key set, order-independent.
            distinctIfCanonical.add(String.join(" ", new TreeSet<>(collectKeys(attributes))));
        }

        double n = SAMPLE_DOCS;
        logger.info("=== metadata deduplication potential over {} documents ===", SAMPLE_DOCS);
        logger.info("metadata written                 {} B/doc", round(metadataBytes / n));
        logger.info(
            "distinct metadata values         {}  ({} of documents)",
            distinctAsEncoded.size(),
            share(distinctAsEncoded.size(), SAMPLE_DOCS)
        );
        logger.info(
            "distinct if order-canonical      {}  ({} of documents)",
            distinctIfCanonical.size(),
            share(distinctIfCanonical.size(), SAMPLE_DOCS)
        );
        logger.info("");
        logger.info("stored once per distinct value   {} B/doc amortised", round((double) distinctBytes / SAMPLE_DOCS));
        logger.info(
            "  => deduplication saves           {} of the metadata on this corpus",
            share(metadataBytes - distinctBytes, metadataBytes)
        );

        // The point of the measurement: on a corpus where documents carry different key sets, nearly every document has its
        // own metadata value and there is almost nothing to deduplicate. Pinned so the limitation cannot be forgotten.
        assertTrue(
            "this corpus should be near-worst-case for deduplication: " + distinctAsEncoded.size() + " distinct values",
            distinctAsEncoded.size() > SAMPLE_DOCS * 0.5
        );
    }

    /**
     * The same measurement across the shape sweep, which is the decision the two-column layout hangs on.
     *
     * <p>Shape count is combinatorial, not gradual: with keys sampled independently the number of possible key sets is
     * C(1000,15), so no two documents collide and deduplication cannot happen. With a bounded set of shapes, key sets
     * recur and the dictionary becomes small enough to hold in memory. This prints where the boundary falls.
     */
    public void testDeduplicationAcrossTheShapeSweep() {
        logger.info("=== distinct key sets by shape count, {} documents each ===", SHAPE_SWEEP_DOCS);
        logger.info(
            String.format(
                Locale.ROOT,
                "  %-16s %10s %11s %10s %10s %10s %10s %10s",
                "preset",
                "shapes",
                "distinct",
                "dict KB",
                "inline",
                "2col",
                "3col",
                "value"
            )
        );
        logger.info(
            "  distinct and dict KB are projected to a {}-document segment; the last four are metadata bytes per document",
            SEGMENT_DOCS
        );
        for (String preset : CorpusConfig.shapeSweep()) {
            CorpusConfig config = CorpusConfig.preset(preset);
            OtelDocGenerator generator = new OtelDocGenerator(config);
            Set<String> distinct = new HashSet<>();
            long metadataBytes = 0;
            long distinctBytes = 0;
            long valueBytes = 0;
            long nameIdBytes = 0;
            for (int doc = 0; doc < SHAPE_SWEEP_DOCS; doc++) {
                Variant variant = encode(attributesJson(generator, doc));
                byte[] metadata = variant.metadataBytes();
                metadataBytes += metadata.length;
                valueBytes += variant.valueBytes().length;
                // What a name-level dictionary would cost instead: one id per key, wide enough for the key pool.
                nameIdBytes += (long) variant.metadata().size() * 2;
                if (distinct.add(new String(metadata, StandardCharsets.ISO_8859_1))) {
                    distinctBytes += metadata.length;
                }
            }
            // What the deduplicated column actually costs per document: one ordinal, bit-packed to the dictionary's
            // width, plus the dictionary itself amortised over the documents in a segment.
            //
            // The dictionary is a fixed cost per segment, so it must be divided by the segment's document count, not by
            // this sample's. Dividing by the sample would overstate it by the ratio between them -- 200x here -- and
            // would make a bounded dictionary look expensive when it is nearly free.
            //
            // Whether the dictionary saturates is the load-bearing question. With bounded shapes the distinct count stops
            // growing while documents keep arriving, so the per-document cost keeps falling. With keys sampled per
            // document it grows in step, so it never amortises at all -- which is why that row is projected to full scale
            // rather than taken from the sample.
            boolean saturates = config.shapeCount() > 0;
            double distinctAtScale = saturates ? distinct.size() : (double) SEGMENT_DOCS * distinct.size() / SHAPE_SWEEP_DOCS;
            double dictBytesAtScale = saturates ? distinctBytes : (double) distinctBytes * SEGMENT_DOCS / SHAPE_SWEEP_DOCS;
            double ordinalBits = Math.max(1, Math.ceil(Math.log(Math.max(2, distinctAtScale)) / Math.log(2)));
            double dedupCost = ordinalBits / 8.0 + dictBytesAtScale / SEGMENT_DOCS;
            logger.info(
                String.format(
                    Locale.ROOT,
                    "  %-16s %10s %11.0f %10.0f %10.1f %10.1f %10.1f %10.1f",
                    preset,
                    config.shapeCount() == 0 ? "unique/doc" : String.valueOf(config.shapeCount()),
                    distinctAtScale,
                    dictBytesAtScale / 1024,
                    (double) metadataBytes / SHAPE_SWEEP_DOCS,
                    dedupCost,
                    (double) nameIdBytes / SHAPE_SWEEP_DOCS,
                    (double) valueBytes / SHAPE_SWEEP_DOCS
                )
            );
        }
    }

    /** Documents per shape-sweep point. Enough that a 1000-shape corpus sees every shape many times. */
    private static final int SHAPE_SWEEP_DOCS = 50_000;

    /**
     * Segment size the dictionary cost is amortised over, matching the benchmark's force-merged single segment.
     *
     * <p>Needed because a dictionary is paid once per segment, so its per-document cost depends entirely on how many
     * documents share it. Amortising over the sample instead would overstate it 200-fold.
     */
    private static final int SEGMENT_DOCS = 10_000_000;

    /** Every key name appearing anywhere in a document, including inside nested objects. */
    private static Set<String> collectKeys(Object value) {
        Set<String> keys = new HashSet<>();
        collectKeys(value, keys);
        return keys;
    }

    private static void collectKeys(Object value, Set<String> keys) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                keys.add(String.valueOf(entry.getKey()));
                collectKeys(entry.getValue(), keys);
            }
        } else if (value instanceof List<?> list) {
            for (Object element : list) {
                collectKeys(element, keys);
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] attributesJson(OtelDocGenerator generator, int doc) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(generator.attributesAsMap(doc));
            return BytesReference.toBytes(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Variant encode(byte[] json) {
        return encode(json, null);
    }

    /**
     * @param sharedDictionary keys that already have ids, or {@code null} for a dictionary private to this value
     */
    private static Variant encode(byte[] json, List<String> sharedDictionary) {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            parser.nextToken();
            VariantBuilder builder = new VariantBuilder();
            if (sharedDictionary != null) {
                builder.presetDictionary(sharedDictionary);
            }
            VariantJson.encode(parser, builder);
            return builder.finish();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Accumulates byte counts and encoding-form counts across many documents. */
    private static final class Tally {
        long metaHeaderBytes;
        long metaOffsetBytes;
        long metaKeyBytes;
        long dictionaryEntries;

        long containerHeaderBytes;
        long fieldIdBytes;
        long fieldOffsetBytes;
        long typeTagBytes;
        long scalarPayloadBytes;

        long nonMinimalIntegers;
        long nonMinimalStrings;
        long nonMinimalFieldIds;
        long nonMinimalOffsets;
        long nonMinimalCounts;

        /** field-id width used -> number of object members encoded at that width, for the shared-dictionary model. */
        final Map<Integer, Long> objectMembersByIdWidth = new TreeMap<>();
        final Map<String, Long> forms = new TreeMap<>();

        void form(String name) {
            forms.merge(name, 1L, Long::sum);
        }
    }

    private record MetadataParts(int count, int headerBytes, int offsetBytes, int keyBytes, List<String> keys) {
    }

    private static MetadataParts splitMetadata(byte[] metadata) {
        int offsetSize = ((metadata[0] & 0xFF) >>> VariantEncoding.OFFSET_SIZE_SHIFT) + 1;
        int count = VariantEncoding.readUnsigned(metadata, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        int keysStart = offsetsStart + (count + 1) * offsetSize;
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int from = VariantEncoding.readUnsigned(metadata, offsetsStart + i * offsetSize, offsetSize);
            int to = VariantEncoding.readUnsigned(metadata, offsetsStart + (i + 1) * offsetSize, offsetSize);
            keys.add(new String(metadata, keysStart + from, to - from, StandardCharsets.UTF_8));
        }
        // The header byte plus dictionary_size; the offset array is counted separately because it is the part a shared
        // dictionary would amortise away.
        return new MetadataParts(count, 1 + offsetSize, (count + 1) * offsetSize, metadata.length - keysStart, keys);
    }

    /**
     * Walks one value tree, attributing every byte to a category and checking each encoding is the narrowest the spec
     * allows.
     */
    private static void walk(byte[] value, int pos, Tally tally) {
        int basic = VariantEncoding.basicType(value[pos]);
        int header = VariantEncoding.valueHeader(value[pos]);
        switch (basic) {
            case VariantEncoding.BASIC_OBJECT: {
                boolean isLarge = ((header >>> VariantEncoding.OBJ_IS_LARGE_SHIFT) & 1) != 0;
                int idSize = ((header >>> VariantEncoding.OBJ_FIELD_ID_SIZE_SHIFT) & 0x03) + 1;
                int offsetSize = ((header >>> VariantEncoding.OBJ_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;
                int countWidth = isLarge ? 4 : 1;
                int count = VariantEncoding.readUnsigned(value, pos + 1, countWidth);
                int idsStart = pos + 1 + countWidth;
                int offsetsStart = idsStart + count * idSize;
                int valuesStart = offsetsStart + (count + 1) * offsetSize;

                tally.form("object");
                tally.containerHeaderBytes += 1 + countWidth;
                tally.fieldIdBytes += (long) count * idSize;
                tally.fieldOffsetBytes += (long) (count + 1) * offsetSize;
                tally.objectMembersByIdWidth.merge(idSize, (long) count, Long::sum);

                int maxId = 0;
                for (int i = 0; i < count; i++) {
                    maxId = Math.max(maxId, VariantEncoding.readUnsigned(value, idsStart + i * idSize, idSize));
                }
                int dataSize = VariantEncoding.readUnsigned(value, offsetsStart + count * offsetSize, offsetSize);
                if (idSize != VariantEncoding.minUnsignedWidth(maxId)) {
                    tally.nonMinimalFieldIds++;
                }
                if (offsetSize != VariantEncoding.minUnsignedWidth(dataSize)) {
                    tally.nonMinimalOffsets++;
                }
                if (isLarge && count <= VariantEncoding.MAX_SMALL_ELEMENT_COUNT) {
                    tally.nonMinimalCounts++;
                }
                for (int i = 0; i < count; i++) {
                    walk(value, valuesStart + VariantEncoding.readUnsigned(value, offsetsStart + i * offsetSize, offsetSize), tally);
                }
                return;
            }
            case VariantEncoding.BASIC_ARRAY: {
                boolean isLarge = ((header >>> VariantEncoding.ARR_IS_LARGE_SHIFT) & 1) != 0;
                int offsetSize = ((header >>> VariantEncoding.ARR_FIELD_OFFSET_SIZE_SHIFT) & 0x03) + 1;
                int countWidth = isLarge ? 4 : 1;
                int count = VariantEncoding.readUnsigned(value, pos + 1, countWidth);
                int offsetsStart = pos + 1 + countWidth;
                int valuesStart = offsetsStart + (count + 1) * offsetSize;

                tally.form("array");
                tally.containerHeaderBytes += 1 + countWidth;
                tally.fieldOffsetBytes += (long) (count + 1) * offsetSize;

                int dataSize = VariantEncoding.readUnsigned(value, offsetsStart + count * offsetSize, offsetSize);
                if (offsetSize != VariantEncoding.minUnsignedWidth(dataSize)) {
                    tally.nonMinimalOffsets++;
                }
                if (isLarge && count <= VariantEncoding.MAX_SMALL_ELEMENT_COUNT) {
                    tally.nonMinimalCounts++;
                }
                for (int i = 0; i < count; i++) {
                    walk(value, valuesStart + VariantEncoding.readUnsigned(value, offsetsStart + i * offsetSize, offsetSize), tally);
                }
                return;
            }
            case VariantEncoding.BASIC_SHORT_STRING: {
                tally.form("string (short form)");
                tally.typeTagBytes += 1;
                tally.scalarPayloadBytes += header;
                return;
            }
            default:
                break;
        }

        tally.typeTagBytes += 1;
        switch (header) {
            case VariantEncoding.P_NULL:
                tally.form("null");
                return;
            case VariantEncoding.P_TRUE:
            case VariantEncoding.P_FALSE:
                tally.form("boolean");
                return;
            case VariantEncoding.P_INT8:
            case VariantEncoding.P_INT16:
            case VariantEncoding.P_INT32:
            case VariantEncoding.P_INT64: {
                int width = switch (header) {
                    case VariantEncoding.P_INT8 -> 1;
                    case VariantEncoding.P_INT16 -> 2;
                    case VariantEncoding.P_INT32 -> 4;
                    default -> 8;
                };
                tally.form("int" + (width * 8));
                tally.scalarPayloadBytes += width;
                long stored = readSigned(value, pos + 1, width);
                if (width > minimalIntegerWidth(stored)) {
                    tally.nonMinimalIntegers++;
                }
                return;
            }
            case VariantEncoding.P_FLOAT:
                tally.form("float");
                tally.scalarPayloadBytes += 4;
                return;
            case VariantEncoding.P_DOUBLE:
                tally.form("double");
                tally.scalarPayloadBytes += 8;
                return;
            case VariantEncoding.P_DECIMAL4:
                tally.form("decimal4");
                tally.scalarPayloadBytes += 5;
                return;
            case VariantEncoding.P_DECIMAL8:
                tally.form("decimal8");
                tally.scalarPayloadBytes += 9;
                return;
            case VariantEncoding.P_DECIMAL16:
                tally.form("decimal16");
                tally.scalarPayloadBytes += 17;
                return;
            case VariantEncoding.P_STRING: {
                int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
                tally.form("string (long form)");
                tally.containerHeaderBytes += 4;
                tally.scalarPayloadBytes += length;
                if (length <= VariantEncoding.MAX_SHORT_STRING_LEN) {
                    tally.nonMinimalStrings++;
                }
                return;
            }
            case VariantEncoding.P_BINARY: {
                int length = VariantEncoding.readUnsigned(value, pos + 1, 4);
                tally.form("binary");
                tally.containerHeaderBytes += 4;
                tally.scalarPayloadBytes += length;
                return;
            }
            default:
                throw new AssertionError("unexpected primitive type id " + header);
        }
    }

    private static int minimalIntegerWidth(long value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return 1;
        }
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return 2;
        }
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return 4;
        }
        return 8;
    }

    private static long readSigned(byte[] value, int offset, int width) {
        long result = 0;
        for (int i = 0; i < width; i++) {
            result |= ((long) (value[offset + i] & 0xFF)) << (8 * i);
        }
        if (width < 8) {
            int shift = 64 - 8 * width;
            result = (result << shift) >> shift;
        }
        return result;
    }

    /** Size of one dictionary holding every key in the sample, as a segment-shared metadata column would. */
    private static long sharedDictionaryBytes(Set<String> keys) {
        int total = 0;
        for (String key : keys) {
            total += key.getBytes(StandardCharsets.UTF_8).length;
        }
        int offsetSize = VariantEncoding.minUnsignedWidth(Math.max(total, keys.size()));
        return 1L + offsetSize + (long) (keys.size() + 1) * offsetSize + total;
    }

    /**
     * Concatenates values into {@link #BLOCK_BYTES} blocks and compresses each, which is how stored fields hold
     * {@code _source}. Compressing per value instead would hide the cross-document redundancy that is the whole question
     * here.
     */
    private static long blockCompress(List<byte[]> values, boolean deflate) {
        long total = 0;
        int start = 0;
        while (start < values.size()) {
            int blockBytes = 0;
            int end = start;
            while (end < values.size() && blockBytes < BLOCK_BYTES) {
                blockBytes += values.get(end).length;
                end++;
            }
            byte[] block = new byte[blockBytes];
            int at = 0;
            for (int i = start; i < end; i++) {
                byte[] item = values.get(i);
                System.arraycopy(item, 0, block, at, item.length);
                at += item.length;
            }
            total += deflate ? deflateSize(block) : lz4Size(block);
            start = end;
        }
        return total;
    }

    private static long lz4Size(byte[] block) {
        try {
            ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            LZ4.compress(block, 0, block.length, out, new LZ4.HighCompressionHashTable());
            return out.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long deflateSize(byte[] block) {
        Deflater deflater = new Deflater(6, true);
        try {
            deflater.setInput(block);
            deflater.finish();
            byte[] scratch = new byte[block.length + 64];
            int written = 0;
            while (deflater.finished() == false && written < scratch.length) {
                written += deflater.deflate(scratch, written, scratch.length - written);
            }
            return written;
        } finally {
            deflater.end();
        }
    }

    private static double meanOccurrences(Map<String, Integer> occurrences) {
        long total = 0;
        for (int count : occurrences.values()) {
            total += count;
        }
        return occurrences.isEmpty() ? 0 : (double) total / occurrences.size();
    }

    private static String row(String label, String a, String b, String c, String d) {
        return String.format(Locale.ROOT, "  %-16s %12s %10s   %14s %10s", label, a, b, c, d);
    }

    private static String round(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    private static String ratio(long actual, long baseline) {
        return String.format(Locale.ROOT, "%+.1f%%", 100.0 * (actual - baseline) / baseline);
    }

    private static String share(long part, long whole) {
        return String.format(Locale.ROOT, "%.1f%%", 100.0 * part / whole);
    }
}
