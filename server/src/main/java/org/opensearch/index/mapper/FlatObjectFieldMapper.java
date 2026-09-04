/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.variant.Variant;
import org.opensearch.common.variant.VariantBuilder;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.flatobject.FlatObjectBlobIndexFieldData;
import org.opensearch.index.mapper.flatobject.FlatObjectBlobObjectIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.opensearch.index.mapper.FlatObjectFieldMapper.FlatObjectFieldType.getKeywordFieldType;
import static org.opensearch.index.mapper.KeywordFieldMapper.normalizeValue;
import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.apache.lucene.search.MultiTermQuery.DOC_VALUES_REWRITE;

/**
 * A field mapper for flat_objects.
 * This mapper accepts JSON object and treat as string fields in one index.
 * @opensearch.internal
 */
public final class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FlatObjectFieldMapper.class);

    public static final String CONTENT_TYPE = "flat_object";
    public static final Object DOC_VALUE_NO_MATCH = new Object();

    static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    static final String VALUE_SUFFIX = "._value";
    static final String DOT_SYMBOL = ".";
    static final String EQUAL_SYMBOL = "=";

    /**
     * Accepted and ignored, never rejected.
     *
     * <p>The columns this used to switch on are written for every {@code flat_object}, so it has no meaning. It is still
     * consumed because this mapper's type parser refuses a mapping with any unrecognised key: an index whose stored
     * mapping names it would fail shard allocation and could never be edited, since a mapping cannot be changed on an
     * index that will not open.
     */
    public static final String VARIANT_BLOB_PARAM = "variant_blob";

    /**
     * First version that writes the Variant blob columns, and therefore the first that can read a path out of them.
     *
     * <p>An index created before this has no columns. Answering an aggregation over one with no values would be worse than
     * refusing: the values exist in {@code _source}, so an empty answer is a wrong number rather than an absent one.
     */
    static final Version BLOB_COLUMNS_VERSION = Version.V_3_9_0;

    /** Suffix of the doc-values column holding each document's Variant value tree. */
    public static final String BLOB_SUFFIX = "._blob";

    /**
     * Never written. Named only so that a segment carrying a column under it can be recognised and refused.
     *
     * <p>Such a segment connects its field ids to the names by an explicit table, so its ids are not in name order. Read
     * by the rule this mapper's readers use, every value would come back under the wrong key and nothing would err, so
     * recognising the name is the difference between refusing and answering wrongly.
     */
    public static final String BLOB_META_SUFFIX = "._blobmeta";

    /**
     * Suffix of the column holding key <em>names</em>, one entry per name in the document.
     *
     * <p>Written as {@code SortedSetDocValues}, so Lucene keeps one copy of each distinct name per segment and gives every
     * document a list of ordinals into it. Deduplicating individual names rather than whole key <em>sets</em> is what bounds
     * the dictionary by how many names the field uses, which does not grow with the corpus.
     */
    public static final String BLOB_NAMES_SUFFIX = "._blobnames";

    /**
     * Most distinct keys in one document that can have their field ids relabelled into name order.
     *
     * <p>The bound is exactly the point where relabelling stops being free. Ids run {@code 0..count-1}, so with 256 or
     * fewer keys every id fits one byte, every object's field-id width is therefore one byte, and any permutation of those
     * ids still fits the bytes already written. One key more and an object holding only low ids gets a one-byte width while
     * a permutation could hand it an id of 256, which would need the value re-laid-out rather than patched. Such a document
     * is re-encoded with its dictionary supplied in name order instead, which reaches the same layout by a longer route.
     */
    static final int MAX_RELABELLED_KEYS = 256;

    /**
     * Most distinct keys one document may put in the blob.
     *
     * <p>Not a limit of the encoding, which allows far more: a guard, so one pathological document cannot make a segment's
     * per-document name lists arbitrarily long.
     */
    static final int MAX_KEYS_PER_DOCUMENT = 0xFFFF;

    /**
     * In flat_object field mapper, field type is similar to keyword field type
     * Cannot be tokenized, can OmitNorms, and can setIndexOption.
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    @Override
    public MappedFieldType keyedFieldType(String key) {
        return new FlatObjectFieldType(
            Strings.isNullOrEmpty(key) ? this.name() : (this.name() + DOT_SYMBOL + key),
            this.name(),
            valueFieldType,
            valueAndPathFieldType,
            fieldType().indexCreatedVersion()
        );
    }

    /**
     * The builder for the flat_object field mapper using default parameters
     * @opensearch.internal
     */
    public static class Builder extends FieldMapper.Builder<Builder> {

        private Version indexCreatedVersion = Version.CURRENT;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder indexCreatedVersion(Version indexCreatedVersion) {
            this.indexCreatedVersion = indexCreatedVersion;
            return this;
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            boolean isSearchable = true;
            boolean hasDocValue = true;
            KeywordFieldType valueFieldType = getKeywordFieldType(buildFullName(context), VALUE_SUFFIX, isSearchable, hasDocValue);
            KeywordFieldType valueAndPathFieldType = getKeywordFieldType(
                buildFullName(context),
                VALUE_AND_PATH_SUFFIX,
                isSearchable,
                hasDocValue
            );
            FlatObjectFieldType fft = new FlatObjectFieldType(
                buildFullName(context),
                null,
                valueFieldType,
                valueAndPathFieldType,
                indexCreatedVersion
            );

            return new FlatObjectFieldMapper(name, Defaults.FIELD_TYPE, fft);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * Creates a new TypeParser for flatObjectFieldMapper that does not use ParameterizedFieldMapper
     */
    public static class TypeParser implements Mapper.TypeParser {
        private final BiFunction<String, ParserContext, Builder> builderFunction;

        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            builder.indexCreatedVersion(parserContext.indexVersionCreated());
            // Removed, not merely read: this type parser rejects a mapping with anything left in the node, so accepting the
            // dead parameter is what keeps an index that still names it openable.
            if (node.remove(VARIANT_BLOB_PARAM) != null) {
                deprecationLogger.deprecate(
                    "flat_object_variant_blob",
                    "Parameter [{}] on field [{}] is ignored: [{}] always stores its values in a doc-values column.",
                    VARIANT_BLOB_PARAM,
                    name,
                    CONTENT_TYPE
                );
            }
            return builder;
        }
    }

    /**
     * flat_object fields type contains its own fieldType, one valueFieldType and one valueAndPathFieldType
     * @opensearch.internal
     */
    public static final class FlatObjectFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;
        private final String rootFieldName;
        private final KeywordFieldType valueFieldType;
        private final KeywordFieldType valueAndPathFieldType;
        /**
         * When the index was created, which decides whether the Variant blob column exists to read.
         *
         * <p>Held on the field type rather than only on the mapper because {@code FieldMapper.merge} replaces the field type
         * from the incoming mapper while leaving mapper-owned fields at their cloned values -- so anything a query needs to
         * consult has to live here.
         */
        private final Version indexCreatedVersion;

        public FlatObjectFieldType(String name, String rootFieldName, boolean isSearchable, boolean hasDocValues) {
            this(name, rootFieldName, isSearchable, hasDocValues, Version.CURRENT);
        }

        public FlatObjectFieldType(
            String name,
            String rootFieldName,
            boolean isSearchable,
            boolean hasDocValues,
            Version indexCreatedVersion
        ) {
            this(
                name,
                rootFieldName,
                getKeywordFieldType(rootFieldName == null ? name : rootFieldName, VALUE_SUFFIX, isSearchable, hasDocValues),
                getKeywordFieldType(rootFieldName == null ? name : rootFieldName, VALUE_AND_PATH_SUFFIX, isSearchable, hasDocValues),
                indexCreatedVersion
            );
        }

        public FlatObjectFieldType(
            String name,
            String rootFieldName,
            KeywordFieldType valueFieldType,
            KeywordFieldType valueAndPathFieldType
        ) {
            this(name, rootFieldName, valueFieldType, valueAndPathFieldType, Version.CURRENT);
        }

        public FlatObjectFieldType(
            String name,
            String rootFieldName,
            KeywordFieldType valueFieldType,
            KeywordFieldType valueAndPathFieldType,
            Version indexCreatedVersion
        ) {
            super(
                name,
                valueFieldType.isSearchable(),
                false,
                valueFieldType.hasDocValues(),
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            assert rootFieldName == null || (name.length() >= rootFieldName.length() && name.startsWith(rootFieldName));
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.rootFieldName = rootFieldName;
            this.valueFieldType = valueFieldType;
            this.valueAndPathFieldType = valueAndPathFieldType;
            this.indexCreatedVersion = indexCreatedVersion;
        }

        Version indexCreatedVersion() {
            return indexCreatedVersion;
        }

        /** Whether this index writes the Variant blob columns, and therefore whether a path can be read from them. */
        boolean hasBlobColumns() {
            return indexCreatedVersion.onOrAfter(BLOB_COLUMNS_VERSION);
        }

        /**
         * The path within the parent object that this keyed field type names, or {@code null} for the parent itself.
         *
         * <p>Empty rather than null for {@code keyedFieldType("")}, which {@code _field_caps} reaches: the name equals the
         * root, so there is no path below it.
         */
        String blobPath() {
            if (rootFieldName == null || name().length() <= rootFieldName.length()) {
                return null;
            }
            return name().substring(rootFieldName.length() + 1);
        }

        static KeywordFieldType getKeywordFieldType(String rootField, String suffix, boolean isSearchable, boolean hasDocValue) {
            return new KeywordFieldType(rootField + suffix, isSearchable, hasDocValue, Collections.emptyMap()) {
                @Override
                protected String rewriteForDocValue(Object value) {
                    assert value instanceof String;
                    return getDVPrefix(rootField) + value;
                }
            };
        }

        public KeywordFieldType getValueFieldType() {
            return this.valueFieldType;
        }

        public KeywordFieldType getValueAndPathFieldType() {
            return this.valueAndPathFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        NamedAnalyzer normalizer() {
            return indexAnalyzer();
        }

        /**
         *
         * Fielddata is an in-memory data structure that is used for aggregations, sorting, and scripting.
         * @param fullyQualifiedIndexName the name of the index this field-data is build for
         * @param searchLookup a {@link SearchLookup} supplier to allow for accessing other fields values in the context of runtime fields
         * @return IndexFieldData.Builder
         */
        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            String path = blobPath();
            if (path == null) {
                // The parent field itself, which only a script reaches: doc['attributes'] hands back the whole value as a
                // lazy map. Bytes rather than numeric on purpose -- ValuesSourceConfig reads the values-source type straight
                // off the fielddata, so declaring numeric here would let `sum` on the bare parent resolve and return
                // nonsense.
                if (hasBlobColumns() == false) {
                    return new SortedSetOrdinalsIndexFieldData.Builder(valueFieldType().name(), CoreValuesSourceType.BYTES);
                }
                return new FlatObjectBlobObjectIndexFieldData.Builder(name(), blobFieldName(name()), blobNamesFieldName(name()));
            }
            // A keyed path reads the Variant blob. This is also the gate: an index created before the columns existed has
            // nothing to read, and returning empty values would answer an aggregation with a plausible wrong number. The
            // aggregation framework never consults isAggregatable(), so throwing here is what actually stops it.
            if (hasBlobColumns() == false) {
                throw new IllegalArgumentException(
                    "Cannot aggregate or sort on ["
                        + name()
                        + "]: index ["
                        + fullyQualifiedIndexName
                        + "] was created in version "
                        + indexCreatedVersion
                        + ", before ["
                        + CONTENT_TYPE
                        + "] stored its values in a doc-values column. Reindex to enable it."
                );
            }
            return new FlatObjectBlobIndexFieldData.Builder(
                name(),
                blobFieldName(rootFieldName),
                blobNamesFieldName(rootFieldName),
                rootFieldName,
                path
            );
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String flatObjectKeywordValue = value.toString();

                    if (flatObjectKeywordValue.length() > ignoreAbove) {
                        return null;
                    }

                    NamedAnalyzer normalizer = normalizer();
                    if (normalizer == null) {
                        return flatObjectKeywordValue;
                    }

                    try {
                        return normalizeValue(normalizer, name(), flatObjectKeywordValue);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            if (rootFieldName != null) {
                if (blobPath() != null && hasBlobColumns()) {
                    // A blob-backed path returns real typed values, so it needs a real format. FlatObjectDocValueFormat
                    // implements only format(BytesRef) -- a numeric aggregation would throw from format(double) while
                    // rendering value_as_string, `missing` would throw from parseDouble before reading a document, and the
                    // format is not a registered NamedWriteable so a multi-shard reduce cannot deserialise it at all.
                    return DocValueFormat.RAW;
                }
                return new FlatObjectDocValueFormat(getDVPrefix(rootFieldName) + getPathPrefix(name()));
            } else {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support doc_value in root field"
                );
            }
        }

        /**
         * True only for a keyed path on an index that has the blob columns.
         *
         * <p>Kept as an override rather than left to {@code MappedFieldType}'s derivation from {@code fielddataBuilder},
         * because the parent field's builder now succeeds (a script needs it) and the base derivation would therefore report
         * the parent as aggregatable. Beyond being wrong, that would stop
         * {@code AggregatorTestCase.testSupportedFieldTypes} skipping {@code flat_object} and start exercising it in every
         * aggregation test in the repo against a document this mapper never wrote.
         */
        @Override
        public boolean isAggregatable() {
            return blobPath() != null && hasBlobColumns();
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // flat_objects are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // flat_object analyzer with the default attribute source which encodes terms using UTF8
                // in that case we skip normalization, which may be slow if there many terms need to
                // parse (eg. large terms query) since Analyzer.normalize involves things like creating
                // attributes through reflection
                // This if statement will be used whenever a normalizer is NOT configured
                return super.indexedValueForSearch(value);
            }

            if (value == null) {
                return null;
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return getTextSearchInfo().getSearchAnalyzer().normalize(name(), value.toString());
        }

        private KeywordFieldType valueFieldType() {
            return (rootFieldName == null) ? valueFieldType : valueAndPathFieldType;
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().termQueryCaseInsensitive(rewriteSearchValue(value), context);
        }

        /**
         * redirect queries with rewrite value to rewriteSearchValue and directSubFieldName
         */
        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().termQuery(rewriteSearchValue(value), context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            List<String> parsedValues = new ArrayList<>(values.size());
            for (Object value : values) {
                parsedValues.add(rewriteSearchValue(value));
            }

            return valueFieldType().termsQuery(parsedValues, context);
        }

        /**
         * To direct search fields, if a dot path was used in search query,
         * then direct to flatObjectFieldName._valueAndPath subfield,
         * else, direct to flatObjectFieldName._value subfield.
         * @return directedSubFieldName
         */
        public String getSearchField() {
            return isSubField() ? rootFieldName + VALUE_AND_PATH_SUFFIX : name() + VALUE_SUFFIX;
        }

        /**
         * If the search key has mappedFieldTypeName as prefix,
         * then the dot path was used in search query,
         * then rewrite the searchValueString as the format "dotpath=value",
         * @return rewriteSearchValue
         */
        public String rewriteSearchValue(Object value) {
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return isSubField() ? getPathPrefix(name()) + value : value.toString();
        }

        boolean isSubField() {
            return rootFieldName != null;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().prefixQuery(rewriteSearchValue(value), method, caseInsensitive, context);
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().regexpQuery(rewriteSearchValue(value), syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().fuzzyQuery(
                rewriteSearchValue(value),
                fuzziness,
                prefixLength,
                maxExpansions,
                transpositions,
                method,
                context
            );
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[range] queries on [text] or [keyword] fields cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false."
                );
            }
            failIfNotIndexedAndNoDocValues();

            if ((lowerTerm != null && upperTerm != null)) {
                return valueFieldType().rangeQuery(
                    rewriteSearchValue(lowerTerm),
                    rewriteSearchValue(upperTerm),
                    includeLower,
                    includeUpper,
                    context
                );
            }

            // when either the upper term or lower term is null,
            // we can't delegate to valueFieldType() and need to process the prefix ourselves
            Query indexQuery = null;
            Query dvQuery = null;
            if (isSearchable()) {
                if (isSubField() == false) {
                    indexQuery = new TermRangeQuery(
                        getSearchField(),
                        lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                        upperTerm == null ? null : indexedValueForSearch(upperTerm),
                        includeLower,
                        includeUpper
                    );
                } else {
                    Automaton a1 = PrefixQuery.toAutomaton(indexedValueForSearch(getPathPrefix(name())));
                    BytesRef lowerTermBytes = lowerTerm == null ? null : indexedValueForSearch(rewriteSearchValue(lowerTerm));
                    BytesRef upperTermBytes = upperTerm == null ? null : indexedValueForSearch(rewriteSearchValue(upperTerm));
                    Automaton a2 = TermRangeQuery.toAutomaton(lowerTermBytes, upperTermBytes, includeLower, includeUpper);
                    Automaton termAutomaton = Operations.intersection(a1, a2);
                    indexQuery = new AutomatonQuery(new Term(getSearchField()), termAutomaton, true);
                }
            }
            if (hasDocValues()) {
                String dvPrefix = isSubField() ? getDVPrefix(rootFieldName) : getDVPrefix(name());
                String prefix = dvPrefix + (isSubField() ? getPathPrefix(name()) : "");
                Automaton a1 = PrefixQuery.toAutomaton(indexedValueForSearch(prefix));
                BytesRef lowerDvBytes = lowerTerm == null ? null : indexedValueForSearch(dvPrefix + rewriteSearchValue(lowerTerm));
                BytesRef upperDvBytes = upperTerm == null ? null : indexedValueForSearch(dvPrefix + rewriteSearchValue(upperTerm));
                Automaton a2 = TermRangeQuery.toAutomaton(lowerDvBytes, upperDvBytes, includeLower, includeUpper);
                Automaton dvAutomaton = Operations.intersection(a1, a2);
                dvQuery = new AutomatonQuery(new Term(getSearchField()), dvAutomaton, true, DOC_VALUES_REWRITE);
            }

            assert indexQuery != null || dvQuery != null;
            return indexQuery == null ? dvQuery : (dvQuery == null ? indexQuery : new IndexOrDocValuesQuery(indexQuery, dvQuery));
        }

        /**
         * if there is dot path. query the field name in flatObject parent field (mappedFieldTypeName).
         * else query in _field_names system field
         */
        @Override
        public Query existsQuery(QueryShardContext context) {
            String searchKey;
            String searchField;
            if (isSubField()) {
                return rangeQuery(null, null, true, true, context);
            } else {
                if (hasDocValues()) {
                    return new FieldExistsQuery(name());
                } else {
                    searchKey = FieldNamesFieldMapper.NAME;
                    searchField = name();
                }
            }
            return new TermQuery(new Term(searchKey, indexedValueForSearch(searchField)));
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            QueryShardContext context
        ) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().wildcardQuery(rewriteSearchValue(value), method, caseInsensitve, context);
        }

        /**
         * A doc_value formatter for flat_object field.
         */
        public class FlatObjectDocValueFormat implements DocValueFormat {
            private static final String NAME = "flat_object";
            private final String prefix;

            public FlatObjectDocValueFormat(String prefix) {
                this.prefix = prefix;
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public Object format(BytesRef value) {
                String parsedValue = value.utf8ToString();
                if (parsedValue.startsWith(prefix) == false) {
                    return DOC_VALUE_NO_MATCH;
                }
                return parsedValue.substring(prefix.length());
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                return new BytesRef((String) valueFieldType.rewriteForDocValue(rewriteSearchValue(value)));
            }
        }
    }

    private final KeywordFieldType valueFieldType;
    private final KeywordFieldType valueAndPathFieldType;
    /**
     * Whether to write the Variant blob columns.
     *
     * <p>Not a mapping choice. It follows the index's creation version, so a new index always writes them and an older one
     * keeps behaving exactly as it did -- including accepting documents the encoder would reject, which is why the write is
     * gated and not merely the read.
     */
    private final boolean writeBlobColumns;

    FlatObjectFieldMapper(String simpleName, FieldType fieldType, FlatObjectFieldType mappedFieldType) {
        super(simpleName, fieldType, mappedFieldType, CopyTo.empty());
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        valueFieldType = mappedFieldType.valueFieldType;
        valueAndPathFieldType = mappedFieldType.valueAndPathFieldType;
        this.writeBlobColumns = mappedFieldType.hasBlobColumns();
    }

    /** The name of the column holding a field's Variant value trees. */
    public static String blobFieldName(String fieldName) {
        return fieldName + BLOB_SUFFIX;
    }

    /** The name of the column holding a field's Variant key metadata. */
    public static String blobMetaFieldName(String fieldName) {
        return fieldName + BLOB_META_SUFFIX;
    }

    /** The name of the column holding a field's key names, when they are stored separately. */
    public static String blobNamesFieldName(String fieldName) {
        return fieldName + BLOB_NAMES_SUFFIX;
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    public FlatObjectFieldType fieldType() {
        return (FlatObjectFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        HashSet<String> pathParts = parseObjectPathParts(context);
        if (pathParts != null) {
            createPathFields(context, pathParts);
        }
    }

    /**
     * Whether the whole array is handed to this mapper instead of one element at a time.
     *
     * <p>Only for an index that writes the blob columns. Element-at-a-time works for the terms -- each element is
     * flattened into the same term set -- but not for one doc-values column per document: the second element would have
     * nowhere to go. Taking the array whole lets it be encoded as a Variant array, which is what the value actually is.
     *
     * <p>An index that writes no blob keeps the element-at-a-time route it has always used, so nothing about it changes.
     */
    @Override
    public boolean parsesArrayValue() {
        return writeBlobColumns;
    }

    /**
     * Parses the flat_object field value and returns the collected path parts,
     * or {@code null} if the field should be skipped (null value or not searchable/stored/docvalues).
     */
    private HashSet<String> parseObjectPathParts(ParseContext context) throws IOException {
        XContentParser ctxParser = context.parser();
        if (fieldType().isSearchable() == false && fieldType().isStored() == false && fieldType().hasDocValues() == false) {
            ctxParser.skipChildren();
            return null;
        }

        if (ctxParser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        boolean isArray = ctxParser.currentToken() == XContentParser.Token.START_ARRAY;
        if (isArray == false && ctxParser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                ctxParser.getTokenLocation(),
                "[" + this.name() + "] unexpected token [" + ctxParser.currentToken() + "] in flat_object field value"
            );
        }
        if (isArray) {
            checkCompositeIndexArray(context);
        }

        // Encoded during the walk the mapper already performs, rather than from a separately materialised tree. Reading
        // the subtree into a map first would be simpler, at the cost of walking every document twice.
        VariantBuilder variantBuilder = writeBlobColumns ? new VariantBuilder() : null;

        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        HashSet<String> pathParts = new HashSet<>();
        if (isArray) {
            parseArrayBody(ctxParser, context, path, pathParts, variantBuilder);
        } else {
            if (variantBuilder != null) {
                variantBuilder.startObject();
            }
            ctxParser.nextToken(); // Skip the outer START_OBJECT. Need to return on END_OBJECT.
            while (ctxParser.currentToken() != XContentParser.Token.END_OBJECT) {
                parseToken(ctxParser, context, path, pathParts, variantBuilder);
            }
            if (variantBuilder != null) {
                // Closed inside the try in writeVariantBlob when the array route does it, so do the same here: this is
                // where a duplicate key surfaces, and it should be a bad request rather than an internal error.
                closeRoot(variantBuilder, true);
            }
        }

        if (variantBuilder != null) {
            writeVariantBlob(context, variantBuilder);
        }
        return pathParts;
    }

    /**
     * Walks a top-level array, leaving the parser on its {@code END_ARRAY} the way the object branch leaves it on
     * {@code END_OBJECT}.
     *
     * <p>Elements are restricted to the same shapes the element-at-a-time route accepted -- an object, a nested array of
     * them, or null -- so a document that used to be a bad request still is. A bare scalar has no key, so flat_object has
     * no term to put it in.
     */
    private void parseArrayBody(
        XContentParser parser,
        ParseContext context,
        Deque<String> path,
        HashSet<String> pathParts,
        VariantBuilder variantBuilder
    ) throws IOException {
        if (variantBuilder != null) {
            variantBuilder.startArray();
        }
        parser.nextToken();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    // Steps past its own END_OBJECT.
                    parseToken(parser, context, path, pathParts, variantBuilder);
                    break;
                case START_ARRAY:
                    parseArrayBody(parser, context, path, pathParts, variantBuilder);
                    parser.nextToken();
                    break;
                case VALUE_NULL:
                    // Indexes nothing, exactly as a null value for the whole field does. The blob keeps the position so
                    // that what it holds is the array the document sent.
                    if (variantBuilder != null) {
                        variantBuilder.appendNull();
                    }
                    parser.nextToken();
                    break;
                default:
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + this.name() + "] unexpected token [" + parser.currentToken() + "] in flat_object field value"
                    );
            }
        }
        if (variantBuilder != null) {
            closeRoot(variantBuilder, false);
        }
    }

    /**
     * The refusal {@code DocumentParser.parseNonDynamicArray} makes, repeated here because taking the array whole skips
     * that method.
     */
    private void checkCompositeIndexArray(ParseContext context) {
        if (context.indexSettings().isCompositeIndex() == false) {
            return;
        }
        boolean nested = name().contains(".") || context.mapperService().isCompositeIndexFieldNestedField(name());
        boolean partOfComposite = nested
            ? context.mapperService().isCompositeIndexFieldNestedField(name())
            : context.mapperService().isFieldPartOfCompositeIndex(simpleName());
        if (partOfComposite) {
            throw new MapperParsingException(
                "object mapping for ["
                    + name()
                    + "] with array for ["
                    + simpleName()
                    + "] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays"
            );
        }
    }

    /** Closes the value's outermost container, turning an encoding refusal into a bad request rather than a 500. */
    private void closeRoot(VariantBuilder variantBuilder, boolean isObject) {
        try {
            if (isObject) {
                variantBuilder.endObject();
            } else {
                variantBuilder.endArray();
            }
        } catch (IllegalStateException e) {
            throw new MapperParsingException("failed to encode [" + name() + "] as a Variant blob: " + e.getMessage(), e);
        }
    }

    private void writeVariantBlob(ParseContext context, VariantBuilder variantBuilder) {
        final Variant variant;
        try {
            variant = variantBuilder.finish();
        } catch (IllegalStateException e) {
            throw new MapperParsingException("failed to encode [" + name() + "] as a Variant blob: " + e.getMessage(), e);
        }
        // The two halves go to separate columns, which is how Parquet represents a Variant and the reason it does not pay
        // for the key names once per row. Here the metadata half is split further: the names go to a sorted column that
        // Lucene deduplicates across the whole segment, and the value half stays BinaryDocValues, distinct per document.
        //
        // Nothing written is positional across documents -- names are matched by their own bytes and field ids index only
        // the document's own name list -- so Lucene's own doc-values merge is correct without further work.
        writeBlobColumns(context, variantBuilder.dictionaryKeys(), variant);
    }

    private void createPathFields(ParseContext context, HashSet<String> pathParts) {
        for (String part : pathParts) {
            final BytesRef value = new BytesRef(name() + DOT_SYMBOL + part);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                context.doc().add(new Field(name(), value, fieldType));
            }
            if (fieldType().hasDocValues()) {
                context.doc().add(new SortedSetDocValuesField(name(), value));
            } else {
                createFieldNamesField(context);
            }
        }
    }

    private void createPathFieldsForPluggableFormat(ParseContext context, HashSet<String> pathParts) {
        for (String part : pathParts) {
            final BytesRef value = new BytesRef(name() + DOT_SYMBOL + part);
            context.documentInput().addField(fieldType(), value);
        }
    }

    /**
     * Writes the key names to their own sorted column and the value tree to a binary one.
     *
     * <p>Two columns rather than three, because of how the two sides meet. A mapper cannot know ordinals: Lucene assigns
     * them when the segment is flushed, long after this runs. What it can know is the order the names sort in
     * <em>within this document</em> -- and the name column hands a reader that document's ordinals ascending, which is name
     * order. So if field id {@code i} is made to mean the document's {@code i}-th smallest name, the reader's {@code i}-th
     * ordinal <em>is</em> the name that field id {@code i} refers to, and nothing has to be stored to connect them.
     *
     * <p>The encoder cannot assign ids that way as it goes, since a smaller name may still arrive, so it assigns them in
     * insertion order and {@link Variant#relabelFieldIds} permutes them here. That is a patch of the field-id bytes at the
     * width already written, which holds for any document with at most {@link #MAX_RELABELLED_KEYS} distinct keys. A wider
     * one keeps its insertion-order ids and gets an explicit rank list in {@link #BLOB_META_SUFFIX} instead -- the layout
     * every document used before relabelling existed, which is why readers still handle it.
     */
    private void writeBlobColumns(ParseContext context, List<String> keys, Variant variant) {
        int count = keys.size();
        String namesField = blobNamesFieldName(name());
        String blobField = blobFieldName(name());
        IndexableField existing = context.doc().getByKey(blobField);
        if (existing != null) {
            // One column holds one value per document, so a second value for the same field has nowhere to go. Refusing the
            // document is not an option -- the terms take every value, so this is a document flat_object accepts -- and
            // keeping only the first would answer an aggregation with part of the document and no sign that it had.
            //
            // So the value already written is replaced by the empty one, and the document reads as missing.
            //
            // Defence rather than a live path: an array is taken whole, and the XContent parsers reject a repeated key
            // outright, so nothing arriving through one calls this twice.
            ((Field) existing).setBytesValue(new BytesRef(EMPTY_OBJECT_VALUE));
            return;
        }
        if (count > MAX_KEYS_PER_DOCUMENT) {
            // Indexed with its terms and no readable blob. Refusing would reject a document plain flat_object accepts.
            writeUnencodable(context, blobField);
            return;
        }

        byte[][] keyBytes = new byte[count][];
        Integer[] byName = new Integer[count];
        for (int i = 0; i < count; i++) {
            keyBytes[i] = keys.get(i).getBytes(StandardCharsets.UTF_8);
            byName[i] = i;
        }
        Arrays.sort(byName, (a, b) -> Arrays.compareUnsigned(keyBytes[a], keyBytes[b]));

        // Two keys the encoder holds apart can arrive here as the same bytes, and that would silently misalign every field
        // id above them. The encoder's dictionary is keyed by String; the name column stores UTF-8 and Lucene deduplicates a
        // document's entries by those bytes. So a document with keys that differ as Strings but not as UTF-8 -- an unpaired
        // surrogate, which encodes to the same '?' as a literal question mark -- writes fewer ordinals than it has field
        // ids, and field id i stops meaning ordinal i.
        //
        // Defence rather than a live path: the XContent parsers refuse a lone surrogate in a property name, so nothing
        // arriving through one can get here. Kept because the consequence is silently wrong values and the check is an
        // adjacent-pair scan of a list already sorted by exactly these bytes.
        for (int rank = 1; rank < count; rank++) {
            if (Arrays.equals(keyBytes[byName[rank - 1]], keyBytes[byName[rank]])) {
                // Not encodable at all: the two keys are one entry in the name column however they are written. Indexed
                // with its terms and no readable blob, because refusing would reject a document flat_object accepts.
                writeUnencodable(context, blobField);
                return;
            }
        }

        byte[] value;
        if (count <= MAX_RELABELLED_KEYS) {
            int[] idMap = new int[count];
            for (int rank = 0; rank < count; rank++) {
                idMap[byName[rank]] = rank;
            }
            // Mutates the value bytes read below. The Variant's own dictionary no longer describes them afterwards, which is
            // why nothing here reads a key back through it.
            variant.relabelFieldIds(idMap);
            value = variant.valueBytes();
        } else {
            // Too wide to patch in place, so re-encode with the dictionary supplied in name order -- the way a Parquet writer
            // produces sorted ids. Same result, and no key-count limit; it just costs a second pass over the tree.
            List<String> sorted = new ArrayList<>(count);
            for (int rank = 0; rank < count; rank++) {
                sorted.add(keys.get(byName[rank]));
            }
            value = variant.reencodeWithDictionary(sorted);
        }

        for (int i = 0; i < count; i++) {
            context.doc().add(new SortedSetDocValuesField(namesField, new BytesRef(keyBytes[i])));
        }
        context.doc().addWithKey(blobField, new BinaryDocValuesField(blobField, new BytesRef(value)));
    }

    /**
     * Records a document whose value the Variant format cannot hold, without refusing it.
     *
     * <p>The terms are already written, so for this document the field behaves exactly as it would on an index with no
     * columns at all: searchable, and absent from anything that reads the column. What goes in is an empty value and no
     * names, which reads as missing.
     *
     * <p>Writing something rather than nothing matters for one case. The read path refuses a segment whose
     * documents have the field but no column, because that is a broken index rather than an absent value -- and a segment
     * in which <em>every</em> document landed here would look exactly like one. An entry keeps the column present, so such
     * a segment answers missing instead of failing the query.
     */
    private void writeUnencodable(ParseContext context, String blobField) {
        context.doc().addWithKey(blobField, new BinaryDocValuesField(blobField, new BytesRef(EMPTY_OBJECT_VALUE)));
    }

    /** The value bytes of an empty Variant object, written for a document whose value cannot be encoded. */
    private static final byte[] EMPTY_OBJECT_VALUE = emptyObjectValue();

    private static byte[] emptyObjectValue() {
        VariantBuilder builder = new VariantBuilder(8);
        builder.startObject();
        builder.endObject();
        return builder.finish().valueBytes();
    }

    private static String getDVPrefix(String rootFieldName) {
        return rootFieldName + DOT_SYMBOL;
    }

    private static String getPathPrefix(String path) {
        return path + EQUAL_SYMBOL;
    }

    private void parseToken(
        XContentParser parser,
        ParseContext context,
        Deque<String> path,
        HashSet<String> pathParts,
        VariantBuilder variantBuilder
    ) throws IOException {
        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
            final String currentFieldName = parser.currentName();
            if (variantBuilder != null) {
                // A key named _blob, _blobnames or _blobmeta needs no special handling. The columns take those names as
                // Lucene fields, while a key of the same name is a name inside the value tree and a term in the field's own
                // terms -- two different namespaces, so nothing collides, and a path query for one resolves it as the key.
                variantBuilder.appendKey(currentFieldName);
            }
            path.addLast(currentFieldName); // Pushing onto the stack *must* be matched by pop
            parser.nextToken(); // advance to the value of fieldName
            parseToken(parser, context, path, pathParts, variantBuilder); // parse the value for fieldName (which will be an array,
            // an object, or a primitive value)
            path.removeLast(); // Here is where we pop fieldName from the stack (since we're done with the value of fieldName)
            // Note that whichever other branch we just passed through has already ended with nextToken(), so we
            // don't need to call it.
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            if (variantBuilder != null) {
                variantBuilder.startArray();
            }
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                parseToken(parser, context, path, pathParts, variantBuilder);
            }
            if (variantBuilder != null) {
                variantBuilder.endArray();
            }
            parser.nextToken();
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            if (variantBuilder != null) {
                variantBuilder.startObject();
            }
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                parseToken(parser, context, path, pathParts, variantBuilder);
            }
            if (variantBuilder != null) {
                variantBuilder.endObject();
            }
            parser.nextToken();
        } else {
            // Appended before the term-building logic below, which skips nulls and over-long values. The blob is a
            // faithful copy of the value, so it must record what the terms drop; otherwise the two stores would disagree
            // for reasons unrelated to where the value lives.
            if (variantBuilder != null) {
                appendScalar(parser, variantBuilder);
            }
            String value = parseValue(parser);
            if (value == null || value.length() > fieldType().ignoreAbove) {
                parser.nextToken();
                return;
            }
            NamedAnalyzer normalizer = fieldType().normalizer();
            if (normalizer != null) {
                value = normalizeValue(normalizer, name(), value);
            }
            final String leafPath = Strings.collectionToDelimitedString(path, ".");
            final String valueAndPath = getPathPrefix(leafPath) + value;
            if (fieldType().isSearchable() || fieldType().isStored()) {
                context.doc().add(new Field(valueFieldType.name(), new BytesRef(value), fieldType));
                context.doc().add(new Field(valueAndPathFieldType.name(), new BytesRef(valueAndPath), fieldType));
            }

            if (fieldType().hasDocValues()) {
                context.doc().add(new SortedSetDocValuesField(valueFieldType.name(), new BytesRef(getDVPrefix(name()) + value)));
                context.doc()
                    .add(new SortedSetDocValuesField(valueAndPathFieldType.name(), new BytesRef(getDVPrefix(name()) + valueAndPath)));
            }

            pathParts.addAll(Arrays.asList(leafPath.substring(name().length() + 1).split("\\.")));
            parser.nextToken();
        }
    }

    private static String parseValue(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_BOOLEAN:
            case VALUE_NUMBER:
            case VALUE_STRING:
            case VALUE_NULL:
                return parser.textOrNull();
            // Handle other token types as needed
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected value token type [" + parser.currentToken() + "]");
        }
    }

    /**
     * Appends the current scalar token to the Variant, keeping its type rather than the string form the terms use.
     *
     * <p>The type comes from the same parser the terms come from, so both stores derive their type information from one
     * source. What Variant adds is that the decision is recorded, instead of being made again by whoever reads
     * {@code _source} later.
     */
    private static void appendScalar(XContentParser parser, VariantBuilder variantBuilder) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                variantBuilder.appendNull();
                break;
            case VALUE_BOOLEAN:
                variantBuilder.appendBoolean(parser.booleanValue());
                break;
            case VALUE_STRING:
                variantBuilder.appendString(parser.text());
                break;
            case VALUE_NUMBER:
                switch (parser.numberType()) {
                    case INT:
                    case LONG:
                        variantBuilder.appendLong(parser.longValue());
                        break;
                    case FLOAT:
                        variantBuilder.appendFloat(parser.floatValue());
                        break;
                    case BIG_INTEGER:
                        java.math.BigInteger big = new java.math.BigInteger(parser.text());
                        if (big.bitLength() > 127) {
                            // Variant's widest integer is decimal16. Rather than refuse a document flat_object accepts
                            // today, keep the number as its text -- which is exactly what this field's terms already hold,
                            // so the two stores agree instead of one of them rejecting.
                            variantBuilder.appendString(parser.text());
                        } else {
                            variantBuilder.appendBigInteger(big);
                        }
                        break;
                    default:
                        variantBuilder.appendDouble(parser.doubleValue());
                        break;
                }
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected value token type [" + parser.currentToken() + "]");
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
