/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.MatcherAssert;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MultiTermsAggregatorTests extends AggregatorTestCase {
    private static final String FIELD_NAME = "field";
    private static final String VALUE_SCRIPT_NAME = "value_script";
    private static final String FIELD_SCRIPT_NAME = "field_script";

    private static final String AGG_NAME = "_name";

    private static final String INT_FIELD = "int";
    private static final String LONG_FIELD = "long";
    private static final String FLOAT_FIELD = "float";
    private static final String DOUBLE_FIELD = "double";
    private static final String KEYWORD_FIELD = "keyword";
    private static final String DATE_FIELD = "date";
    private static final String IP_FIELD = "ip";
    private static final String GEO_POINT_FIELD = "geopoint";
    private static final String BOOL_FIELD = "bool";
    private static final String UNRELATED_KEYWORD_FIELD = "unrelated";

    private static final Map<String, MappedFieldType> mappedFieldTypeMap = new HashMap<String, MappedFieldType>() {
        {
            put(INT_FIELD, new NumberFieldMapper.NumberFieldType(INT_FIELD, NumberFieldMapper.NumberType.INTEGER));
            put(LONG_FIELD, new NumberFieldMapper.NumberFieldType(LONG_FIELD, NumberFieldMapper.NumberType.LONG));
            put(FLOAT_FIELD, new NumberFieldMapper.NumberFieldType(FLOAT_FIELD, NumberFieldMapper.NumberType.FLOAT));
            put(DOUBLE_FIELD, new NumberFieldMapper.NumberFieldType(DOUBLE_FIELD, NumberFieldMapper.NumberType.DOUBLE));
            put(DATE_FIELD, dateFieldType(DATE_FIELD));
            put(KEYWORD_FIELD, new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD));
            put(IP_FIELD, new IpFieldMapper.IpFieldType(IP_FIELD));
            put(FIELD_NAME, new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.INTEGER));
            put(UNRELATED_KEYWORD_FIELD, new KeywordFieldMapper.KeywordFieldType(UNRELATED_KEYWORD_FIELD));
            put(GEO_POINT_FIELD, new GeoPointFieldMapper.GeoPointFieldType(GEO_POINT_FIELD));
            put(BOOL_FIELD, new BooleanFieldMapper.BooleanFieldType(BOOL_FIELD));
        }
    };

    private static final Consumer<MultiTermsAggregationBuilder> NONE_DECORATOR = null;

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Collections.unmodifiableList(
            asList(
                CoreValuesSourceType.NUMERIC,
                CoreValuesSourceType.BYTES,
                CoreValuesSourceType.IP,
                CoreValuesSourceType.DATE,
                CoreValuesSourceType.BOOLEAN
            )
        );
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createTestAggregatorBuilder(asList(term(fieldName), term(fieldName)));
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = org.opensearch.common.collect.Map.of(
            VALUE_SCRIPT_NAME,
            vars -> ((Number) vars.get("_value")).doubleValue() + 1,
            FIELD_SCRIPT_NAME,
            vars -> {
                final String fieldName = (String) vars.get(FIELD_NAME);
                final LeafDocLookup lookup = (LeafDocLookup) vars.get("doc");
                return lookup.get(fieldName).stream().map(value -> ((Number) value).longValue() + 1).collect(toList());
            }
        );
        final MockScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, emptyMap());
        final Map<String, ScriptEngine> engines = singletonMap(engine.getType(), engine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testNumbers() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            fieldConfigs(asList(INT_FIELD, LONG_FIELD, FLOAT_FIELD, DOUBLE_FIELD)),
            NONE_DECORATOR,
            iw -> {
                iw.addDocument(
                    asList(
                        new NumericDocValuesField(INT_FIELD, 1),
                        new SortedNumericDocValuesField(LONG_FIELD, 1L),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                        new DoubleDocValuesField(DOUBLE_FIELD, 1.0d)
                    )
                );
                iw.addDocument(
                    asList(
                        new NumericDocValuesField(INT_FIELD, 1),
                        new SortedNumericDocValuesField(LONG_FIELD, 1L),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                        new DoubleDocValuesField(DOUBLE_FIELD, 1.0d)
                    )
                );
                iw.addDocument(
                    asList(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new SortedNumericDocValuesField(LONG_FIELD, 2L),
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f),
                        new DoubleDocValuesField(DOUBLE_FIELD, 2.0d)
                    )
                );
                iw.addDocument(
                    asList(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new SortedNumericDocValuesField(LONG_FIELD, 2L),
                        new FloatDocValuesField(FLOAT_FIELD, 3.0f),
                        new DoubleDocValuesField(DOUBLE_FIELD, 3.0d)
                    )
                );
                iw.addDocument(
                    asList(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new SortedNumericDocValuesField(LONG_FIELD, 2L),
                        new FloatDocValuesField(FLOAT_FIELD, 3.0f),
                        new DoubleDocValuesField(DOUBLE_FIELD, 3.0d)
                    )
                );
            },
            h -> {
                assertEquals(3, h.getBuckets().size());
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo(1L), equalTo(1L), equalTo(1.0), equalTo(1.0)));
                assertEquals(2, h.getBuckets().get(0).getDocCount());
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo(2L), equalTo(2L), equalTo(3.0), equalTo(3.0)));
                assertEquals(2, h.getBuckets().get(1).getDocCount());
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo(2L), equalTo(2L), equalTo(2.0), equalTo(2.0)));
                assertEquals(1, h.getBuckets().get(2).getDocCount());
            }
        );
    }

    public void testMixNumberAndKeywordWithFilter() throws IOException {
        testAggregation(
            new TermQuery(new Term(KEYWORD_FIELD, "a")),
            fieldConfigs(asList(KEYWORD_FIELD, FLOAT_FIELD)),
            NONE_DECORATOR,
            iw -> {
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                        new StringField(KEYWORD_FIELD, "a", Field.Store.NO),
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                        new StringField(KEYWORD_FIELD, "a", Field.Store.NO),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                        new StringField(KEYWORD_FIELD, "b", Field.Store.NO),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                        new StringField(KEYWORD_FIELD, "a", Field.Store.NO),
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                    )
                );
            },
            h -> {
                assertEquals(2, h.getBuckets().size());
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(2.0)));
                assertEquals(2, h.getBuckets().get(0).getDocCount());
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1.0)));
                assertEquals(1, h.getBuckets().get(1).getDocCount());
            }
        );
    }

    public void testMixNumberAndKeyword() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD, FLOAT_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(3));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L), equalTo(1.0)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("b"), equalTo(1L), equalTo(2.0)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo(2L), equalTo(2.0)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
        });
    }

    public void testMultiValuesField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(
                asList(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedNumericDocValuesField(INT_FIELD, 1)
                )
            );
            iw.addDocument(
                asList(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedNumericDocValuesField(INT_FIELD, 1),
                    new SortedNumericDocValuesField(INT_FIELD, 3)
                )
            );
        }, h -> {
            assertEquals(3, h.getBuckets().size());
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(3L)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("b"), equalTo(1L)));
        });

        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(
                asList(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedNumericDocValuesField(INT_FIELD, 1),
                    new SortedNumericDocValuesField(INT_FIELD, 2)
                )
            );
            iw.addDocument(
                asList(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new SortedNumericDocValuesField(INT_FIELD, 1),
                    new SortedNumericDocValuesField(INT_FIELD, 3)
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(7));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(2L)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(3L)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(1L)));
            assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(4).getKey(), contains(equalTo("b"), equalTo(2L)));
            assertThat(h.getBuckets().get(4).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(5).getKey(), contains(equalTo("c"), equalTo(1L)));
            assertThat(h.getBuckets().get(5).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(6).getKey(), contains(equalTo("c"), equalTo(3L)));
            assertThat(h.getBuckets().get(6).getDocCount(), equalTo(1L));
        });
    }

    public void testScripts() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(KEYWORD_FIELD).build(),
                new MultiTermsValuesSourceConfig.Builder().setScript(
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap(FIELD_NAME, FIELD_NAME))
                ).setUserValueTypeHint(ValueType.LONG).build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(FIELD_NAME, 1))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new NumericDocValuesField(FIELD_NAME, 3))
                );
            },
            h -> {
                assertEquals(3, h.getBuckets().size());
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("b"), equalTo(3L)));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(2L)));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo(4L)));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testScriptsWithoutValueTypeHint() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(KEYWORD_FIELD).build(),
                new MultiTermsValuesSourceConfig.Builder().setScript(
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap(FIELD_NAME, FIELD_NAME))
                ).build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(FIELD_NAME, 1))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new NumericDocValuesField(FIELD_NAME, 3))
                );
            },
            h -> {
                assertEquals(3, h.getBuckets().size());
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("b"), equalTo("3")));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo("2")));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo("4")));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testValueScripts() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(KEYWORD_FIELD).build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName(FIELD_NAME)
                    .setScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap()))
                    .build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(FIELD_NAME, 1))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(FIELD_NAME, 2))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new NumericDocValuesField(FIELD_NAME, 3))
                );
            },
            h -> {
                assertEquals(3, h.getBuckets().size());
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("b"), equalTo(3.0)));
                assertEquals(2L, h.getBuckets().get(0).getDocCount());
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(2.0)));
                assertEquals(1L, h.getBuckets().get(1).getDocCount());
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo(4.0)));
                assertEquals(1L, h.getBuckets().get(2).getDocCount());
            }
        );
    }

    public void testOrderByMetrics() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD)), b -> {
            b.order(BucketOrder.aggregation("max", false));
            b.subAggregation(new MaxAggregationBuilder("max").field(FLOAT_FIELD));
        }, iw -> {
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 3.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 4.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 3.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                )
            );
        }, h -> {

            assertEquals(3, h.getBuckets().size());
            org.hamcrest.MatcherAssert.assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertEquals(2L, h.getBuckets().get(0).getDocCount());
            MatcherAssert.assertThat(((InternalMax) (h.getBuckets().get(0).getAggregations().get("max"))).value(), closeTo(4.0f, 0.01));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("b"), equalTo(2L)));
            assertEquals(2L, h.getBuckets().get(1).getDocCount());
            assertEquals(3.0f, ((InternalMax) (h.getBuckets().get(1).getAggregations().get("max"))).value(), 0.01);
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo(3L)));
            assertEquals(2L, h.getBuckets().get(2).getDocCount());
            assertEquals(3.0f, ((InternalMax) (h.getBuckets().get(2).getAggregations().get("max"))).value(), 0.01);
        });
    }

    public void testNumberFieldFormat() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(term(KEYWORD_FIELD), new MultiTermsValuesSourceConfig.Builder().setFieldName(DOUBLE_FIELD).setFormat("00.00").build()),
            null,
            iw -> {
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new DoubleDocValuesField(DOUBLE_FIELD, 1.0d))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new DoubleDocValuesField(DOUBLE_FIELD, 2.0d))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new DoubleDocValuesField(DOUBLE_FIELD, 2.0d))
                );
                iw.addDocument(
                    asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new DoubleDocValuesField(DOUBLE_FIELD, 1.0d))
                );
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(3));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("a|01.00"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("a|02.00"));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("b|02.00"));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testDates() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(new MultiTermsValuesSourceConfig.Builder().setFieldName(DATE_FIELD).build(), term(KEYWORD_FIELD)),
            null,
            iw -> {
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-22")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-21")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c"))
                    )
                );
            },
            h -> {
                MatcherAssert.assertThat(h.getBuckets(), hasSize(4));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("2022-03-23|a"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("2022-03-21|c"));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("2022-03-22|a"));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKeyAsString(), equalTo("2022-03-23|b"));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testDatesFieldFormat() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(DATE_FIELD).setFormat("yyyy/MM/dd").build(),
                term(KEYWORD_FIELD)
            ),
            null,
            iw -> {
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-22")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-23")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-03-21")),
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c"))
                    )
                );
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(4));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("2022/03/23|a"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("2022/03/21|c"));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("2022/03/22|a"));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKeyAsString(), equalTo("2022/03/23|b"));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testIpAndKeyword() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, IP_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedDocValuesField(IP_FIELD, new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.0"))))
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedDocValuesField(IP_FIELD, new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.1"))))
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new SortedDocValuesField(IP_FIELD, new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.2"))))
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedDocValuesField(IP_FIELD, new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.0"))))
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(3));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo("192.168.0.0")));
            assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("a|192.168.0.0"));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("b"), equalTo("192.168.0.1")));
            assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("b|192.168.0.1"));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("c"), equalTo("192.168.0.2")));
            assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("c|192.168.0.2"));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
        });
    }

    public void testEmpty() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD)), NONE_DECORATOR, iw -> {}, h -> {
            assertThat(h.getName(), equalTo(AGG_NAME));
            assertThat(h.getBuckets(), hasSize(0));
        });
    }

    public void testNull() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD, FLOAT_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                )
            );
            iw.addDocument(
                asList(
                    new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                )
            );
            iw.addDocument(asList(new NumericDocValuesField(INT_FIELD, 1), new FloatDocValuesField(FLOAT_FIELD, 2.0f)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new FloatDocValuesField(FLOAT_FIELD, 2.0f)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("d")), new NumericDocValuesField(INT_FIELD, 3)));

        }, h -> {
            assertEquals(1, h.getBuckets().size());
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L), equalTo(1.0)));
            assertEquals(2, h.getBuckets().get(0).getDocCount());
        });

    }

    public void testMissing() throws IOException {
        testAggregation(
            new MatchAllDocsQuery(),
            asList(
                new MultiTermsValuesSourceConfig.Builder().setFieldName(KEYWORD_FIELD).setMissing("a").build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName(INT_FIELD).setMissing(1).build(),
                new MultiTermsValuesSourceConfig.Builder().setFieldName(FLOAT_FIELD).setMissing(2.0f).build()
            ),
            NONE_DECORATOR,
            iw -> {
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                        new NumericDocValuesField(INT_FIELD, 1),
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                    )
                );
                iw.addDocument(
                    asList(
                        // missing KEYWORD_FIELD
                        new NumericDocValuesField(INT_FIELD, 1),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f)
                    )
                );
                iw.addDocument(
                    asList(
                        new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                        // missing INT_FIELD
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f)
                    )
                );
                iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new NumericDocValuesField(INT_FIELD, 2)
                // missing FLOAT_FIELD
                ));
                iw.addDocument(singletonList(new SortedDocValuesField(UNRELATED_KEYWORD_FIELD, new BytesRef("unrelated"))));
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(4));
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L), equalTo(2.0)));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1L), equalTo(1.0)));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("b"), equalTo(1L), equalTo(2.0)));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("c"), equalTo(2L), equalTo(2.0)));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testMixKeywordAndBoolean() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, BOOL_FIELD)), NONE_DECORATOR, iw -> {
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(BOOL_FIELD, 1)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(BOOL_FIELD, 0)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(BOOL_FIELD, 0)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(BOOL_FIELD, 1)));
        }, h -> {
            assertThat(h.getBuckets(), hasSize(4));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(false)));
            assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("a|false"));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(true)));
            assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("a|true"));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("b"), equalTo(false)));
            assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("b|false"));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(true)));
            assertThat(h.getBuckets().get(3).getKeyAsString(), equalTo("b|true"));
            assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
        });
    }

    public void testGeoPointField() {
        assertThrows(
            IllegalArgumentException.class,
            () -> testAggregation(
                new MatchAllDocsQuery(),
                asList(term(KEYWORD_FIELD), term(GEO_POINT_FIELD)),
                NONE_DECORATOR,
                iw -> {},
                f -> fail("should throw exception")
            )
        );
    }

    public void testMinDocCount() throws IOException {
        testAggregation(new MatchAllDocsQuery(), fieldConfigs(asList(KEYWORD_FIELD, INT_FIELD)), b -> b.minDocCount(2), iw -> {
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(INT_FIELD, 1)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(INT_FIELD, 1)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("a")), new NumericDocValuesField(INT_FIELD, 2)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("b")), new NumericDocValuesField(INT_FIELD, 1)));
            iw.addDocument(asList(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef("c")), new NumericDocValuesField(INT_FIELD, 2)));
        }, h -> {
            assertEquals(1, h.getBuckets().size());
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertEquals(2, h.getBuckets().get(0).getDocCount());
        });
    }

    private void testAggregation(
        Query query,
        List<MultiTermsValuesSourceConfig> terms,
        Consumer<MultiTermsAggregationBuilder> decorator,
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder,
        Consumer<InternalMultiTerms> verify
    ) throws IOException {
        MultiTermsAggregationBuilder builder = createTestAggregatorBuilder(terms);
        if (decorator != NONE_DECORATOR) {
            decorator.accept(builder);
        }
        testCase(builder, query, indexBuilder, verify, mappedFieldTypeMap.values().toArray(new MappedFieldType[] {}));
    }

    private MultiTermsValuesSourceConfig term(String field) {
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        // ZoneId timeZone = randomBoolean() ? randomZone() : null;
        TermQueryBuilder filter = randomBoolean() ? QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;
        ValueType userValueTypeHint = randomBoolean()
            ? randomFrom(ValueType.STRING, ValueType.LONG, ValueType.DOUBLE, ValueType.DATE, ValueType.IP)
            : null;
        // String format = randomBoolean() ? randomNumericDocValueFormat().toString() : null;
        return new MultiTermsValuesSourceConfig.Builder().setFieldName(field)
            // .setMissing(missing)
            // .setScript(null)
            // .setTimeZone(timeZone)
            // .setFilter(filter)
            // .setUserValueTypeHint(userValueTypeHint)
            // .setFormat(format)
            .build();
    }

    private MultiTermsAggregationBuilder createTestAggregatorBuilder(List<MultiTermsValuesSourceConfig> termsConfig) {
        MultiTermsAggregationBuilder factory = new MultiTermsAggregationBuilder(AGG_NAME);
        factory.terms(termsConfig);

        if (randomBoolean()) {
            factory.size(randomIntBetween(10, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(10, Integer.MAX_VALUE));
        }
        // if (randomBoolean()) {
        // int minDocCount = randomInt(4);
        // switch (minDocCount) {
        // case 0:
        // break;
        // case 1:
        // case 2:
        // case 3:
        // case 4:
        // minDocCount = randomIntBetween(0, Integer.MAX_VALUE);
        // break;
        // default:
        // fail();
        // }
        // factory.minDocCount(minDocCount);
        // }
        // if (randomBoolean()) {
        // int shardMinDocCount = randomInt(4);
        // switch (shardMinDocCount) {
        // case 0:
        // break;
        // case 1:
        // case 2:
        // case 3:
        // case 4:
        // shardMinDocCount = randomIntBetween(0, Integer.MAX_VALUE);
        // break;
        // default:
        // fail();
        // }
        // factory.shardMinDocCount(shardMinDocCount);
        // }
        // if (randomBoolean()) {
        // factory.collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()));
        // }
        if (randomBoolean()) {
            factory.showTermDocCountError(randomBoolean());
        }
        return factory;
    }

    private List<MultiTermsValuesSourceConfig> fieldConfigs(List<String> terms) {
        List<MultiTermsValuesSourceConfig> termConfigs = new ArrayList<>();
        for (String term : terms) {
            termConfigs.add(term(term));
        }
        return termConfigs;
    }

    private static DateFieldMapper.DateFieldType dateFieldType(String name) {
        return new DateFieldMapper.DateFieldType(
            name,
            true,
            false,
            true,
            DateFormatter.forPattern("date"),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }
}
