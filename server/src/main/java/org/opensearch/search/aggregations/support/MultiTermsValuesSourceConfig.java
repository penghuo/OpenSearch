/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.time.DateUtils;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A configuration that tells multi-terms aggregations how to retrieve data from index
 * in order to run a specific aggregation.
 */
public class MultiTermsValuesSourceConfig implements Writeable, ToXContentObject {
    private final String fieldName;
    private final Object missing;
    private final Script script;
    private final ZoneId timeZone;
    private final QueryBuilder filter;

    private final ValueType userValueTypeHint;
    private final String format;

    private static final String NAME = "field_config";
    public static final ParseField FILTER = new ParseField("filter");

    public interface ParserSupplier {
        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> apply(
            Boolean scriptable,
            Boolean timezoneAware,
            Boolean filtered,
            Boolean valueTypeHinted,
            Boolean formatted
        );
    }

    public static final MultiTermsValuesSourceConfig.ParserSupplier PARSER = (
        scriptable,
        timezoneAware,
        filtered,
        valueTypeHinted,
        formatted) -> {

        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> parser = new ObjectParser<>(
            MultiTermsValuesSourceConfig.NAME,
            MultiTermsValuesSourceConfig.Builder::new
        );

        parser.declareString(MultiTermsValuesSourceConfig.Builder::setFieldName, ParseField.CommonFields.FIELD);
        parser.declareField(
            MultiTermsValuesSourceConfig.Builder::setMissing,
            XContentParser::objectText,
            ParseField.CommonFields.MISSING,
            ObjectParser.ValueType.VALUE
        );

        if (scriptable) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
        }

        if (timezoneAware) {
            parser.declareField(MultiTermsValuesSourceConfig.Builder::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }

        if (filtered) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setFilter,
                (p, context) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                FILTER,
                ObjectParser.ValueType.OBJECT
            );
        }

        if (valueTypeHinted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setUserValueTypeHint,
                p -> ValueType.lenientParse(p.text()),
                ValueType.VALUE_TYPE,
                ObjectParser.ValueType.STRING
            );
        }

        if (formatted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setFormat,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }

        return parser;
    };

    protected MultiTermsValuesSourceConfig(
        String fieldName,
        Object missing,
        Script script,
        ZoneId timeZone,
        QueryBuilder filter,
        ValueType userValueTypeHint,
        String format
    ) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
        this.filter = filter;
        this.userValueTypeHint = userValueTypeHint;
        this.format = format;
    }

    public MultiTermsValuesSourceConfig(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            this.fieldName = in.readOptionalString();
        } else {
            this.fieldName = in.readString();
        }
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        if (in.getVersion().before(LegacyESVersion.V_7_0_0)) {
            this.timeZone = DateUtils.dateTimeZoneToZoneId(in.readOptionalTimeZone());
        } else {
            this.timeZone = in.readOptionalZoneId();
        }
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        } else {
            this.filter = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_0_0)) {
            this.userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
            this.format = in.readOptionalString();
        } else {
            this.userValueTypeHint = null;
            this.format = null;
        }
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public String getFieldName() {
        return fieldName;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public ValueType getUserValueTypeHint() {
        return userValueTypeHint;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            out.writeOptionalString(fieldName);
        } else {
            out.writeString(fieldName);
        }
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        if (out.getVersion().before(LegacyESVersion.V_7_0_0)) {
            out.writeOptionalTimeZone(DateUtils.zoneIdToDateTimeZone(timeZone));
        } else {
            out.writeOptionalZoneId(timeZone);
        }
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            out.writeOptionalNamedWriteable(filter);
        }
        if (out.getVersion().onOrAfter(Version.V_2_0_0)) {
            out.writeOptionalWriteable(userValueTypeHint);
            out.writeOptionalString(format);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (missing != null) {
            builder.field(ParseField.CommonFields.MISSING.getPreferredName(), missing);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        }
        if (fieldName != null) {
            builder.field(ParseField.CommonFields.FIELD.getPreferredName(), fieldName);
        }
        if (timeZone != null) {
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone.getId());
        }
        if (filter != null) {
            builder.field(FILTER.getPreferredName());
            filter.toXContent(builder, params);
        }
        if (userValueTypeHint != null) {
            builder.field(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field(AggregationBuilder.CommonFields.FORMAT.getPreferredName(), format);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiTermsValuesSourceConfig that = (MultiTermsValuesSourceConfig) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(missing, that.missing)
            && Objects.equals(script, that.script)
            && Objects.equals(timeZone, that.timeZone)
            && Objects.equals(filter, that.filter)
            && Objects.equals(userValueTypeHint, that.userValueTypeHint)
            && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, missing, script, timeZone, filter, userValueTypeHint, format);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {
        private String fieldName;
        private Object missing = null;
        private Script script = null;
        private ZoneId timeZone = null;
        private QueryBuilder filter = null;
        private ValueType userValueTypeHint = null;
        private String format;

        public String getFieldName() {
            return fieldName;
        }

        public MultiTermsValuesSourceConfig.Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Object getMissing() {
            return missing;
        }

        public MultiTermsValuesSourceConfig.Builder setMissing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Script getScript() {
            return script;
        }

        public MultiTermsValuesSourceConfig.Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        public ZoneId getTimeZone() {
            return timeZone;
        }

        public MultiTermsValuesSourceConfig.Builder setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public MultiTermsValuesSourceConfig.Builder setFilter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public ValueType getUserValueTypeHint() {
            return userValueTypeHint;
        }

        public MultiTermsValuesSourceConfig.Builder setUserValueTypeHint(ValueType userValueTypeHint) {
            this.userValueTypeHint = userValueTypeHint;
            return this;
        }

        public String getFormat() {
            return format;
        }

        public MultiTermsValuesSourceConfig.Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        public MultiTermsValuesSourceConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be null.  "
                        + "Please specify one or the other."
                );
            }
            return new MultiTermsValuesSourceConfig(fieldName, missing, script, timeZone, filter, userValueTypeHint, format);
        }
    }
}
