/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.json;

import org.bson.BsonBinary;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import static org.bson.assertions.Assertions.notNull;

/**
 * Settings to control the behavior of a {@code JSONWriter} instance.
 *
 * @see JsonWriter
 * @since 3.0
 */
public class JsonWriterSettings extends BsonWriterSettings {

    private static final JsonNullConverter JSON_NULL_CONVERTER = new JsonNullConverter();
    private static final JsonStringConverter JSON_STRING_CONVERTER = new JsonStringConverter();
    private static final JsonBooleanConverter JSON_BOOLEAN_CONVERTER = new JsonBooleanConverter();
    private static final JsonDoubleConverter JSON_DOUBLE_CONVERTER = new JsonDoubleConverter();
    private static final ExtendedJsonDoubleConverter EXTENDED_JSON_DOUBLE_CONVERTER = new ExtendedJsonDoubleConverter();
    private static final RelaxedExtendedJsonDoubleConverter RELAXED_EXTENDED_JSON_DOUBLE_CONVERTER =
            new RelaxedExtendedJsonDoubleConverter();
    private static final JsonInt32Converter JSON_INT_32_CONVERTER = new JsonInt32Converter();
    private static final ExtendedJsonInt32Converter EXTENDED_JSON_INT_32_CONVERTER = new ExtendedJsonInt32Converter();
    private static final JsonSymbolConverter JSON_SYMBOL_CONVERTER = new JsonSymbolConverter();
    private static final ExtendedJsonMinKeyConverter EXTENDED_JSON_MIN_KEY_CONVERTER = new ExtendedJsonMinKeyConverter();
    private static final ShellMinKeyConverter SHELL_MIN_KEY_CONVERTER = new ShellMinKeyConverter();
    private static final ExtendedJsonMaxKeyConverter EXTENDED_JSON_MAX_KEY_CONVERTER = new ExtendedJsonMaxKeyConverter();
    private static final ShellMaxKeyConverter SHELL_MAX_KEY_CONVERTER = new ShellMaxKeyConverter();
    private static final ExtendedJsonUndefinedConverter EXTENDED_JSON_UNDEFINED_CONVERTER = new ExtendedJsonUndefinedConverter();
    private static final ShellUndefinedConverter SHELL_UNDEFINED_CONVERTER = new ShellUndefinedConverter();
    private static final LegacyExtendedJsonDateTimeConverter LEGACY_EXTENDED_JSON_DATE_TIME_CONVERTER =
            new LegacyExtendedJsonDateTimeConverter();
    private static final ExtendedJsonDateTimeConverter EXTENDED_JSON_DATE_TIME_CONVERTER = new ExtendedJsonDateTimeConverter();
    private static final RelaxedExtendedJsonDateTimeConverter RELAXED_EXTENDED_JSON_DATE_TIME_CONVERTER =
            new RelaxedExtendedJsonDateTimeConverter();
    private static final ShellDateTimeConverter SHELL_DATE_TIME_CONVERTER = new ShellDateTimeConverter();
    private static final ExtendedJsonBinaryConverter EXTENDED_JSON_BINARY_CONVERTER = new ExtendedJsonBinaryConverter();
    private static final LegacyExtendedJsonBinaryConverter LEGACY_EXTENDED_JSON_BINARY_CONVERTER = new LegacyExtendedJsonBinaryConverter();
    private static final ShellBinaryConverter SHELL_BINARY_CONVERTER = new ShellBinaryConverter();
    private static final ExtendedJsonInt64Converter EXTENDED_JSON_INT_64_CONVERTER = new ExtendedJsonInt64Converter();
    private static final RelaxedExtendedJsonInt64Converter RELAXED_JSON_INT_64_CONVERTER = new RelaxedExtendedJsonInt64Converter();
    private static final ShellInt64Converter SHELL_INT_64_CONVERTER = new ShellInt64Converter();
    private static final ExtendedJsonDecimal128Converter EXTENDED_JSON_DECIMAL_128_CONVERTER = new ExtendedJsonDecimal128Converter();
    private static final ShellDecimal128Converter SHELL_DECIMAL_128_CONVERTER = new ShellDecimal128Converter();
    private static final ExtendedJsonObjectIdConverter EXTENDED_JSON_OBJECT_ID_CONVERTER = new ExtendedJsonObjectIdConverter();
    private static final ShellObjectIdConverter SHELL_OBJECT_ID_CONVERTER = new ShellObjectIdConverter();
    private static final ExtendedJsonTimestampConverter EXTENDED_JSON_TIMESTAMP_CONVERTER = new ExtendedJsonTimestampConverter();
    private static final ShellTimestampConverter SHELL_TIMESTAMP_CONVERTER = new ShellTimestampConverter();
    private static final ExtendedJsonRegularExpressionConverter EXTENDED_JSON_REGULAR_EXPRESSION_CONVERTER =
            new ExtendedJsonRegularExpressionConverter();
    private static final LegacyExtendedJsonRegularExpressionConverter LEGACY_EXTENDED_JSON_REGULAR_EXPRESSION_CONVERTER =
            new LegacyExtendedJsonRegularExpressionConverter();
    private static final ShellRegularExpressionConverter SHELL_REGULAR_EXPRESSION_CONVERTER = new ShellRegularExpressionConverter();

    private final boolean indent;
    private final String newLineCharacters;
    private final String indentCharacters;
    private final JsonMode outputMode;
    private final Converter<BsonNull> nullConverter;
    private final Converter<String> stringConverter;
    private final Converter<Long> dateTimeConverter;
    private final Converter<BsonBinary> binaryConverter;
    private final Converter<Boolean> booleanConverter;
    private final Converter<Double> doubleConverter;
    private final Converter<Integer> int32Converter;
    private final Converter<Long> int64Converter;
    private final Converter<Decimal128> decimal128Converter;
    private final Converter<ObjectId> objectIdConverter;
    private final Converter<BsonTimestamp> timestampConverter;
    private final Converter<BsonRegularExpression> regularExpressionConverter;
    private final Converter<String> symbolConverter;
    private final Converter<BsonUndefined> undefinedConverter;
    private final Converter<BsonMinKey> minKeyConverter;
    private final Converter<BsonMaxKey> maxKeyConverter;
    private final Converter<String> javaScriptConverter;

    /**
     * Create a builder for JsonWriterSettings, which are immutable.
     * <p>
     *     Defaults to {@link JsonMode#RELAXED}
     * </p>
     *
     * @return a Builder instance
     * @since 3.5
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new instance with default values for all properties.
     * <p>
     *     Defaults to {@link JsonMode#STRICT}
     * </p>
     *
     * @deprecated Prefer {@link #builder()}, but note that the default output mode is different for that method
     */
    @Deprecated
    public JsonWriterSettings() {
        this(builder().outputMode(JsonMode.STRICT));
    }

    /**
     * Creates a new instance with the given output mode and default values for all other properties.
     *
     * @param outputMode the output mode
     * @deprecated Use the {@link Builder} instead
     */
    @Deprecated
    public JsonWriterSettings(final JsonMode outputMode) {
        this(builder().outputMode(outputMode));
    }

    /**
     * Creates a new instance with indent mode enabled, and the default value for all other properties.
     *
     * @param indent whether indent mode is enabled
     * @deprecated Use the {@link Builder} instead
     */
    @Deprecated
    public JsonWriterSettings(final boolean indent) {
        this(builder().indent(indent));
    }

    /**
     * Creates a new instance with the given output mode, indent mode enabled, and the default value for all other properties.
     *
     * @param outputMode the output mode
     * @param indent     whether indent mode is enabled
     * @deprecated Use the {@link Builder} instead
     */
    @Deprecated
    public JsonWriterSettings(final JsonMode outputMode, final boolean indent) {
        this(builder().outputMode(outputMode).indent(indent));
    }

    /**
     * Creates a new instance with the given values for all properties, indent mode enabled and the default value of {@code
     * newLineCharacters}.
     *
     * @param outputMode       the output mode
     * @param indentCharacters the indent characters
     * @deprecated Use the {@link Builder} instead
     */
    @Deprecated
    public JsonWriterSettings(final JsonMode outputMode, final String indentCharacters) {
        this(builder().outputMode(outputMode).indent(true).indentCharacters(indentCharacters));
    }

    /**
     * Creates a new instance with the given values for all properties and indent mode enabled.
     *
     * @param outputMode        the output mode
     * @param indentCharacters  the indent characters
     * @param newLineCharacters the new line character(s) to use
     * @deprecated Use the {@link Builder} instead
     */
    @Deprecated
    public JsonWriterSettings(final JsonMode outputMode, final String indentCharacters, final String newLineCharacters) {
        this(builder().outputMode(outputMode).indent(true).indentCharacters(indentCharacters).newLineCharacters(newLineCharacters));
    }

    @SuppressWarnings("deprecation")
    private JsonWriterSettings(final Builder builder) {
        indent = builder.indent;
        newLineCharacters = builder.newLineCharacters != null ? builder.newLineCharacters : System.getProperty("line.separator");
        indentCharacters = builder.indentCharacters;
        outputMode = builder.outputMode;

        if (builder.nullConverter != null) {
            nullConverter = builder.nullConverter;
        } else {
            nullConverter = JSON_NULL_CONVERTER;
        }

        if (builder.stringConverter != null) {
            stringConverter = builder.stringConverter;
        } else {
            stringConverter = JSON_STRING_CONVERTER;
        }

        if (builder.booleanConverter != null) {
            booleanConverter = builder.booleanConverter;
        } else {
            booleanConverter = JSON_BOOLEAN_CONVERTER;
        }

        if (builder.doubleConverter != null) {
            doubleConverter = builder.doubleConverter;
        } else if (outputMode == JsonMode.EXTENDED) {
            doubleConverter = EXTENDED_JSON_DOUBLE_CONVERTER;
        } else if (outputMode == JsonMode.RELAXED) {
            doubleConverter = RELAXED_EXTENDED_JSON_DOUBLE_CONVERTER;
        } else {
            doubleConverter = JSON_DOUBLE_CONVERTER;
        }

        if (builder.int32Converter != null) {
            int32Converter = builder.int32Converter;
        } else if (outputMode == JsonMode.EXTENDED) {
            int32Converter = EXTENDED_JSON_INT_32_CONVERTER;
        }
        else {
            int32Converter = JSON_INT_32_CONVERTER;
        }

        if (builder.symbolConverter != null) {
            symbolConverter = builder.symbolConverter;
        } else {
            symbolConverter = JSON_SYMBOL_CONVERTER;
        }

        if (builder.javaScriptConverter != null) {
            javaScriptConverter = builder.javaScriptConverter;
        } else {
            javaScriptConverter = new JsonJavaScriptConverter();
        }

        if (builder.minKeyConverter != null) {
            minKeyConverter = builder.minKeyConverter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            minKeyConverter = EXTENDED_JSON_MIN_KEY_CONVERTER;
        } else {
            minKeyConverter = SHELL_MIN_KEY_CONVERTER;
        }

        if (builder.maxKeyConverter != null) {
            maxKeyConverter = builder.maxKeyConverter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            maxKeyConverter = EXTENDED_JSON_MAX_KEY_CONVERTER;
        } else {
            maxKeyConverter = SHELL_MAX_KEY_CONVERTER;
        }

        if (builder.undefinedConverter != null) {
            undefinedConverter = builder.undefinedConverter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            undefinedConverter = EXTENDED_JSON_UNDEFINED_CONVERTER;
        } else {
            undefinedConverter = SHELL_UNDEFINED_CONVERTER;
        }

        if (builder.dateTimeConverter != null) {
            dateTimeConverter = builder.dateTimeConverter;
        } else if (outputMode == JsonMode.STRICT) {
            dateTimeConverter = LEGACY_EXTENDED_JSON_DATE_TIME_CONVERTER;
        } else if (outputMode == JsonMode.EXTENDED) {
            dateTimeConverter = EXTENDED_JSON_DATE_TIME_CONVERTER;
        } else if (outputMode == JsonMode.RELAXED) {
            dateTimeConverter = RELAXED_EXTENDED_JSON_DATE_TIME_CONVERTER;
        } else {
            dateTimeConverter = SHELL_DATE_TIME_CONVERTER;
        }

        if (builder.binaryConverter != null) {
            binaryConverter = builder.binaryConverter;
        } else if (outputMode == JsonMode.STRICT) {
            binaryConverter = LEGACY_EXTENDED_JSON_BINARY_CONVERTER;
        } else if (outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            binaryConverter = EXTENDED_JSON_BINARY_CONVERTER;
        } else {
            binaryConverter = SHELL_BINARY_CONVERTER;
        }

        if (builder.int64Converter != null) {
            int64Converter = builder.int64Converter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED) {
            int64Converter = EXTENDED_JSON_INT_64_CONVERTER;
        } else if (outputMode == JsonMode.RELAXED) {
            int64Converter = RELAXED_JSON_INT_64_CONVERTER;
        } else {
            int64Converter = SHELL_INT_64_CONVERTER;
        }

        if (builder.decimal128Converter != null) {
            decimal128Converter = builder.decimal128Converter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            decimal128Converter = EXTENDED_JSON_DECIMAL_128_CONVERTER;
        } else {
            decimal128Converter = SHELL_DECIMAL_128_CONVERTER;
        }

        if (builder.objectIdConverter != null) {
            objectIdConverter = builder.objectIdConverter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            objectIdConverter = EXTENDED_JSON_OBJECT_ID_CONVERTER;
        } else {
            objectIdConverter = SHELL_OBJECT_ID_CONVERTER;
        }

        if (builder.timestampConverter != null) {
            timestampConverter = builder.timestampConverter;
        } else if (outputMode == JsonMode.STRICT || outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            timestampConverter = EXTENDED_JSON_TIMESTAMP_CONVERTER;
        } else {
            timestampConverter = SHELL_TIMESTAMP_CONVERTER;
        }

        if (builder.regularExpressionConverter != null) {
            regularExpressionConverter = builder.regularExpressionConverter;
        } else if (outputMode == JsonMode.EXTENDED || outputMode == JsonMode.RELAXED) {
            regularExpressionConverter = EXTENDED_JSON_REGULAR_EXPRESSION_CONVERTER;
        } else if (outputMode == JsonMode.STRICT) {
            regularExpressionConverter = LEGACY_EXTENDED_JSON_REGULAR_EXPRESSION_CONVERTER;
        } else {
            regularExpressionConverter = SHELL_REGULAR_EXPRESSION_CONVERTER;
        }
    }

    /**
     * The indentation mode.  If true, output will be indented.  Otherwise, it will all be on the same line. The default value is {@code
     * false}.
     *
     * @return whether output should be indented.
     */
    public boolean isIndent() {
        return indent;
    }

    /**
     * The new line character(s) to use if indent mode is enabled.  The default value is {@code System.getProperty("line.separator")}.
     *
     * @return the new line character(s) to use.
     */
    public String getNewLineCharacters() {
        return newLineCharacters;
    }

    /**
     * The indent characters to use if indent mode is enabled.  The default value is two spaces.
     *
     * @return the indent character(s) to use.
     */
    public String getIndentCharacters() {
        return indentCharacters;
    }

    /**
     * The output mode to use.  The default value is {@code }JSONMode.STRICT}.
     *
     * @return the output mode.
     */
    public JsonMode getOutputMode() {
        return outputMode;
    }

    /**
     * A converter from BSON Null values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonNull> getNullConverter() {
        return nullConverter;
    }

    /**
     * A converter from BSON String values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<String> getStringConverter() {
        return stringConverter;
    }

    /**
     * A converter from BSON Binary values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonBinary> getBinaryConverter() {
        return binaryConverter;
    }

    /**
     * A converter from BSON Boolean values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Boolean> getBooleanConverter() {
        return booleanConverter;
    }

    /**
     * A converter from BSON DateTime values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Long> getDateTimeConverter() {
        return dateTimeConverter;
    }

    /**
     * A converter from BSON Double values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Double> getDoubleConverter() {
        return doubleConverter;
    }

    /**
     * A converter from BSON Int32 values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Integer> getInt32Converter() {
        return int32Converter;
    }

    /**
     * A converter from BSON Int64 values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Long> getInt64Converter() {
        return int64Converter;
    }

    /**
     * A converter from BSON Decimal128 values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<Decimal128> getDecimal128Converter() {
        return decimal128Converter;
    }

    /**
     * A converter from BSON ObjectId values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<ObjectId> getObjectIdConverter() {
        return objectIdConverter;
    }

    /**
     * A converter from BSON RegularExpression values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonRegularExpression> getRegularExpressionConverter() {
        return regularExpressionConverter;
    }

    /**
     * A converter from BSON Timestamp values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonTimestamp> getTimestampConverter() {
        return timestampConverter;
    }

    /**
     * A converter from BSON Symbol values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<String> getSymbolConverter() {
        return symbolConverter;
    }

    /**
     * A converter from BSON MinKey values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonMinKey> getMinKeyConverter() {
        return minKeyConverter;
    }

    /**
     * A converter from BSON MaxKey values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonMaxKey> getMaxKeyConverter() {
        return maxKeyConverter;
    }

    /**
     * A converter from BSON Undefined values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<BsonUndefined> getUndefinedConverter() {
        return undefinedConverter;
    }

    /**
     * A converter from BSON JavaScript values to JSON.
     *
     * @return this
     * @since 3.5
     */
    public Converter<String> getJavaScriptConverter() {
        return javaScriptConverter;
    }

    /**
     * A builder for JsonWriterSettings
     *
     * @since 3.5
     */
    @SuppressWarnings("deprecation")
    public static final class Builder {
        private boolean indent;
        private String newLineCharacters = System.getProperty("line.separator");
        private String indentCharacters = "  ";
        private JsonMode outputMode = JsonMode.RELAXED;
        private Converter<BsonNull> nullConverter;
        private Converter<String> stringConverter;
        private Converter<Long> dateTimeConverter;
        private Converter<BsonBinary> binaryConverter;
        private Converter<Boolean> booleanConverter;
        private Converter<Double> doubleConverter;
        private Converter<Integer> int32Converter;
        private Converter<Long> int64Converter;
        private Converter<Decimal128> decimal128Converter;
        private Converter<ObjectId> objectIdConverter;
        private Converter<BsonTimestamp> timestampConverter;
        private Converter<BsonRegularExpression> regularExpressionConverter;
        private Converter<String> symbolConverter;
        private Converter<BsonUndefined> undefinedConverter;
        private Converter<BsonMinKey> minKeyConverter;
        private Converter<BsonMaxKey> maxKeyConverter;
        private Converter<String> javaScriptConverter;

        /**
         * Build a JsonWriterSettings instance.
         *
         * @return a JsonWriterSettings instance
         */
        public JsonWriterSettings build() {
            return new JsonWriterSettings(this);
        }

        /**
         * Sets whether indentation is enabled, which defaults to false.
         *
         * @param indent whether indentation is enabled
         * @return this
         */
        public Builder indent(final boolean indent) {
            this.indent = indent;
            return this;
        }

        /**
         * Sets the new line character string to use when indentation is enabled, which defaults to
         * {@code System.getProperty("line.separator")}.
         *
         * @param newLineCharacters the non-null new line character string
         * @return this
         */
        public Builder newLineCharacters(final String newLineCharacters) {
            notNull("newLineCharacters", newLineCharacters);
            this.newLineCharacters = newLineCharacters;
            return this;
        }

        /**
         * Sets the indent character string to use when indentation is enabled, which defaults to two spaces.
         *
         * @param indentCharacters the non-null indent character string
         * @return this
         */
        public Builder indentCharacters(final String indentCharacters) {
            notNull("indentCharacters", indentCharacters);
            this.indentCharacters = indentCharacters;
            return this;
        }

        /**
         * Sets the output mode, which defaults to {@link JsonMode#RELAXED}.
         *
         * @param outputMode the non-null output mode
         * @return this
         */
        public Builder outputMode(final JsonMode outputMode) {
            notNull("outputMode", outputMode);
            this.outputMode = outputMode;
            return this;
        }

        /**
         * Sets the converter from BSON Null values to JSON.
         *
         * @param nullConverter the converter
         * @return this
         */
        public Builder nullConverter(final Converter<BsonNull> nullConverter) {
            this.nullConverter = nullConverter;
            return this;
        }

        /**
         * Sets the converter from BSON String values to JSON.
         *
         * @param stringConverter the converter
         * @return this
         */
        public Builder stringConverter(final Converter<String> stringConverter) {
            this.stringConverter = stringConverter;
            return this;
        }

        /**
         * Sets the converter from BSON DateTime values to JSON.
         *
         * @param dateTimeConverter the converter
         * @return this
         */
        public Builder dateTimeConverter(final Converter<Long> dateTimeConverter) {
            this.dateTimeConverter = dateTimeConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Binary values to JSON.
         *
         * @param binaryConverter the converter
         * @return this
         */
        public Builder binaryConverter(final Converter<BsonBinary> binaryConverter) {
            this.binaryConverter = binaryConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Boolean values to JSON.
         *
         * @param booleanConverter the converter
         * @return this
         */
        public Builder booleanConverter(final Converter<Boolean> booleanConverter) {
            this.booleanConverter = booleanConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Double values to JSON.
         *
         * @param doubleConverter the converter
         * @return this
         */
        public Builder doubleConverter(final Converter<Double> doubleConverter) {
            this.doubleConverter = doubleConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Int32 values to JSON.
         *
         * @param int32Converter the converter
         * @return this
         */
        public Builder int32Converter(final Converter<Integer> int32Converter) {
            this.int32Converter = int32Converter;
            return this;
        }

        /**
         * Sets the converter from BSON Int64 values to JSON.
         *
         * @param int64Converter the converter
         * @return this
         */
        public Builder int64Converter(final Converter<Long> int64Converter) {
            this.int64Converter = int64Converter;
            return this;
        }

        /**
         * Sets the converter from BSON Decimal128 values to JSON.
         *
         * @param decimal128Converter the converter
         * @return this
         */
        public Builder decimal128Converter(final Converter<Decimal128> decimal128Converter) {
            this.decimal128Converter = decimal128Converter;
            return this;
        }

        /**
         * Sets the converter from BSON ObjectId values to JSON.
         *
         * @param objectIdConverter the converter
         * @return this
         */
        public Builder objectIdConverter(final Converter<ObjectId> objectIdConverter) {
            this.objectIdConverter = objectIdConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Timestamp values to JSON.
         *
         * @param timestampConverter the converter
         * @return this
         */
        public Builder timestampConverter(final Converter<BsonTimestamp> timestampConverter) {
            this.timestampConverter = timestampConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Regular Expression values to JSON.
         *
         * @param regularExpressionConverter the converter
         * @return this
         */
        public Builder regularExpressionConverter(final Converter<BsonRegularExpression> regularExpressionConverter) {
            this.regularExpressionConverter = regularExpressionConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Symbol values to JSON.
         *
         * @param symbolConverter the converter
         * @return this
         */
        public Builder symbolConverter(final Converter<String> symbolConverter) {
            this.symbolConverter = symbolConverter;
            return this;
        }

        /**
         * Sets the converter from BSON MinKey values to JSON.
         *
         * @param minKeyConverter the converter
         * @return this
         */
        public Builder minKeyConverter(final Converter<BsonMinKey> minKeyConverter) {
            this.minKeyConverter = minKeyConverter;
            return this;
        }

        /**
         * Sets the converter from BSON MaxKey values to JSON.
         *
         * @param maxKeyConverter the converter
         * @return this
         */
        public Builder maxKeyConverter(final Converter<BsonMaxKey> maxKeyConverter) {
            this.maxKeyConverter = maxKeyConverter;
            return this;
        }

        /**
         * Sets the converter from BSON Undefined values to JSON.
         *
         * @param undefinedConverter the converter
         * @return this
         */
        public Builder undefinedConverter(final Converter<BsonUndefined> undefinedConverter) {
            this.undefinedConverter = undefinedConverter;
            return this;
        }

        /**
         * Sets the converter from BSON JavaScript values to JSON.
         *
         * @param javaScriptConverter the converter
         * @return this
         */
        public Builder javaScriptConverter(final Converter<String> javaScriptConverter) {
            this.javaScriptConverter = javaScriptConverter;
            return this;
        }

        private Builder() {
        }
    }
}
