/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.json

import spock.lang.Specification

class JsonWriterSettingsSpecification extends Specification {

    def 'test defaults'() {
        when:
        def settings = new JsonWriterSettings();

        then:
        !settings.isIndent()
        settings.getOutputMode() == JsonMode.STRICT

        when:
        settings = JsonWriterSettings.builder().build();

        then:
        !settings.isIndent()
        settings.getOutputMode() == JsonMode.RELAXED
        settings.getMaxLength() == 0
    }


    def 'test output mode'() {
        when:
        def settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build()

        then:
        settings.getOutputMode() == JsonMode.SHELL
    }

    def 'test indent defaults'() {
        when:
        def settings = JsonWriterSettings.builder().indent(true).build()

        then:
        settings.isIndent()
        settings.getIndentCharacters() == '  '
        settings.getNewLineCharacters() == System.getProperty('line.separator')
    }

    def 'test indent settings'() {
        when:
        def settings = JsonWriterSettings.builder()
                .indent(true).indentCharacters('\t').newLineCharacters('\r\n').build()

        then:
        settings.getIndentCharacters() == '\t'
        settings.getNewLineCharacters() == '\r\n'
    }

    def 'test max length setting'() {
        when:
        def settings = JsonWriterSettings.builder()
                .maxLength(100).build()

        then:
        settings.getMaxLength() == 100
    }

    def 'test constructors'() {
        when:
        def settings = new JsonWriterSettings()

        then:
        !settings.isIndent()
        settings.getOutputMode() == JsonMode.STRICT
        settings.getMaxLength() == 0

        when:
        settings = new JsonWriterSettings(JsonMode.SHELL)

        then:
        settings.getOutputMode() == JsonMode.SHELL

        when:
        settings = new JsonWriterSettings(true)

        then:
        settings.isIndent()

        when:
        settings = new JsonWriterSettings(JsonMode.SHELL, true)

        then:
        settings.getOutputMode() == JsonMode.SHELL
        settings.isIndent()

        when:
        settings = new JsonWriterSettings(JsonMode.SHELL, '\t')

        then:
        settings.getOutputMode() == JsonMode.SHELL
        settings.isIndent()
        settings.getIndentCharacters() == '\t'

        when:
        settings = new JsonWriterSettings(JsonMode.SHELL, '\t', '\r')

        then:
        settings.getOutputMode() == JsonMode.SHELL
        settings.isIndent()
        settings.getIndentCharacters() == '\t'
        settings.getNewLineCharacters() == '\r'

    }

    def 'should use legacy extended json converters for strict mode'() {
        when:
        def settings = JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()

        then:
        settings.binaryConverter.class == LegacyExtendedJsonBinaryConverter
        settings.booleanConverter.class == JsonBooleanConverter
        settings.dateTimeConverter.class == LegacyExtendedJsonDateTimeConverter
        settings.decimal128Converter.class == ExtendedJsonDecimal128Converter
        settings.doubleConverter.class == JsonDoubleConverter
        settings.int32Converter.class == JsonInt32Converter
        settings.int64Converter.class == ExtendedJsonInt64Converter
        settings.javaScriptConverter.class == JsonJavaScriptConverter
        settings.maxKeyConverter.class == ExtendedJsonMaxKeyConverter
        settings.minKeyConverter.class == ExtendedJsonMinKeyConverter
        settings.nullConverter.class == JsonNullConverter
        settings.objectIdConverter.class == ExtendedJsonObjectIdConverter
        settings.regularExpressionConverter.class == LegacyExtendedJsonRegularExpressionConverter
        settings.stringConverter.class == JsonStringConverter
        settings.symbolConverter.class == JsonSymbolConverter
        settings.timestampConverter.class == ExtendedJsonTimestampConverter
        settings.undefinedConverter.class == ExtendedJsonUndefinedConverter
    }

    def 'should use extended json converters for extended json mode'() {
        when:
        def settings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

        then:
        settings.binaryConverter.class == ExtendedJsonBinaryConverter
        settings.booleanConverter.class == JsonBooleanConverter
        settings.dateTimeConverter.class == ExtendedJsonDateTimeConverter
        settings.decimal128Converter.class == ExtendedJsonDecimal128Converter
        settings.doubleConverter.class == ExtendedJsonDoubleConverter
        settings.int32Converter.class == ExtendedJsonInt32Converter
        settings.int64Converter.class == ExtendedJsonInt64Converter
        settings.javaScriptConverter.class == JsonJavaScriptConverter
        settings.maxKeyConverter.class == ExtendedJsonMaxKeyConverter
        settings.minKeyConverter.class == ExtendedJsonMinKeyConverter
        settings.nullConverter.class == JsonNullConverter
        settings.objectIdConverter.class == ExtendedJsonObjectIdConverter
        settings.regularExpressionConverter.class == ExtendedJsonRegularExpressionConverter
        settings.stringConverter.class == JsonStringConverter
        settings.symbolConverter.class == JsonSymbolConverter
        settings.timestampConverter.class == ExtendedJsonTimestampConverter
        settings.undefinedConverter.class == ExtendedJsonUndefinedConverter
    }

    def 'should use shell converters for shell mode'() {
        when:
        def settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build()

        then:
        settings.binaryConverter.class == ShellBinaryConverter
        settings.booleanConverter.class == JsonBooleanConverter
        settings.dateTimeConverter.class == ShellDateTimeConverter
        settings.decimal128Converter.class == ShellDecimal128Converter
        settings.doubleConverter.class == JsonDoubleConverter
        settings.int32Converter.class == JsonInt32Converter
        settings.int64Converter.class == ShellInt64Converter
        settings.javaScriptConverter.class == JsonJavaScriptConverter
        settings.maxKeyConverter.class == ShellMaxKeyConverter
        settings.minKeyConverter.class == ShellMinKeyConverter
        settings.nullConverter.class == JsonNullConverter
        settings.objectIdConverter.class == ShellObjectIdConverter
        settings.regularExpressionConverter.class == ShellRegularExpressionConverter
        settings.stringConverter.class == JsonStringConverter
        settings.symbolConverter.class == JsonSymbolConverter
        settings.timestampConverter.class == ShellTimestampConverter
        settings.undefinedConverter.class == ShellUndefinedConverter
    }

    def 'should set converters'() {
        given:
        def binaryConverter = new ShellBinaryConverter()
        def booleanConverter = new JsonBooleanConverter()
        def dateTimeConverter = new ShellDateTimeConverter()
        def decimal128Converter = new ShellDecimal128Converter()
        def doubleConverter = new JsonDoubleConverter()
        def int32Converter = new JsonInt32Converter()
        def int64Converter = new ShellInt64Converter()
        def javaScriptConverter = new JsonJavaScriptConverter()
        def maxKeyConverter = new ShellMaxKeyConverter()
        def minKeyConverter = new ShellMinKeyConverter()
        def nullConverter = new JsonNullConverter()
        def objectIdConverter = new ShellObjectIdConverter()
        def regularExpressionConverter = new ShellRegularExpressionConverter()
        def stringConverter = new JsonStringConverter()
        def symbolConverter = new JsonSymbolConverter()
        def timestampConverter = new ShellTimestampConverter()
        def undefinedConverter = new ShellUndefinedConverter()

        when:
        def settings = JsonWriterSettings.builder()
                .binaryConverter(binaryConverter)
                .booleanConverter(booleanConverter)
                .dateTimeConverter(dateTimeConverter)
                .decimal128Converter(decimal128Converter)
                .doubleConverter(doubleConverter)
                .int32Converter(int32Converter)
                .int64Converter(int64Converter)
                .javaScriptConverter(javaScriptConverter)
                .maxKeyConverter(maxKeyConverter)
                .minKeyConverter(minKeyConverter)
                .nullConverter(nullConverter)
                .objectIdConverter(objectIdConverter)
                .regularExpressionConverter(regularExpressionConverter)
                .stringConverter(stringConverter)
                .symbolConverter(symbolConverter)
                .timestampConverter(timestampConverter)
                .undefinedConverter(undefinedConverter)
                .build()

        then:
        settings.binaryConverter == binaryConverter
        settings.booleanConverter == booleanConverter
        settings.dateTimeConverter == dateTimeConverter
        settings.decimal128Converter == decimal128Converter
        settings.doubleConverter == doubleConverter
        settings.int32Converter == int32Converter
        settings.int64Converter == int64Converter
        settings.javaScriptConverter == javaScriptConverter
        settings.maxKeyConverter == maxKeyConverter
        settings.minKeyConverter == minKeyConverter
        settings.nullConverter == nullConverter
        settings.objectIdConverter == objectIdConverter
        settings.regularExpressionConverter == regularExpressionConverter
        settings.stringConverter == stringConverter
        settings.symbolConverter == symbolConverter
        settings.timestampConverter == timestampConverter
        settings.undefinedConverter == undefinedConverter
    }
}
