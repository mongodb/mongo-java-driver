/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.json

import org.bson.BsonInvalidOperationException
import spock.lang.Specification

import static java.lang.String.format

class StrictCharacterStreamJsonWriterSpecification extends Specification {

    private StringWriter stringWriter
    private StrictCharacterStreamJsonWriter writer

    def setup() {
        stringWriter = new StringWriter()
        writer = new StrictCharacterStreamJsonWriter(stringWriter, StrictCharacterStreamJsonWriterSettings.builder().build())
    }

    def 'should write empty document'() {
        when:
        writer.writeStartObject()
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ }'
    }

    def 'should write empty array'() {
        when:
        writer.writeStartArray()
        writer.writeEndArray()

        then:
        stringWriter.toString() == '[]'
    }

    def 'should write null'() {
        when:
        writer.writeStartObject()
        writer.writeNull('n')
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "n" : null }'
    }

    def 'should write boolean'() {
        when:
        writer.writeStartObject()
        writer.writeBoolean('b1', true)
        writer.writeBoolean('b2', false)
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "b1" : true, "b2" : false }'
    }

    def 'should write number'() {
        when:
        writer.writeStartObject()
        writer.writeNumber('n', '42')
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "n" : 42 }'
    }

    def 'should write string'() {
        when:
        writer.writeStartObject()
        writer.writeString('n', '42')
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "n" : "42" }'
    }

    def 'should write unquoted string'() {
        when:
        writer.writeStartObject()
        writer.writeRaw('s', 'NumberDecimal("42.0")')
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "s" : NumberDecimal("42.0") }'
    }

    def 'should write document'() {
        when:
        writer.writeStartObject()
        writer.writeStartObject('d')
        writer.writeEndObject()
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "d" : { } }'
    }

    def 'should write array'() {
        when:
        writer.writeStartObject()
        writer.writeStartArray('a')
        writer.writeEndArray()
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "a" : [] }'
    }

    def 'should write array of values'() {
        when:
        writer.writeStartObject()
        writer.writeStartArray('a')
        writer.writeNumber('1')
        writer.writeNull()
        writer.writeString('str')
        writer.writeEndArray()
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "a" : [1, null, "str"] }'
    }

    def 'should write strings'() {
        when:
        writer.writeStartObject()
        writer.writeString('str', value)
        writer.writeEndObject()

        then:
        stringWriter.toString() == '{ "str" : ' + expected + ' }'

        where:
        value                | expected
        ''                   | '""'
        ' '                  | '" "'
        'a'                  | '"a"'
        'ab'                 | '"ab"'
        'abc'                | '"abc"'
        'abc\u0000def'       | '"abc\\u0000def"'
        '\\'                 | '"\\\\"'
        '\''                 | '"\'"'
        '"'                  | '"\\""'
        '\0'                 | '"\\u0000"'
        '\b'                 | '"\\b"'
        '\f'                 | '"\\f"'
        '\n'                 | '"\\n"'
        '\r'                 | '"\\r"'
        '\t'                 | '"\\t"'
        '\u0080'             | '"\\u0080"'
        '\u0080\u0081'       | '"\\u0080\\u0081"'
        '\u0080\u0081\u0082' | '"\\u0080\\u0081\\u0082"'
    }

    def 'should indent one element'() {
        given:
        writer = new StrictCharacterStreamJsonWriter(stringWriter, StrictCharacterStreamJsonWriterSettings.builder().indent(true).build())

        when:
        writer.writeStartObject()
        writer.writeString('name', 'value')
        writer.writeEndObject()

        then:
        stringWriter.toString() == format('{%n  "name" : "value"%n}')
    }

    def 'should indent one element with indent and newline characters'() {
        given:
        writer = new StrictCharacterStreamJsonWriter(stringWriter, StrictCharacterStreamJsonWriterSettings.builder()
                .indent(true)
                .indentCharacters('\t')
                .newLineCharacters('\r')
                .build())

        when:
        writer.writeStartObject()
        writer.writeString('name', 'value')
        writer.writeEndObject()

        then:
        stringWriter.toString() == format('{\r\t"name" : "value"\r}')
    }

    def 'should indent two elements'() {
        given:
        writer = new StrictCharacterStreamJsonWriter(stringWriter, StrictCharacterStreamJsonWriterSettings.builder().indent(true).build())

        when:
        writer.writeStartObject()
        writer.writeString('a', 'x')
        writer.writeString('b', 'y')
        writer.writeEndObject()

        then:
        stringWriter.toString() == format('{%n  "a" : "x",%n  "b" : "y"%n}')
    }

    def 'should indent embedded document'() {
        given:
        writer = new StrictCharacterStreamJsonWriter(stringWriter, StrictCharacterStreamJsonWriterSettings.builder().indent(true).build())

        when:
        writer.writeStartObject()
        writer.writeStartObject('doc')
        writer.writeNumber('a', '1')
        writer.writeNumber('b', '2')
        writer.writeEndObject()
        writer.writeEndObject()

        then:
        stringWriter.toString() == format('{%n  "doc" : {%n    "a" : 1,%n    "b" : 2%n  }%n}')
    }

    def shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument() {
        when:
        writer.writeBoolean('b1', true)

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowExceptionForNameWhenWritingBeforeStartingDocument() {
        when:
        writer.writeName('name')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowExceptionForStringWhenStateIsValue() {
        given:
        writer.writeStartObject()

        when:
        writer.writeString('SomeString')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue() {
        given:
        writer.writeStartObject()

        when:
        writer.writeEndArray()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowExceptionWhenWritingASecondName() {
        given:
        writer.writeStartObject()
        writer.writeName('f1')

        when:
        writer.writeName('i2')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten() {
        given:
        writer.writeStartObject()
        writer.writeName('f1')

        when:
        writer.writeEndObject()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenTryingToWriteAValue() {
        when:
        writer.writeString('i2')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenWritingANameInAnArray() {
        given:
        writer.writeStartObject()
        writer.writeStartArray('f2')

        when:
        writer.writeName('i3')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray() {
        given:
        writer.writeStartObject()
        writer.writeStartArray('f2')

        when:
        writer.writeEndObject()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenEndingAnArrayInASubDocument() {
        given:
        writer.writeStartObject()
        writer.writeStartArray('f2')
        writer.writeStartObject()

        when:
        writer.writeEndArray()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenEndingAnArrayWhenValueIsExpected() {
        given:
        writer.writeStartObject()
        writer.writeName('a')

        when:
        writer.writeEndArray()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray() {
        given:
        writer.writeStartObject()
        writer.writeStartArray('f2')
        writer.writeStartObject()
        writer.writeEndObject()

        when:
        writer.writeName('i3')

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted() {
        given:
        writer.writeStartObject()
        writer.writeStartArray('f2')
        writer.writeEndArray()

        when:
        writer.writeEndArray()

        then:
        thrown(BsonInvalidOperationException)
    }

    def shouldThrowAnExceptionWhenWritingNullName() {
        given:
        writer.writeStartObject()

        when:
        writer.writeName(null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullValue() {
        given:
        writer.writeStartObject()
        writer.writeName('v')

        when:
        writer.writeNumber(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeString(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRaw(null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullMemberValue() {
        given:
        writer.writeStartObject()

        when:
        writer.writeNumber('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeString('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRaw('v', null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullMemberName() {
        given:
        writer.writeStartObject()

        when:
        writer.writeNumber(null, '1')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeString(null, 'str')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRaw(null, 'raw')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeBoolean(null, true)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeNull(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeStartObject(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeStartArray(null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldStopAtMaxLength() {
        given:
        def fullJsonText = '{ "n" : null }'
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().maxLength(maxLength).build())

        when:
        writer.writeStartObject()
        writer.writeNull('n')
        writer.writeEndObject()

        then:
        stringWriter.toString() == fullJsonText[0..<Math.min(maxLength, fullJsonText.length())]
        writer.getCurrentLength() == Math.min(maxLength, fullJsonText.length())
        writer.isTruncated() || fullJsonText.length() <= maxLength

        where:
        maxLength << (1..20)
    }
}
