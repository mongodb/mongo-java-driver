/*
 * Copyright 2016-2017 MongoDB, Inc.
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

import org.bson.BsonBinary
import org.bson.BsonDbPointer
import org.bson.BsonDocumentReader
import org.bson.BsonRegularExpression
import org.bson.BsonTimestamp
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import spock.lang.Specification

import static org.bson.BsonHelper.documentWithValuesOfEveryType

class JsonWriterSpecification extends Specification {

    def stringWriter = new StringWriter();
    def writer = new JsonWriter(stringWriter)
    def jsonWithValuesOfEveryType = documentWithValuesOfEveryType().toJson(JsonWriterSettings.builder().build())

    def 'should pipe all types'() {
        given:
        def reader = new BsonDocumentReader(documentWithValuesOfEveryType())

        when:
        writer.pipe(reader)

        then:
        stringWriter.toString() == documentWithValuesOfEveryType().toJson()
    }

    def 'should pipe all types with capped length'() {
        given:
        def reader = new BsonDocumentReader(documentWithValuesOfEveryType())
        def writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().maxLength(maxLength).build())

        when:
        writer.pipe(reader)

        then:
        stringWriter.toString() == jsonWithValuesOfEveryType[0..Math.min(maxLength, jsonWithValuesOfEveryType.length()) - 1]

        where:
        maxLength << (0..1000)
    }

    def shouldThrowAnExceptionWhenWritingNullName() {
        given:
        writer.writeStartDocument()

        when:
        writer.writeName(null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullValue() {
        given:
        writer.writeStartDocument()
        writer.writeName('v')

        when:
        writer.writeString(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeBinaryData(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDBPointer(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDecimal128(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScript(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScriptWithScope(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeObjectId(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRegularExpression(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeSymbol(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeTimestamp(null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullMemberValue() {
        given:
        writer.writeStartDocument()

        when:
        writer.writeString('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeBinaryData('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDBPointer('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDecimal128('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScript('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScriptWithScope('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeObjectId('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRegularExpression('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeSymbol('v', null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeTimestamp('v', null)

        then:
        thrown(IllegalArgumentException)
    }

    def shouldThrowAnExceptionWhenWritingNullMemberName() {
        given:
        writer.writeStartDocument()

        when:
        writer.writeStartDocument(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeStartArray(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeString(null, 's')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeBinaryData(null, new BsonBinary(new byte[1]))

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDBPointer(null, new BsonDbPointer('a.b', new ObjectId()))

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDecimal128(null, Decimal128.NaN)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScript(null, 'function() {}')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeJavaScriptWithScope(null, 'function() {}')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeObjectId(null, new ObjectId())

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeRegularExpression(null, new BsonRegularExpression('.*'))

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeSymbol(null, 's')

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeTimestamp(null, new BsonTimestamp(42, 1))

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeNull(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeBoolean(null, true)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeInt32(null, 1)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeInt64(null, 1L)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDouble(null, 2)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeDateTime(null, 100)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeMaxKey(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeMinKey(null)

        then:
        thrown(IllegalArgumentException)

        when:
        writer.writeUndefined(null)

        then:
        thrown(IllegalArgumentException)
    }
}