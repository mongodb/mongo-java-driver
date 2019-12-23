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

package org.bson

import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.json.JsonMode
import org.bson.json.JsonReader
import org.bson.json.JsonWriter
import org.bson.json.JsonWriterSettings
import spock.lang.Specification

@SuppressWarnings('UnnecessaryObjectReferences')
class LimitedLookaheadMarkSpecification extends Specification {


    def 'Lookahead should work at various states with Mark'(BsonWriter writer, boolean useAlternateReader) {
        given:
        writer.with {
            writeStartDocument()
            writeInt64('int64', 52L)
            writeStartArray('array')
            writeInt32(1)
            writeInt64(2L)
            writeStartArray()
            writeInt32(3)
            writeInt32(4)
            writeEndArray()
            writeStartDocument()
            writeInt32('a', 5)
            writeEndDocument()
            writeNull()
            writeEndArray()
            writeStartDocument('document')
            writeInt32('a', 6)
            writeEndDocument()
            writeEndDocument()
        }


        when:
        BsonReader reader
        BsonReaderMark mark
        if (writer instanceof BsonDocumentWriter) {
            reader = new BsonDocumentReader(writer.document)
        } else if (writer instanceof BsonBinaryWriter) {
            BasicOutputBuffer buffer = (BasicOutputBuffer) writer.getBsonOutput()
            reader = new BsonBinaryReader(new ByteBufferBsonInput(buffer.getByteBuffers().get(0)))
        } else if (writer instanceof JsonWriter) {
            if (useAlternateReader) {
                reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(writer.writer.toString().getBytes())))
            } else {
                reader = new JsonReader(writer.writer.toString())
            }
        }

        reader.readStartDocument()
        // mark beginning of document * 1
        mark = reader.getMark()

        then:
        reader.readName() == 'int64'
        reader.readInt64() == 52L
        reader.readStartArray()

        when:
        // reset to beginning of document * 2
        mark.reset()
        // mark beginning of document * 2
        mark = reader.getMark()

        then:
        reader.readName() == 'int64'
        reader.readInt64() == 52L

        when:
        // make sure it's possible to reset to a mark after getting a new mark
        reader.getMark()
        // reset to beginning of document * 3
        mark.reset()
        // mark beginning of document * 3
        mark = reader.getMark()

        then:
        reader.readName() == 'int64'
        reader.readInt64() == 52L
        reader.readName() == 'array'
        reader.readStartArray()
        reader.readInt32() == 1
        reader.readInt64() == 2
        reader.readStartArray()
        reader.readInt32() == 3
        reader.readInt32() == 4
        reader.readEndArray()
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 5
        reader.readEndDocument()
        reader.readNull()
        reader.readEndArray()
        reader.readName() == 'document'
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 6
        reader.readEndDocument()
        reader.readEndDocument()

        when:
        // read entire document, reset to beginning
        mark.reset()

        then:
        reader.readName() == 'int64'
        reader.readInt64() == 52L
        reader.readName() == 'array'

        when:
        // mark in outer-document * 1
        mark = reader.getMark()

        then:
        reader.readStartArray()
        reader.readInt32() == 1
        reader.readInt64() == 2
        reader.readStartArray()

        when:
        // reset in sub-document * 1
        mark.reset()
        // mark in outer-document * 2
        mark = reader.getMark()

        then:
        reader.readStartArray()
        reader.readInt32() == 1
        reader.readInt64() == 2
        reader.readStartArray()
        reader.readInt32() == 3

        when:
        // reset in sub-document * 2
        mark.reset()

        then:
        reader.readStartArray()
        reader.readInt32() == 1
        reader.readInt64() == 2
        reader.readStartArray()
        reader.readInt32() == 3
        reader.readInt32() == 4

        when:
        // mark in sub-document * 1
        mark = reader.getMark()

        then:
        reader.readEndArray()
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 5
        reader.readEndDocument()
        reader.readNull()
        reader.readEndArray()

        when:
        // reset in outer-document * 1
        mark.reset()
        // mark in sub-document * 2
        mark = reader.getMark()

        then:
        reader.readEndArray()
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 5
        reader.readEndDocument()
        reader.readNull()
        reader.readEndArray()

        when:
        // reset in out-document * 2
        mark.reset()

        then:
        reader.readEndArray()
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 5
        reader.readEndDocument()
        reader.readNull()
        reader.readEndArray()
        reader.readName() == 'document'
        reader.readStartDocument()
        reader.readName() == 'a'
        reader.readInt32() == 6
        reader.readEndDocument()
        reader.readEndDocument()

        where:
        writer | useAlternateReader
        new BsonDocumentWriter(new BsonDocument()) | false
        new BsonBinaryWriter(new BasicOutputBuffer()) | false
        new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()) | false
        new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()) | true
    }

    def 'should peek binary subtype and size'(BsonWriter writer) {
        given:
        writer.with {
            writeStartDocument()
            writeBinaryData('binary', new BsonBinary(BsonBinarySubType.UUID_LEGACY, new byte[16]))
            writeInt64('int64', 52L)
            writeEndDocument()
        }

        when:
        BsonReader reader
        if (writer instanceof BsonDocumentWriter) {
            reader = new BsonDocumentReader(writer.document)
        } else if (writer instanceof BsonBinaryWriter) {
            BasicOutputBuffer buffer = (BasicOutputBuffer) writer.getBsonOutput()
            reader = new BsonBinaryReader(new ByteBufferBsonInput(buffer.getByteBuffers().get(0)))
        } else if (writer instanceof JsonWriter) {
            reader = new JsonReader(writer.writer.toString())
        }

        reader.readStartDocument()
        reader.readName()
        def subType = reader.peekBinarySubType()
        def size = reader.peekBinarySize()
        def binary = reader.readBinaryData()
        def longValue = reader.readInt64('int64')
        reader.readEndDocument()

        then:
        subType == BsonBinarySubType.UUID_LEGACY.value
        size == 16
        binary == new BsonBinary(BsonBinarySubType.UUID_LEGACY, new byte[16])
        longValue == 52L

        where:
        writer << [
                new BsonDocumentWriter(new BsonDocument()),
                new BsonBinaryWriter(new BasicOutputBuffer()),
                new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build())
        ]
    }
}
