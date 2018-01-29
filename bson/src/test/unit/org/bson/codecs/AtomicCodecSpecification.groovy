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

package org.bson.codecs

import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonInt64
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class AtomicCodecSpecification extends Specification {
    def 'should encode and decode atomic boolean'() {
        given:
        def codec = new AtomicBooleanCodec()
        def atomicBoolean = new AtomicBoolean(true)
        def document = new BsonDocument()

        when:
        def writer = new BsonDocumentWriter(document)
        writer.writeStartDocument()
        writer.writeName('b')
        codec.encode(writer, atomicBoolean, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        document == new BsonDocument('b', BsonBoolean.TRUE)

        when:
        def reader = new BsonDocumentReader(document)
        reader.readStartDocument()
        reader.readName('b')
        def value = codec.decode(reader, DecoderContext.builder().build())

        then:
        value.get() == atomicBoolean.get()
    }

    def 'should encode and decode atomic integer'() {
        given:
        def codec = new AtomicIntegerCodec()
        def atomicInteger = new AtomicInteger(1)
        def document = new BsonDocument()

        when:
        def writer = new BsonDocumentWriter(document)
        writer.writeStartDocument()
        writer.writeName('i')
        codec.encode(writer, atomicInteger, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        document == new BsonDocument('i', new BsonInt32(1))

        when:
        def reader = new BsonDocumentReader(document)
        reader.readStartDocument()
        reader.readName('i')
        def value = codec.decode(reader, DecoderContext.builder().build())

        then:
        value.get() == atomicInteger.get()
    }

    def 'should encode and decode atomic long'() {
        given:
        def codec = new AtomicLongCodec()
        def atomicLong = new AtomicLong(1L)
        def document = new BsonDocument()

        when:
        def writer = new BsonDocumentWriter(document)
        writer.writeStartDocument()
        writer.writeName('l')
        codec.encode(writer, atomicLong, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        document == new BsonDocument('l', new BsonInt64(1L))

        when:
        def reader = new BsonDocumentReader(document)
        reader.readStartDocument()
        reader.readName('l')
        def value = codec.decode(reader, DecoderContext.builder().build())

        then:
        value.get() == atomicLong.get()
    }

}
