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

package com.mongodb.codecs

import org.bson.BsonBinaryReader
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.BsonElement
import org.bson.ByteBufNIO
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.io.BasicInputBuffer
import org.mongodb.BSONDocumentBuffer
import org.mongodb.SimpleBufferProvider
import spock.lang.Specification

import java.nio.ByteBuffer

class BsonDocumentBufferCodecSpecification extends Specification {

    def codec = new BSONDocumentBufferCodec(new SimpleBufferProvider())
    def document = new BsonDocument([new BsonElement('b1', BsonBoolean.TRUE), new BsonElement('b2', BsonBoolean.FALSE)])
    def documentBytes = [15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0] as byte[];

    def 'should get encoder class'() {
        expect:
        codec.encoderClass == BSONDocumentBuffer
    }

    def 'should encode'() {
        given:
        def document = new BsonDocument()
        def writer = new BsonDocumentWriter(document)

        when:
        codec.encode(writer, new BSONDocumentBuffer(documentBytes), EncoderContext.builder().build())

        then:
        document == new BsonDocument([new BsonElement('b1', BsonBoolean.TRUE), new BsonElement('b2', BsonBoolean.FALSE)])
    }

    def 'should decode'() {
        given:
        def reader = new BsonBinaryReader(new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(documentBytes))), false)

        when:
        BSONDocumentBuffer buffer = codec.decode(reader, DecoderContext.builder().build())

        then:
        buffer.byteBuffer.array() == documentBytes
    }
}