/*
 * Copyright 2016 MongoDB, Inc.
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

package org.bson.codecs

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.types.Binary
import spock.lang.Specification

import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class IterableCodecSpecification extends Specification {

    static final REGISTRY = fromProviders(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider(),
            new IterableCodecProvider())

    def 'should have Iterable encoding class'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap())

        expect:
        codec.getEncoderClass() == Iterable
    }

    def 'should encode an Iterable to a BSON array'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap())
        def writer = new BsonDocumentWriter(new BsonDocument())

        when:
        writer.writeStartDocument()
        writer.writeName('array')
        codec.encode(writer, [1, 2, 3, null], EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.document == parse('{array : [1, 2, 3, null]}')
    }

    def 'should decode a BSON array to an Iterable'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap())
        def reader = new BsonDocumentReader(parse('{array : [1, 2, 3, null]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def iterable = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        iterable == [1, 2, 3, null]
    }

    def 'should use provided transformer'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), { Object from ->
            from.toString()
        })
        def reader = new BsonDocumentReader(parse('{array : [1, 2, 3]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def iterable = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        iterable == ['1', '2', '3']
    }

    def 'should decode binary subtypes for UUID'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null)
        def reader = new BsonDocumentReader(parse(document))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def iterable = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        iterable == value

        where:
        document                                                                 | value
        '{"array": [{ "$binary" : "c3QL", "$type" : "3" }]}'                     | [new Binary((byte) 0x03, (byte[]) [115, 116, 11])]
        '{"array": [{ "$binary" : "c3QL", "$type" : "4" }]}'                     | [new Binary((byte) 0x04, (byte[]) [115, 116, 11])]
        '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}' | [UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09')]
        '{"array": [{ "$binary" : "CAcGBQQDAgEQDw4NDAsKCQ==", "$type" : "3" }]}' | [UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')]
    }

}
