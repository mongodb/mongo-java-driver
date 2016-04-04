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

    def 'should decode a BSON Binary with subtype of UIID_LEGACY to a UUID'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null)
        def reader = new BsonDocumentReader(parse('{array : [{ "$binary" : "D0dqZ20GeYvWzXdt0gkSlA==", "$type" : "3" }]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def iterable = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        iterable == [UUID.fromString('8b79066d-676a-470f-9412-09d26d77cdd6')]
    }

    def 'should decode a BSON Binary with subtype of UIID_STANDARD to a UUID'() {
        given:
        def codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null)
        def reader = new BsonDocumentReader(parse('{array : [{ "$binary" : "i3kGbWdqRw+UEgnSbXfN1g==", "$type" : "4" }]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def iterable = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        iterable == [UUID.fromString('8b79066d-676a-470f-9412-09d26d77cdd6')]
    }

}