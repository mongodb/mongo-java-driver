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

import org.bson.BsonArray
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.jsr310.Jsr310CodecProvider
import org.bson.types.Binary
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.ParameterizedType
import java.time.Instant

import static org.bson.BsonDocument.parse
import static org.bson.UuidRepresentation.C_SHARP_LEGACY
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static org.bson.UuidRepresentation.PYTHON_LEGACY
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.UuidRepresentation.UNSPECIFIED
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class CollectionCodecSpecification extends Specification {

    static final REGISTRY = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider(),
                    new CollectionCodecProvider(), new MapCodecProvider()))

    def 'should decode to specified generic class'() {
        given:
        def doc = new BsonDocument('a', new BsonArray())

        when:
        def codec = new CollectionCodec(fromProviders([new ValueCodecProvider()]), new BsonTypeClassMap(), null, collectionType)
        def reader = new BsonDocumentReader(doc)
        reader.readStartDocument()
        reader.readName('a')
        def collection = codec.decode(reader, DecoderContext.builder().build())

        then:
        codec.getEncoderClass() == collectionType
        collection.getClass() == decodedType

        where:
        collectionType     | decodedType
        Collection         | ArrayList
        List               | ArrayList
        AbstractList       | ArrayList
        AbstractCollection | ArrayList
        ArrayList          | ArrayList
        Set                | HashSet
        AbstractSet        | HashSet
        HashSet            | HashSet
        NavigableSet       | TreeSet
        SortedSet          | TreeSet
        TreeSet            | TreeSet
    }

    def 'should encode a Collection to a BSON array'() {
        given:
        def codec = new CollectionCodec(REGISTRY, new BsonTypeClassMap(), null, Collection)
        def writer = new BsonDocumentWriter(new BsonDocument())

        when:
        writer.writeStartDocument()
        writer.writeName('array')
        codec.encode(writer, [1, 2, 3, null], EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.document == parse('{array : [1, 2, 3, null]}')
    }

    def 'should decode a BSON array to a Collection'() {
        given:
        def codec = new CollectionCodec(REGISTRY, new BsonTypeClassMap(), null, Collection)
        def reader = new BsonDocumentReader(parse('{array : [1, 2, 3, null]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def collection = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        collection == [1, 2, 3, null]
    }

    def 'should decode a BSON array of arrays to a Collection of Collection'() {
        given:
        def codec = new CollectionCodec(REGISTRY, new BsonTypeClassMap(), null, Collection)
        def reader = new BsonDocumentReader(parse('{array : [[1, 2], [3, 4, 5]]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def collection = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        collection == [[1, 2], [3, 4, 5]]
    }

    def 'should use provided transformer'() {
        given:
        def codec = new CollectionCodec(REGISTRY, new BsonTypeClassMap(), { Object from ->
            from.toString()
        }, Collection)
        def reader = new BsonDocumentReader(parse('{array : [1, 2, 3]}'))

        when:
        reader.readStartDocument()
        reader.readName('array')
        def collection = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        collection == ['1', '2', '3']
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 3 for UUID'() {
        given:
        def reader = new BsonDocumentReader(parse(document))
        def codec = new CollectionCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()), new BsonTypeClassMap(),
        null, Collection)
                .withUuidRepresentation(representation)

        when:
        reader.readStartDocument()
        reader.readName('array')
        def collection = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        value == collection

        where:
        representation | value                                                     | document
        JAVA_LEGACY    | [UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09')] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}'
        C_SHARP_LEGACY | [UUID.fromString('04030201-0605-0807-090a-0b0c0d0e0f10')] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}'
        PYTHON_LEGACY  | [UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}'
        STANDARD       | [new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}'
        UNSPECIFIED    | [new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "3" }]}'
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 4 for UUID'() {
        given:
        def reader = new BsonDocumentReader(parse(document))
        def codec = new CollectionCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()), new BsonTypeClassMap(),
        null, Collection)
                .withUuidRepresentation(representation)

        when:
        reader.readStartDocument()
        reader.readName('array')
        def collection = codec.decode(reader, DecoderContext.builder().build())
        reader.readEndDocument()

        then:
        value == collection

        where:
        representation | value                                                     | document
        STANDARD       | [UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "4" }]}'
        JAVA_LEGACY    | [UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')] | '{"array": [{ "$binary" : "CAcGBQQDAgEQDw4NDAsKCQ==", "$type" : "3" }]}'
        C_SHARP_LEGACY | [new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "4" }]}'
        PYTHON_LEGACY  | [new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "4" }]}'
        UNSPECIFIED    | [new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])] | '{"array": [{ "$binary" : "AQIDBAUGBwgJCgsMDQ4PEA==", "$type" : "4" }]}'
    }

    def 'should parameterize'() {
        given:
        def codec = new CollectionCodec(REGISTRY, new BsonTypeClassMap(), null, Collection)
        def writer = new BsonDocumentWriter(new BsonDocument())
        def reader = new BsonDocumentReader(writer.getDocument())
        def instants = [
                ['firstMap': [Instant.ofEpochMilli(1), Instant.ofEpochMilli(2)]],
                ['secondMap': [Instant.ofEpochMilli(3), Instant.ofEpochMilli(4)]]]
        when:
        codec = codec.parameterize(fromProviders(new Jsr310CodecProvider(), REGISTRY),
                Arrays.asList(((ParameterizedType) Container.getMethod('getInstants').genericReturnType).actualTypeArguments))
        writer.writeStartDocument()
        writer.writeName('instants')
        codec.encode(writer, instants, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.getDocument() == new BsonDocument()
                .append('instants', new BsonArray(
                        [
                                new BsonDocument('firstMap', new BsonArray([new BsonDateTime(1), new BsonDateTime(2)])),
                                new BsonDocument('secondMap', new BsonArray([new BsonDateTime(3), new BsonDateTime(4)]))
                        ]))

        when:
        reader.readStartDocument()
        reader.readName('instants')
        def decodedInstants = codec.decode(reader, DecoderContext.builder().build())

        then:
        decodedInstants == instants
    }

    @SuppressWarnings('unused')
    static class Container {
        private final List<Map<String, List<Instant>>> instants = []

        List<Map<String, List<Instant>>> getInstants() {
            instants
        }
    }
}
