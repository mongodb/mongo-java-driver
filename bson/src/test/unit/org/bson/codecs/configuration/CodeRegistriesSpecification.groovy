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

package org.bson.codecs.configuration

import org.bson.BsonArray
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.codecs.BsonInt32Codec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.CollectionCodecProvider
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.IntegerCodec
import org.bson.codecs.LongCodec
import org.bson.codecs.MapCodecProvider
import org.bson.codecs.UuidCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.jsr310.Jsr310CodecProvider
import org.bson.internal.ProvidersCodecRegistry
import spock.lang.Specification

import java.lang.reflect.ParameterizedType
import java.time.Instant

import static CodecRegistries.fromCodecs
import static CodecRegistries.fromProviders
import static CodecRegistries.fromRegistries
import static java.util.Arrays.asList
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.UuidRepresentation.UNSPECIFIED
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation

class CodeRegistriesSpecification extends Specification {
    def 'fromCodec should return a SingleCodecRegistry'() {
        when:
        def registry = fromCodecs(new UuidCodec(), new LongCodec())

        then:
        registry instanceof ProvidersCodecRegistry
        registry.get(UUID) instanceof UuidCodec
        registry.get(Long) instanceof LongCodec
    }

    def 'fromProvider should return ProvidersCodecRegistry'() {
        when:
        def registry = fromProviders(new BsonValueCodecProvider())

        then:
        registry instanceof ProvidersCodecRegistry
        registry.get(BsonInt32) instanceof BsonInt32Codec
    }

    def 'fromProviders should return ProvidersCodecRegistry'() {
        when:
        def providers = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider()])

        then:
        providers instanceof ProvidersCodecRegistry
        providers.get(BsonInt32) instanceof BsonInt32Codec
        providers.get(Integer) instanceof IntegerCodec
    }

    def 'fromRegistries should return ProvidersCodecRegistry'() {
        def uuidCodec = new UuidCodec()
        when:
        def registry = fromRegistries(fromCodecs(uuidCodec), fromProviders(new ValueCodecProvider()))

        then:
        registry instanceof ProvidersCodecRegistry
        registry.get(UUID).is(uuidCodec)
        registry.get(Integer) instanceof IntegerCodec
    }

    def 'withUuidRepresentation should apply uuid representation'() {
        given:
        def registry = fromProviders(new ValueCodecProvider())
        def registryWithStandard = withUuidRepresentation(registry, STANDARD)

        when:
        def uuidCodec = registry.get(UUID) as UuidCodec

        then:
        uuidCodec.getUuidRepresentation() == UNSPECIFIED

        when:
        uuidCodec = registryWithStandard.get(UUID) as UuidCodec

        then:
        uuidCodec.getUuidRepresentation() == STANDARD
    }

    def 'withUuidRepresentation should not break parameterization'() {
        given:
        def registry = fromProviders(
                new Jsr310CodecProvider(),
                new ValueCodecProvider(),
                withUuidRepresentation(fromProviders(new CollectionCodecProvider()), STANDARD),
                withUuidRepresentation(fromProviders(new MapCodecProvider()), STANDARD)
        )
        def codec = registry.get(Collection, asList(
                ((ParameterizedType) CodeRegistriesSpecification.getMethod('parameterizedTypeProvider').genericReturnType)
                        .actualTypeArguments))
        def writer = new BsonDocumentWriter(new BsonDocument())
        def reader = new BsonDocumentReader(writer.getDocument())
        def value = [
                ['firstMap': [Instant.ofEpochMilli(1), Instant.ofEpochMilli(2)]],
                ['secondMap': [Instant.ofEpochMilli(3), Instant.ofEpochMilli(4)]]]
        when:
        writer.writeStartDocument()
        writer.writeName('value')
        codec.encode(writer, value, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.getDocument() == new BsonDocument()
                .append('value', new BsonArray(
                        [
                                new BsonDocument('firstMap', new BsonArray([new BsonDateTime(1), new BsonDateTime(2)])),
                                new BsonDocument('secondMap', new BsonArray([new BsonDateTime(3), new BsonDateTime(4)]))
                        ]))

        when:
        reader.readStartDocument()
        reader.readName('value')
        def decodedValue = codec.decode(reader, DecoderContext.builder().build())

        then:
        decodedValue == value
    }

    @SuppressWarnings('unused')
    List<Map<String, List<Instant>>> parameterizedTypeProvider() {
        []
    }
}
