/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs


import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class BsonCodecSpecification extends Specification {

    def provider = new BsonCodecProvider()
    def registry = fromProviders(provider)

    def 'should encode Bson'() {
        given:
        def codec = new BsonCodec()
        def customBson = new CustomBson()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('customBson')
        codec.encode(writer, customBson, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        BsonDocument.parse('{a: 1, b:2}') == writer.getDocument().get('customBson')
    }

    def 'should throw CodecConfiguration exception if cannot encode Bson'() {
        given:
        def codec = new BsonCodec()
        def customBson = new ExceptionRaisingBson()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('customBson')
        codec.encode(writer, customBson, EncoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)
    }

    def 'should throw UnsupportedOperation exception if decode is called'() {
        when:
        new BsonCodec().decode(Stub(BsonReader), DecoderContext.builder().build())

        then:
        thrown(UnsupportedOperationException)
    }

    class CustomBson implements Bson {
        @Override
        <TDocument> BsonDocument toBsonDocument(final Class<TDocument> clazz, final CodecRegistry codecRegistry) {
            BsonDocument.parse('{a: 1, b: 2}')
        }
    }

    class ExceptionRaisingBson implements Bson {
        @Override
        <TDocument> BsonDocument toBsonDocument(final Class<TDocument> clazz, final CodecRegistry codecRegistry) {
           throw new Exception('Cannot encode')
        }
    }
}
