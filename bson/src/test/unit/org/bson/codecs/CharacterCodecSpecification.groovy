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

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInvalidOperationException
import org.bson.BsonString
import spock.lang.Specification

class CharacterCodecSpecification extends Specification {
    private final CharacterCodec codec = new CharacterCodec()

    def 'should get encoder class'() {
        expect:
        codec.encoderClass == Character
    }
    def 'when encoding a character, should throw if it is null'() {
        given:
        def writer = new BsonDocumentWriter(new BsonDocument())

        when:
        codec.encode(writer, null, EncoderContext.builder().build())

        then:
        thrown(IllegalArgumentException)
    }

    def 'should encode a character'() {
        given:
        def writer = new BsonDocumentWriter(new BsonDocument())

        when:
        writer.writeStartDocument()
        writer.writeName('str')
        codec.encode(writer, 'c' as char, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.document == new BsonDocument('str', new BsonString('c'))
    }

    def 'should decode a character'() {
        given:
        def reader = new BsonDocumentReader(new BsonDocument('str', new BsonString('c')))

        when:
        reader.readStartDocument()
        reader.readName()
        def character = codec.decode(reader, DecoderContext.builder().build())

        then:
        character == 'c' as char
    }

    def 'when decoding a string whose length is not 1, should throw a BsonInvalidOperationException'() {
        given:
        def reader = new BsonDocumentReader(new BsonDocument('str', new BsonString('cc')))

        when:
        reader.readStartDocument()
        reader.readName()
        codec.decode(reader, DecoderContext.builder().build())

        then:
        thrown(BsonInvalidOperationException)
    }
}
