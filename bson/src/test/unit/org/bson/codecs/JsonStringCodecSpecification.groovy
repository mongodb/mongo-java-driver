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
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import spock.lang.Specification
import org.bson.json.JsonString;

import static org.bson.BsonDocument.parse

class JsonStringCodecSpecification extends Specification {
    def 'should have JsonString encoding class'() {
        given:
        def codec = new JsonStringCodec()

        expect:
        codec.getEncoderClass() == JsonString
    }

    def 'should encode JsonString correctly'() {
        given:
        def codec = new JsonStringCodec()
        def writer = new BsonDocumentWriter(new BsonDocument())

        when:
        codec.encode(writer, new JsonString("{hello: {world: 1}}"), EncoderContext.builder().build())

        then:
        writer.document == parse('{hello: {world: 1}}')
    }

    def 'should decode JsonString correctly'() {
        given:
        def codec = new JsonStringCodec()
        def reader = new BsonDocumentReader(parse("{hello: {world: 1}}"))

        when:
        def jsonString = codec.decode(reader, DecoderContext.builder().build())

        then:
        jsonString.getJson() == "{\"hello\": {\"world\": 1}}"
    }

    def 'should use JsonWriterSettings'() {
        given:
        def codec = new JsonStringCodec(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build())
        def reader = new BsonDocumentReader(parse("{hello: 1}"))

        when:
        def jsonString = codec.decode(reader, DecoderContext.builder().build())

        then:
        jsonString.getJson() == "{\"hello\": {\"\$numberInt\": \"1\"}}"
    }

}
