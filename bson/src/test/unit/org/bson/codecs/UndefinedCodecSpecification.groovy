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

import org.bson.BsonReader
import org.bson.BsonUndefined
import org.bson.BsonWriter
import spock.lang.Specification
import spock.lang.Subject

class UndefinedCodecSpecification extends Specification {
    @Subject
    BsonUndefinedCodec codec = new BsonUndefinedCodec()

    def 'should return Undefined class'() {
        expect:
        codec.encoderClass == BsonUndefined
    }

    def 'should decode undefined type from BsonReader'() {
        given:
        BsonReader reader = Mock()

        when:
        def result = codec.decode(reader, DecoderContext.builder().build())

        then:
        1 * reader.readUndefined()
        result != null
        result.class == BsonUndefined
    }

    def 'should encode undefined type to BsonWriter'() {
        given:
        BsonWriter writer = Mock()

        when:
        codec.encode(writer, new BsonUndefined(), EncoderContext.builder().build())

        then:
        1 * writer.writeUndefined()
    }


}
