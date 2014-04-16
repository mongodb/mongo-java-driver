/*
 * Copyright (c) 2008 - 2014 10gen, Inc. <http://10gen.com>
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

package org.mongodb.codecs

import org.bson.BSONReader
import org.bson.BSONWriter
import org.bson.types.Undefined
import spock.lang.Specification
import spock.lang.Subject

class UndefinedCodecSpecification extends Specification {
    @Subject
    UndefinedCodec codec = new UndefinedCodec();

    def 'should return Undefined class'() {
        expect:
        codec.encoderClass == Undefined
    }

    def 'should decode undefined type from BSONReader'() {
        given:
        BSONReader reader = Mock()

        when:
        def result = codec.decode(reader)

        then:
        1 * reader.readUndefined()
        result != null
        result.class == Undefined
    }

    def 'should encode undefined type to BSONWriter'() {
        given:
        BSONWriter writer = Mock()

        when:
        codec.encode(writer, new Undefined())

        then:
        1 * writer.writeUndefined()
    }


}
