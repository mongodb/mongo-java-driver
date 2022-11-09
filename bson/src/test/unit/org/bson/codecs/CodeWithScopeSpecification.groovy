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

import org.bson.BsonBinaryReader
import org.bson.BsonWriter
import org.bson.Document
import org.bson.types.CodeWithScope
import spock.lang.Specification
import spock.lang.Subject

import static CodecTestUtil.prepareReaderWithObjectToBeDecoded

class CodeWithScopeSpecification extends Specification {
    private final BsonWriter bsonWriter = Mock()

    @Subject
    private final CodeWithScopeCodec codeWithScopeCodec = new CodeWithScopeCodec(new DocumentCodec())

    def 'should encode code with scope as java script followed by document of scope'() {
        given:
        String javascriptCode = '<javascript code>'
        CodeWithScope codeWithScope = new CodeWithScope(javascriptCode, new Document('the', 'scope'))

        when:
        codeWithScopeCodec.encode(bsonWriter, codeWithScope, EncoderContext.builder().build())

        then:
        1 * bsonWriter.writeJavaScriptWithScope(javascriptCode)
        then:
        1 * bsonWriter.writeStartDocument()
        then:
        1 * bsonWriter.writeName('the')
        then:
        1 * bsonWriter.writeString('scope')
        then:
        1 * bsonWriter.writeEndDocument()
    }

    def 'should decode code with scope'() {
        given:
        CodeWithScope codeWithScope = new CodeWithScope('{javascript code}', new Document('the', 'scope'))
        BsonBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope)

        when:
        CodeWithScope actualCodeWithScope = codeWithScopeCodec.decode(reader, DecoderContext.builder().build())

        then:
        actualCodeWithScope == codeWithScope
    }
}
