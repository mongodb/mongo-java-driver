/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BSONBinaryReader
import org.bson.BSONWriter
import org.bson.types.CodeWithScope
import org.mongodb.DBRef
import org.mongodb.Document
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class CodecsSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    @Subject
    private final Codecs codecs = Codecs.builder().primitiveCodecs(PrimitiveCodecs.createDefault()).build();

    def 'should encode code with scope as java script followed by document of scope when passed in as object'() {
        given:
        String javascriptCode = '<javascript code>';
        Object codeWithScope = new CodeWithScope(javascriptCode, new Document('the', 'scope'));

        when:
        codecs.encode(bsonWriter, codeWithScope);

        then:
        1 * bsonWriter.writeJavaScriptWithScope(javascriptCode);
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('the');
        then:
        1 * bsonWriter.writeString('scope');
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should decode code with scope'() {
        given:
        CodeWithScope codeWithScope = new CodeWithScope('{javascript code}', new Document('the', 'scope'));
        BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope);

        when:
        Object actualCodeWithScope = codecs.decode(reader);

        then:
        actualCodeWithScope == codeWithScope;
    }

    def 'should encode db ref when disguised as an object'() {
        given:
        String namespace = 'theNamespace';
        String theId = 'TheId';
        Object dbRef = new DBRef(theId, namespace);

        when:
        codecs.encode(bsonWriter, dbRef);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeString('$ref', namespace);
        then:
        1 * bsonWriter.writeName('$id');
        then:
        1 * bsonWriter.writeString(theId);
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should encode null'() {
        when:
        codecs.encode(bsonWriter, (Object) null);

        then:
        1 * bsonWriter.writeNull();
    }

    def 'should be able to encode map'() {
        expect:
        codecs.canEncode(['myObj': new Object()]) == true;
    }

    def 'should be able to encode array'() {
        expect:
        codecs.canEncode(['some string'] as String[]) == true;
    }

    def 'should be able to encode list'() {
        expect:
        codecs.canEncode(['List', 'of', 'Strings'])
    }

    def 'should be able to encode primitive'() {
        expect:
        codecs.canEncode(1)
    }

    def 'should be able to encode code with scope'() {
        expect:
        codecs.canEncode(new CodeWithScope(null, null))
    }

    def 'should be able to encode d b ref'() {
        expect:
        codecs.canEncode(new DBRef(null, null))
    }

    def 'should be able to encode null'() {
        expect:
        codecs.canEncode(null)
    }

    def 'should be able to decode map'() {
        expect:
        codecs.canDecode(Map)
    }

    def 'should be able to decode hash map'() {
        expect:
        codecs.canDecode(HashMap)
    }

    @Ignore('not supported yet')
    def 'should be able to decode primitive'() {
        expect:
        codecs.canDecode(int)
    }

}
