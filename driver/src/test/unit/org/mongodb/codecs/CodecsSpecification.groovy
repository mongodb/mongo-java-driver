/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class CodecsSpecification extends Specification {
    private BSONWriter bsonWriter = Mock(BSONWriter);

    private Codecs codecs = Codecs.builder().primitiveCodecs(PrimitiveCodecs.createDefault()).build();

    def shouldEncodeCodeWithScopeAsJavaScriptFollowedByDocumentOfScopeWhenPassedInAsObject() {
        setup:
        final String javascriptCode = "<javascript code>";
        final Object codeWithScope = new CodeWithScope(javascriptCode, new Document("the", "scope"));

        when:
        codecs.encode(bsonWriter, codeWithScope);

        then:
        1 * bsonWriter.writeJavaScriptWithScope(javascriptCode);
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("the");
        1 * bsonWriter.writeString("scope");
        1 * bsonWriter.writeEndDocument();
    }

    def shouldDecodeCodeWithScope() {
        setup:
        final CodeWithScope codeWithScope = new CodeWithScope("{javascript code}", new Document("the", "scope"));
        final BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope);

        when:
        final Object actualCodeWithScope = codecs.decode(reader);

        then:
        actualCodeWithScope == codeWithScope;
    }

    def shouldEncodeDbRefWhenDisguisedAsAnObject() {
        setup:
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final Object dbRef = new DBRef(theId, namespace);

        when:
        codecs.encode(bsonWriter, dbRef);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeString('$ref', namespace);
        1 * bsonWriter.writeName('$id');
        1 * bsonWriter.writeString(theId);
        1 * bsonWriter.writeEndDocument();
    }

    def shouldEncodeNull() {
        when:
        codecs.encode(bsonWriter, (Object) null);

        then:
        1 * bsonWriter.writeNull();
    }

    def shouldBeAbleToEncodeMap() {
        expect:
        codecs.canEncode(new HashMap<String, Object>()) == true;
    }

    def shouldBeAbleToEncodeArray() {
        expect:
        codecs.canEncode(["some string"] as String[]) == true;
    }

    def shouldBeAbleToEncodeList() {
        expect:
        codecs.canEncode(new ArrayList<String>()) == true;
    }

    def shouldBeAbleToEncodePrimitive() {
        expect:
        codecs.canEncode(1) == true;
    }

    def shouldBeAbleToEncodeCodeWithScope() {
        expect:
        codecs.canEncode(new CodeWithScope(null, null)) == true;
    }

    def shouldBeAbleToEncodeDBRef() {
        expect:
        codecs.canEncode(new DBRef(null, null)) == true;
    }

    def shouldBeAbleToEncodeNull() {
        expect:
        codecs.canEncode(null) == true;
    }

    def shouldBeAbleToDecodeMap() {
        expect:
        codecs.canDecode(Map.class) == true;
    }

    def shouldBeAbleToDecodeHashMap() {
        expect:
        codecs.canDecode(HashMap.class) == true;
    }

    @Ignore("not supported yet")
    def shouldBeAbleToDecodePrimitive() {
        expect:
        codecs.canDecode(int.class) == true;
    }

}
