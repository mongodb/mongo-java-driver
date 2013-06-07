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
import org.mongodb.Document
import spock.lang.Specification

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class CodeWithScopeSpecification extends Specification {
    private BSONWriter bsonWriter = Mock(BSONWriter);

    private CodeWithScopeCodec codeWithScopeCodec = new CodeWithScopeCodec(Codecs.createDefault());

    public void 'should encode code with scope as java script followed by document of scope'() {
        setup:
        final String javascriptCode = "<javascript code>";
        final CodeWithScope codeWithScope = new CodeWithScope(javascriptCode, new Document("the", "scope"));

        when:
        codeWithScopeCodec.encode(bsonWriter, codeWithScope);

        then:
        1 * bsonWriter.writeJavaScriptWithScope(javascriptCode);
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("the");
        1 * bsonWriter.writeString("scope");
        1 * bsonWriter.writeEndDocument();
    }

    public void 'should decode code with scope'() {
        setup:
        final CodeWithScope codeWithScope = new CodeWithScope("{javascript code}", new Document("the", "scope"));
        final BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope);

        when:
        final CodeWithScope actualCodeWithScope = codeWithScopeCodec.decode(reader);

        then:
        actualCodeWithScope == codeWithScope;
    }
}
