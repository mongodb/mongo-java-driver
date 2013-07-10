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
import org.mongodb.DBRef
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class DBRefCodecSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();
    private final Codecs codecs = Mock();

    @Subject
    private final DBRefEncoder dbRefCodec = new DBRefEncoder(codecs);

    def 'should encode db ref as string namespace and delegate encoding of id to codecs'() {
        given:
        String namespace = 'theNamespace';
        String theId = 'TheId';
        DBRef dbRef = new DBRef(theId, namespace);

        when:
        dbRefCodec.encode(bsonWriter, dbRef);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeString('$ref', namespace);
        then:
        1 * bsonWriter.writeName('$id');
        then:
        1 * codecs.encode(bsonWriter, theId);
        then:
        1 * bsonWriter.writeEndDocument();
    }

    @Ignore('decoding not implemented yet')
    def 'should decode code with scope'() {
        given:
        String namespace = 'theNamespace';
        String theId = 'TheId';
        DBRef dbRef = new DBRef(theId, namespace);
        BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(dbRef);

        DBRef actualDBRef = dbRefCodec.decode(reader);

        assertThat(actualDBRef, is(dbRef));
    }
}
