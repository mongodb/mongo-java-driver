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

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class DBRefCodecSpecification extends Specification {
    private BSONWriter bsonWriter = Mock(BSONWriter);
    private Codecs codecs = Mock(Codecs);

    private DBRefCodec dbRefCodec = new DBRefCodec(codecs);

    public void 'should encode db ref as string namespace and delegate encoding of id to codecs'() {
        setup:
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final DBRef dbRef = new DBRef(theId, namespace);

        when:
        dbRefCodec.encode(bsonWriter, dbRef);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeString('$ref', namespace);
        1 * bsonWriter.writeName('$id');
        1 * codecs.encode(bsonWriter, theId);
        1 * bsonWriter.writeEndDocument();
    }

    @Ignore("decoding not implemented yet")
    public void 'should decode code with scope'() {
        setup:
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final DBRef dbRef = new DBRef(theId, namespace);
        final BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(dbRef);

//        final DBRef actualDBRef = dbRefCodec.decode(reader);

//        assertThat(actualDBRef, is(dbRef));
    }
}
