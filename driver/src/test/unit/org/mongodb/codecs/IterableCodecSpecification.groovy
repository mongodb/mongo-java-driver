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

import org.bson.BSONReader
import org.bson.BSONWriter
import org.mongodb.Decoder
import org.mongodb.Document
import spock.lang.Specification
import spock.lang.Subject

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class IterableCodecSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    @Subject
    private final IterableCodec iterableCodec = new IterableCodec(Codecs.createDefault());

    def 'should encode list of strings'() {
        given:
        List<String> stringList = ['Uno', 'Dos', 'Tres'];

        when:
        iterableCodec.encode(bsonWriter, stringList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString('Uno');
        1 * bsonWriter.writeString('Dos');
        1 * bsonWriter.writeString('Tres');
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode list of integers'() {
        given:
        List<Integer> stringList = [1, 2, 3];

        when:
        iterableCodec.encode(bsonWriter, stringList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(1);
        1 * bsonWriter.writeInt32(2);
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeEndArray();
    }

    def 'should delegate encoding of complex types to codecs'() {
        given:
        // different setup means this should be in a different test class
        Codecs mockCodecs = Mock(Codecs);
        IterableCodec iterableCodecWithMock = new IterableCodec(mockCodecs);

        Object document = new Document('field', 'value');
        List<Object> documentList = [document];

        when:
        iterableCodecWithMock.encode(bsonWriter, documentList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * mockCodecs.encode(bsonWriter, document);
        1 * bsonWriter.writeEndArray();
    }

    def 'should decode arrays as lists of objects'() {
        given:
        Iterable expectedList = [1, 2, 3];
        BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        Iterable actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
        actualDecodedObject instanceof ArrayList;
    }

    def 'should decode array of arrays'() {
        given:
        Iterable<List<Integer>> expectedList = [[1, 2], [3]];
        BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        Iterable actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
    }

    def 'should decode array of documents'() {
        given:
        Object document = new Document('field', 'value');
        Iterable<Object> expectedList = [document];
        BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        Iterable<Object> actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
    }

    def 'should be able to decode into set'() {
        given:
        IterableCodec iterableCodecForSet = new IterableCodec(Codecs.createDefault(), new HashSetFactory(), Codecs.createDefault());

        Iterable<Integer> expectedSet = new HashSet<Integer>([1, 2, 3]);
        BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedSet);

        when:
        Iterable<Integer> actualDecodedObject = iterableCodecForSet.decode(reader);

        then:
        actualDecodedObject == expectedSet;
        actualDecodedObject instanceof Set;
    }

    def 'should be able to plug in custom decoder'() {
        given:
        int timesDecodeCalled = 0;

        BSONReader reader = prepareReaderWithObjectToBeDecoded(new HashSet<Integer>([1]));

        Decoder decoder = Mock();
        //this magic incantation is the stubbing
        decoder.decode(reader) >> { reader.readInt32(); timesDecodeCalled++; }
        IterableCodec iterableCodecForSet = new IterableCodec(Codecs.createDefault(), new HashSetFactory(), decoder);

        when:
        iterableCodecForSet.decode(reader);

        then:
        timesDecodeCalled == 1;
    }
}
