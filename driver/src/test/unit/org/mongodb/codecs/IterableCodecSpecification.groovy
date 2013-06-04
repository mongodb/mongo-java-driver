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

import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded

class IterableCodecSpecification extends Specification {
    private BSONWriter bsonWriter = Mock(BSONWriter);

    private final IterableCodec iterableCodec = new IterableCodec(Codecs.createDefault());

    public void 'should encode list of strings'() {
        setup:
        final List<String> stringList = ["Uno", "Dos", "Tres"];

        when:
        iterableCodec.encode(bsonWriter, stringList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString("Uno");
        1 * bsonWriter.writeString("Dos");
        1 * bsonWriter.writeString("Tres");
        1 * bsonWriter.writeEndArray();
    }

    public void 'should encode list of integers'() {
        setup:
        final List<Integer> stringList = [1, 2, 3];

        when:
        iterableCodec.encode(bsonWriter, stringList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(1);
        1 * bsonWriter.writeInt32(2);
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeEndArray();
    }

    public void 'should delegate encoding of complex types to codecs'() {
        setup:
        // different setup means this should be in a different test class
        final Codecs mockCodecs = Mock(Codecs);
        final IterableCodec iterableCodecWithMock = new IterableCodec(mockCodecs);

        final Object document = new Document("field", "value");
        final List<Object> documentList = [document];

        when:
        iterableCodecWithMock.encode(bsonWriter, documentList);

        then:
        1 * bsonWriter.writeStartArray();
        1 * mockCodecs.encode(bsonWriter, document);
        1 * bsonWriter.writeEndArray();
    }

    public void 'should decode arrays as lists of objects'() {
        setup:
        final Iterable expectedList = [1, 2, 3];
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        final Iterable actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
        actualDecodedObject instanceof ArrayList;
    }

    public void 'should decode array of arrays'() {
        setup:
        final Iterable<List<Integer>> expectedList = [[1, 2], [3]];
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        final Iterable actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
    }

    public void 'should decode array of documents'() {
        setup:
        final Object document = new Document("field", "value");
        final Iterable<Object> expectedList = [document];
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        when:
        final Iterable<Object> actualDecodedObject = iterableCodec.decode(reader);

        then:
        actualDecodedObject == expectedList;
    }

    public void 'should be able to decode into set'() {
        setup:
        final IterableCodec iterableCodecForSet = new IterableCodec(Codecs.createDefault(), new HashSetFactory(), Codecs.createDefault());

        final Iterable<Integer> expectedSet = new HashSet<Integer>([1, 2, 3]);
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedSet);

        when:
        final Iterable<Integer> actualDecodedObject = iterableCodecForSet.decode(reader);

        then:
        actualDecodedObject == expectedSet;
        actualDecodedObject instanceof Set;
    }

    public void 'should be able to plug in custom decoder'() {
        setup:
        final StubDecoder decoder = new StubDecoder();
        final IterableCodec iterableCodecForSet = new IterableCodec(Codecs.createDefault(), new HashSetFactory(), decoder);

        final Iterable<Integer> expectedSet = new HashSet<Integer>([1]);
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedSet);

        when:
        iterableCodecForSet.decode(reader);

        then:
        decoder.timesCalled == 1;
    }

    private static final class StubDecoder implements Decoder<Object> {
        private int timesCalled;

        @Override
        public Object decode(final BSONReader reader) {
            timesCalled++;
            return reader.readInt32();
        }
    }
}
