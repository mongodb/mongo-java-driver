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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.Decoder;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;

public class IterableCodecTest {

    //CHECKSTYLE:OFF
    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private final Codecs codecs = Codecs.createDefault();
    private final IterableCodec iterableCodec = new IterableCodec(codecs);

    private BSONWriter bsonWriter;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        context.setThreadingPolicy(new Synchroniser());
        bsonWriter = context.mock(BSONWriter.class);
    }

    @Test
    public void shouldEncodeListOfStrings() {
        final List<String> stringList = asList("Uno", "Dos", "Tres");

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeString("Uno");
            oneOf(bsonWriter).writeString("Dos");
            oneOf(bsonWriter).writeString("Tres");
            oneOf(bsonWriter).writeEndArray();
        }});

        iterableCodec.encode(bsonWriter, stringList);
    }

    @Test
    public void shouldEncodeListOfIntegers() {
        final List<Integer> stringList = asList(1, 2, 3);

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt32(1);
            oneOf(bsonWriter).writeInt32(2);
            oneOf(bsonWriter).writeInt32(3);
            oneOf(bsonWriter).writeEndArray();
        }});

        iterableCodec.encode(bsonWriter, stringList);
    }

    @Test
    public void shouldDelegateEncodingOfComplexTypesToCodecs() {
        // different setup means this should be in a different test class
        final Codecs mockCodecs = context.mock(Codecs.class);
        final IterableCodec iterableCodecWithMock = new IterableCodec(mockCodecs);

        final Object document = new Document("field", "value");
        final List<Object> stringList = asList(document);

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(mockCodecs).encode(bsonWriter, document);
            oneOf(bsonWriter).writeEndArray();
        }});

        iterableCodecWithMock.encode(bsonWriter, stringList);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void shouldDecodeArraysAsListsOfObjects() {
        final Iterable expectedList = asList(1, 2, 3);
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        final Iterable actualDecodedObject = iterableCodec.decode(reader);

        assertThat(actualDecodedObject, is(expectedList));
        assertThat(actualDecodedObject, is(instanceOf(ArrayList.class)));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"}) // don't think there's any way around the varargs warning in anything below Java 8
    public void shouldDecodeArrayOfArrays() {
        final Iterable<List<Integer>> expectedList = asList(asList(1, 2), asList(3));
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        final Iterable actualDecodedObject = iterableCodec.decode(reader);

        assertThat((List<List<Integer>>) actualDecodedObject, is(expectedList));
    }

    @Test
    public void shouldDecodeArrayOfDocuments() {
        final Object document = new Document("field", "value");
        final Iterable<Object> expectedList = asList(document);
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedList);

        final Iterable<Object> actualDecodedObject = iterableCodec.decode(reader);

        assertThat(actualDecodedObject, is(expectedList));
    }

    @Test
    public void shouldBeAbleToDecodeIntoSet() {
        final IterableCodec iterableCodecForSet = new IterableCodec(codecs, new HashSetFactory(), codecs);

        final Iterable<Integer> expectedSet = new HashSet<Integer>(asList(1, 2, 3));
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedSet);

        final Iterable<Integer> actualDecodedObject = iterableCodecForSet.decode(reader);

        assertThat(actualDecodedObject, is(expectedSet));
        assertThat(actualDecodedObject, is(instanceOf(Set.class)));
    }

    @Test
    public void shouldBeAbleToPlugInCustomDecoder() {
        final StubDecoder decoder = new StubDecoder();
        final IterableCodec iterableCodecForSet = new IterableCodec(codecs, new HashSetFactory(), decoder);

        final Iterable<Integer> expectedSet = new HashSet<Integer>(asList(1));
        final BSONReader reader = prepareReaderWithObjectToBeDecoded(expectedSet);

        iterableCodecForSet.decode(reader);

        assertThat(decoder.timesCalled, is(1));
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
