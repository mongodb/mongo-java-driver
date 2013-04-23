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

import org.bson.BSONWriter;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.Document;

import java.util.List;

import static java.util.Arrays.asList;

public class IterableCodecTest {

    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private final Codecs codecs = Codecs.createDefault();
    private final IterableCodec iterableCodec = new IterableCodec(codecs);

    private BSONWriter bsonWriter;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
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

}
