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

public class ArrayCodecWithObjectArrayTest {

    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    //Mocks
    private BSONWriter bsonWriter;
    private Codecs codecs;

    //Object under test
    private ArrayCodec arrayCodec;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        codecs = context.mock(Codecs.class);
        arrayCodec = new ArrayCodec(codecs);
    }

    @Test
    public void shouldWriteStartAndEndForArrayOfObjectsAndDelegateEncodingOfObject() {
        final Object object1 = new Object();
        final Object object2 = new Object();
        final Object[] arrayOfObjects = {object1, object2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(codecs).encode(bsonWriter, object1);
            oneOf(codecs).encode(bsonWriter, object2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfObjects);
    }

    @Test
    public void shouldWriteStartAndEndForArrayOfObjectsAndDelegateEncodingOfObjectWhenArrayDisguisedAsObject() {
        final Object object1 = new Object();
        final Object object2 = new Object();
        final Object arrayOfObjects = new Object[]{object1, object2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(codecs).encode(bsonWriter, object1);
            oneOf(codecs).encode(bsonWriter, object2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfObjects);
    }
}
