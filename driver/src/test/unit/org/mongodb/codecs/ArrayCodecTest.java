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
import org.bson.types.Binary;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class ArrayCodecTest {
    //CHECKSTYLE:OFF
    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    //Mocks
    private BSONWriter bsonWriter;

    private ArrayCodec arrayCodec;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        context.setThreadingPolicy(new Synchroniser());
        bsonWriter = context.mock(BSONWriter.class);
        arrayCodec = new ArrayCodec(null);
    }

    @Test
    public void shouldEncodeArrayOfInts() {
        final int[] arrayOfInts = {1, 2, 3};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt32(1);
            oneOf(bsonWriter).writeInt32(2);
            oneOf(bsonWriter).writeInt32(3);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfInts);
    }

    @Test
    public void shouldEncodeArrayOfIntsWhenItIsDisguisedAsAnObject() {
        final Object arrayOfInts = new int[]{1, 2, 3};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt32(1);
            oneOf(bsonWriter).writeInt32(2);
            oneOf(bsonWriter).writeInt32(3);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfInts);
    }

    @Test
    public void shouldEncodeArrayOfLongs() {
        final long[] array = {1, 2, 3};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt64(1);
            oneOf(bsonWriter).writeInt64(2);
            oneOf(bsonWriter).writeInt64(3);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfLongsWhenItIsDisguisedAsAnObject() {
        final Object array = new long[]{1, 2, 3};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt64(1);
            oneOf(bsonWriter).writeInt64(2);
            oneOf(bsonWriter).writeInt64(3);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfBoolean() {
        final boolean[] array = {true, false};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeBoolean(true);
            oneOf(bsonWriter).writeBoolean(false);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfBooleanWhenItIsDisguisedAsAnObject() {
        final Object array = new boolean[]{true, false};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeBoolean(true);
            oneOf(bsonWriter).writeBoolean(false);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfByte() {
        final byte[] array = {1, 2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeBinaryData(new Binary(array));
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfByteWhenItIsDisguisedAsAnObject() {
        final byte[] byteArray = {1, 2};
        @SuppressWarnings("UnnecessaryLocalVariable")
        final Object array = byteArray;
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeBinaryData(new Binary(byteArray));
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    @Ignore("char is not supported as a primitive type")
    public void shouldEncodeArrayOfChar() {
        //TODO: should this be supported?
        //CHECKSTYLE:OFF
        final char[] array = {'a', 'c'};
        //CHECKSTYLE:ON
    }

    @Test
    public void shouldEncodeArrayOfDouble() {
        final double[] array = {1.1, 2.2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeDouble(1.1);
            oneOf(bsonWriter).writeDouble(2.2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfDoubleWhenItIsDisguisedAsAnObject() {
        final Object array = new double[]{1.1, 2.2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeDouble(1.1);
            oneOf(bsonWriter).writeDouble(2.2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfFloat() {
        final float[] array = {1.4F, 2.6F};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeDouble(1.4F);
            oneOf(bsonWriter).writeDouble(2.6F);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfFloatWhenItIsDisguisedAsAnObject() {
        final Object array = new float[]{1.4F, 2.6F};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeDouble(1.4F);
            oneOf(bsonWriter).writeDouble(2.6F);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfShort() {
        final short[] array = {3, 4};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt32(3);
            oneOf(bsonWriter).writeInt32(4);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfShortWhenItIsDisguisedAsAnObject() {
        final Object array = new short[]{3, 4};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeInt32(3);
            oneOf(bsonWriter).writeInt32(4);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, array);
    }

    @Test
    public void shouldEncodeArrayOfStrings() {
        final String[] arrayOfStrings = {"1", "2", "3"};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeString("1");
            oneOf(bsonWriter).writeString("2");
            oneOf(bsonWriter).writeString("3");
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfStrings);
    }
    
    @Test
    public void shouldEncodeArrayOfStringsWhenItIsDisguisedAsAnObject() {
        final Object arrayOfStrings = new String[]{"1", "2", "3"};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeString("1");
            oneOf(bsonWriter).writeString("2");
            oneOf(bsonWriter).writeString("3");
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodec.encode(bsonWriter, arrayOfStrings);
    }

    @Test
    public void shouldWriteStartAndEndForArrayOfObjectsAndDelegateEncodingOfObject() {
        final Codecs codecToEncodeObjects = context.mock(Codecs.class);
        final ArrayCodec arrayCodecWithMock = new ArrayCodec(codecToEncodeObjects);
        final Object object1 = new Object();
        final Object object2 = new Object();
        final Object[] arrayOfObjects = {object1, object2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(codecToEncodeObjects).encode(bsonWriter, object1);
            oneOf(codecToEncodeObjects).encode(bsonWriter, object2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodecWithMock.encode(bsonWriter, arrayOfObjects);
    }

    @Test
    public void shouldWriteStartAndEndForArrayOfObjectsAndDelegateEncodingOfObjectWhenArrayDisguisedAsObject() {
        final Codecs codecToEncodeObjects = context.mock(Codecs.class);
        final ArrayCodec arrayCodecWithMock = new ArrayCodec(codecToEncodeObjects);
        final Object object1 = new Object();
        final Object object2 = new Object();
        final Object arrayOfObjects = new Object[]{object1, object2};
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartArray();
            oneOf(codecToEncodeObjects).encode(bsonWriter, object1);
            oneOf(codecToEncodeObjects).encode(bsonWriter, object2);
            oneOf(bsonWriter).writeEndArray();
        }});

        arrayCodecWithMock.encode(bsonWriter, arrayOfObjects);
    }
}
