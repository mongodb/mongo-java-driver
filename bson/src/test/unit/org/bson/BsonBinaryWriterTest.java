/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BsonBinaryWriterTest {

    private BsonBinaryWriter writer;
    private BasicOutputBuffer buffer;

    @Before
    public void setup() {
        buffer = new BasicOutputBuffer();
        writer = new BsonBinaryWriter(new BsonWriterSettings(100), new BsonBinaryWriterSettings(1024), buffer);
    }

    @After
    public void tearDown() {
        writer.close();
    }

    @Test
    public void shouldThrowWhenMaxDocumentSizeIsExceeded() {
        try {
            writer.writeStartDocument();
            writer.writeBinaryData("b", new BsonBinary(new byte[1024]));
            writer.writeEndDocument();
            fail();
        } catch (BsonSerializationException e) {
            assertEquals("Document size of 1037 is larger than maximum of 1024.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIfAPushedMaxDocumentSizeIsExceeded() {
        try {
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(10);
            writer.writeStartDocument("doc");
            writer.writeString("s", "123456789");
            writer.writeEndDocument();
        } catch (BsonSerializationException e) {
            assertEquals("Document size of 22 is larger than maximum of 10.", e.getMessage());
        }
    }

    @Test
    public void shouldNotThrowIfAPoppedMaxDocumentSizeIsExceeded() {
        writer.writeStartDocument();
        writer.pushMaxDocumentSize(10);
        writer.writeStartDocument("doc");
        writer.writeEndDocument();
        writer.popMaxDocumentSize();
        writer.writeBinaryData("bin", new BsonBinary(new byte[256]));
        writer.writeEndDocument();
    }

    @Test
    public void testWriteAndReadBoolean() {
        writer.writeStartDocument();
        writer.writeBoolean("b1", true);
        writer.writeBoolean("b2", false);
        writer.writeEndDocument();

        byte[] expectedValues = {15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());

        BsonReader reader = createReaderForBytes(expectedValues);
        reader.readStartDocument();
        assertThat(reader.readBsonType(), is(BsonType.BOOLEAN));
        assertEquals("b1", reader.readName());
        assertEquals(true, reader.readBoolean());
        assertThat(reader.readBsonType(), is(BsonType.BOOLEAN));
        assertEquals("b2", reader.readName());
        assertEquals(false, reader.readBoolean());
        reader.readEndDocument();
    }

    @Test
    public void testWriteAndReadString() {
        writer.writeStartDocument();

        writer.writeString("s1", "");
        writer.writeString("s2", "danke");
        writer.writeString("s3", ",+\\\"<>;[]{}@#$%^&*()+_");
        writer.writeString("s4", "a\u00e9\u3042\u0430\u0432\u0431\u0434");

        writer.writeEndDocument();

        byte[] expectedValues = {82, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 2, 115, 50,
                                 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 2, 115, 51, 0, 23,
                                 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35,
                                 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 2, 115, 52, 0, 15, 0,
                                 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0,
                                 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());

        BsonReader reader = createReaderForBytes(expectedValues);
        reader.readStartDocument();

        assertThat(reader.readBsonType(), is(BsonType.STRING));
        assertEquals("s1", reader.readName());
        assertEquals("", reader.readString());

        assertThat(reader.readBsonType(), is(BsonType.STRING));
        assertEquals("s2", reader.readName());
        assertEquals("danke", reader.readString());

        assertThat(reader.readBsonType(), is(BsonType.STRING));
        assertEquals("s3", reader.readName());
        assertEquals(",+\\\"<>;[]{}@#$%^&*()+_", reader.readString());

        assertThat(reader.readBsonType(), is(BsonType.STRING));
        assertEquals("s4", reader.readName());
        assertEquals("a\u00e9\u3042\u0430\u0432\u0431\u0434", reader.readString());

        reader.readEndDocument();
    }

    @Test
    public void testWriteNumbers() {

        writer.writeStartDocument();

        writer.writeInt32("i1", -12);
        writer.writeInt32("i2", Integer.MIN_VALUE);
        writer.writeInt64("i3", Long.MAX_VALUE);
        writer.writeInt64("i4", 0);

        writer.writeEndDocument();

        byte[] expectedValues = {45, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 16, 105, 50, 0, 0, 0, 0, -128, 18,
                                 105,
                                 51, 0, -1, -1, -1, -1, -1, -1, -1, 127, 18, 105, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteArray() {

        writer.writeStartDocument();

        writer.writeStartArray("a1");
        writer.writeEndArray();
        writer.writeStartArray("a2");

        writer.writeStartArray();
        writer.writeEndArray();

        writer.writeEndArray();

        writer.writeEndDocument();

        byte[] expectedValues = {31, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0, 0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5,
                                 0,
                                 0, 0, 0, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteArrayElements() {

        writer.writeStartDocument();
        writer.writeStartArray("a1");
        writer.writeBoolean(true);
        writer.writeBoolean(false);
        writer.writeEndArray();
        writer.writeEndDocument();
        byte[] expectedValues = {22, 0, 0, 0, 4, 97, 49, 0, 13, 0, 0, 0, 8, 48, 0, 1, 8, 49, 0, 0, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteNull() {

        writer.writeStartDocument();

        writer.writeNull("n1");
        writer.writeName("n2");
        writer.writeNull();

        writer.writeEndDocument();

        byte[] expectedValues = {13, 0, 0, 0, 10, 110, 49, 0, 10, 110, 50, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteUndefined() {

        writer.writeStartDocument();

        writer.writeName("u1");
        writer.writeUndefined();
        writer.writeUndefined("u2");

        writer.writeEndDocument();

        byte[] expectedValues = {13, 0, 0, 0, 6, 117, 49, 0, 6, 117, 50, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteObjectId() {

        ObjectId id = new ObjectId("50d3332018c6a1d8d1662b61");

        writer.writeStartDocument();

        writer.writeObjectId("_id", id);

        writer.writeEndDocument();

        byte[] expectedValues = {22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102,
                                 43,
                                 97, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteJavaScript() {
        writer.writeStartDocument();

        writer.writeJavaScript("js1", "var i = 0");
        writer.writeJavaScriptWithScope("js2", "i++");
        writer.writeStartDocument();

        writer.writeInt32("x", 1);

        writer.writeEndDocument();

        writer.writeEndDocument();

        byte[] expectedValues = {53, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32,
                                 48,
                                 0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16,
                                 120, 0, 1, 0, 0, 0,
                                 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteMinMaxKeys() {

        writer.writeStartDocument();

        writer.writeMaxKey("k1");
        writer.writeMinKey("k2");
        writer.writeName("k3");
        writer.writeMaxKey();

        writer.writeEndDocument();

        byte[] expectedValues = {17, 0, 0, 0, 127, 107, 49, 0, -1, 107, 50, 0, 127, 107, 51, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteBinary() {

        writer.writeStartDocument();

        writer.writeBinaryData("b1", new BsonBinary(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        writer.writeBinaryData("b2", new BsonBinary(BsonBinarySubType.OLD_BINARY, new byte[]{1, 1, 1, 1, 1}));
        writer.writeBinaryData("b3", new BsonBinary(BsonBinarySubType.FUNCTION, new byte[]{}));

        writer.writeEndDocument();

        byte[] expectedValues = {49, 0, 0, 0, 5, 98, 49, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 98, 50, 0,
                                 9, 0,
                                 0, 0, 2, 5, 0, 0, 0, 1, 1, 1, 1, 1, 5, 98, 51, 0, 0, 0, 0, 0, 1, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteRegularExpression() {

        writer.writeStartDocument();

        writer.writeRegularExpression("r1", new BsonRegularExpression("([01]?[0-9][0-9]?)"));
        writer.writeRegularExpression("r2", new BsonRegularExpression("[ \\t]+$", "i"));

        writer.writeEndDocument();

        byte[] expectedValues = {43, 0, 0, 0, 11, 114, 49, 0, 40, 91, 48, 49, 93, 63, 91, 48, 45, 57, 93, 91, 48,
                                 45,
                                 57, 93, 63, 41, 0, 0, 11, 114, 50, 0, 91, 32, 92, 116, 93, 43, 36, 0, 105, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteTimestamp() {
        writer.writeStartDocument();

        writer.writeTimestamp("t1", new BsonTimestamp(123999401, 44332));

        writer.writeEndDocument();

        byte[] expectedValues = {17, 0, 0, 0, 17, 116, 49, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteDBPointer() {
        writer.writeStartDocument();

        BsonDbPointer dbPointer = new BsonDbPointer("my.test", new ObjectId("50d3332018c6a1d8d1662b61"));
        writer.writeDBPointer("pt", dbPointer);

        writer.writeEndDocument();

        byte[] expectedValues = {33, 0, 0, 0, 12, 112, 116, 0, 8, 0, 0, 0, 109, 121, 46, 116, 101, 115, 116, 0, 80, -45, 51, 32, 24, -58,
                                 -95, -40, -47, 102, 43, 97, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());

        BsonReader reader = createReaderForBytes(expectedValues);
        reader.readStartDocument();
        assertThat(reader.readBsonType(), is(BsonType.DB_POINTER));
        assertEquals("pt", reader.readName());
        assertEquals(dbPointer, reader.readDBPointer());
        reader.readEndDocument();
    }

    @Test
    //CHECKSTYLE:OFF
    public void testWriteRead() throws IOException {
        ObjectId oid1 = new ObjectId();

        writer.writeStartDocument();
        {
            writer.writeBoolean("b1", true);
            writer.writeBoolean("b2", false);
            writer.writeStartArray("a1");
            {
                writer.writeString("danke");
                writer.writeString("");
            }
            writer.writeEndArray();
            writer.writeStartDocument("d1");
            {
                writer.writeDouble("do", 60);
                writer.writeInt32("i32", 40);
                writer.writeInt64("i64", Long.MAX_VALUE);
            }
            writer.writeEndDocument();
            writer.writeJavaScriptWithScope("js1", "print x");
            writer.writeStartDocument();
            {
                writer.writeInt32("x", 1);
            }
            writer.writeEndDocument();
            writer.writeObjectId("oid1", oid1);
        }
        writer.writeEndDocument();

        assertEquals(139, buffer.getPosition());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);

        ByteBufferBsonInput basicInputBuffer = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray())));

        BsonBinaryReader reader = new BsonBinaryReader(basicInputBuffer);

        try {
            assertEquals(BsonType.DOCUMENT, reader.readBsonType());
            reader.readStartDocument();
            {
                assertEquals(BsonType.BOOLEAN, reader.readBsonType());
                assertEquals("b1", reader.readName());
                assertEquals(true, reader.readBoolean());

                assertEquals(BsonType.BOOLEAN, reader.readBsonType());
                assertEquals("b2", reader.readName());
                assertEquals(false, reader.readBoolean());

                assertEquals(BsonType.ARRAY, reader.readBsonType());
                assertEquals("a1", reader.readName());
                reader.readStartArray();
                {
                    assertEquals(BsonType.STRING, reader.readBsonType());
                    assertEquals("danke", reader.readString());

                    assertEquals(BsonType.STRING, reader.readBsonType());
                    assertEquals("", reader.readString());
                }
                assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
                reader.readEndArray();
                assertEquals("d1", reader.readName());
                reader.readStartDocument();
                {
                    assertEquals(BsonType.DOUBLE, reader.readBsonType());
                    assertEquals("do", reader.readName());
                    assertEquals(60, reader.readDouble(), 0);

                    assertEquals(BsonType.INT32, reader.readBsonType());
                    assertEquals("i32", reader.readName());
                    assertEquals(40, reader.readInt32());

                    assertEquals(BsonType.INT64, reader.readBsonType());
                    assertEquals("i64", reader.readName());
                    assertEquals(Long.MAX_VALUE, reader.readInt64());
                }
                assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
                reader.readEndDocument();

                assertEquals(BsonType.JAVASCRIPT_WITH_SCOPE, reader.readBsonType());
                assertEquals("js1", reader.readName());
                assertEquals("print x", reader.readJavaScriptWithScope());

                reader.readStartDocument();
                {
                    assertEquals(BsonType.INT32, reader.readBsonType());
                    assertEquals("x", reader.readName());
                    assertEquals(1, reader.readInt32());
                }
                assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
                reader.readEndDocument();

                assertEquals(BsonType.OBJECT_ID, reader.readBsonType());
                assertEquals("oid1", reader.readName());
                assertEquals(oid1, reader.readObjectId());

                assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
                reader.readEndDocument();

            }
        } finally {
            reader.close();
        }
    }
    //CHECKSTYLE:ON

    @Test
    public void testPipe() {
        writer.writeStartDocument();
        writer.writeBoolean("a", true);
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        try {
            BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
            try {
                newWriter.pipe(reader);
            } finally {
                reader.close();
            }
        } finally {
            newWriter.close();
        }
        assertArrayEquals(bytes, newBuffer.toByteArray());
    }

    @Test
    public void testPipeNestedDocument() {
        // {
        //    "value" : { "a" : true},
        //    "b"     : 2
        // }
        writer.writeStartDocument();
        writer.writeStartDocument("value");
        writer.writeBoolean("a", true);
        writer.writeEndDocument();
        writer.writeInt32("b", 2);
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        BsonBinaryReader reader1 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
        reader1.readStartDocument();
        reader1.readName();

        newWriter.pipe(reader1); //pipe {'a':true} to writer

        assertEquals(BsonType.INT32, reader1.readBsonType()); //continue reading from the same reader
        assertEquals("b", reader1.readName());
        assertEquals(2, reader1.readInt32());

        BsonBinaryReader reader2 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(newBuffer
                                                                                                                     .toByteArray()))));

        reader2.readStartDocument(); //checking what writer piped
        assertEquals(BsonType.BOOLEAN, reader2.readBsonType());
        assertEquals("a", reader2.readName());
        assertEquals(true, reader2.readBoolean());
        reader2.readEndDocument();
    }


    @Test
    public void testPipeDocumentIntoArray() {
        writer.writeStartDocument();
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        BsonBinaryReader reader1 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));

        newWriter.writeStartDocument();
        newWriter.writeStartArray("a");
        newWriter.pipe(reader1);
        newWriter.writeEndArray();
        newWriter.writeEndDocument();

        BsonBinaryReader reader2 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(newBuffer
                                                                                                                     .toByteArray()))));

        //checking what writer piped
        reader2.readStartDocument();
        reader2.readStartArray();
        reader2.readStartDocument();
        reader2.readEndDocument();
        reader2.readEndArray();
        reader2.readEndDocument();
    }

    @Test
    public void testPipeDocumentIntoDocument() {
        writer.writeStartDocument();
        writer.writeString("str", "value");
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        BsonBinaryReader reader1 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));

        newWriter.writeStartDocument();
        newWriter.writeName("doc");
        newWriter.pipe(reader1);
        newWriter.writeEndDocument();

        BsonBinaryReader reader2 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(newBuffer
                                                                                                                     .toByteArray()))));

        //checking what writer piped
        reader2.readStartDocument();
        assertEquals("doc", reader2.readName());
        reader2.readStartDocument();
        assertEquals("value", reader2.readString("str"));
        reader2.readEndDocument();
        reader2.readEndDocument();
    }

    @Test
    public void testPipeDocumentIntoTopLevel() {
        writer.writeStartDocument();
        writer.writeString("str", "value");
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        BsonBinaryReader reader1 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));

        newWriter.pipe(reader1);

        BsonBinaryReader reader2 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(newBuffer
                                                                                                                     .toByteArray()))));

        //checking what writer piped
        reader2.readStartDocument();
        assertEquals("value", reader2.readString("str"));
        reader2.readEndDocument();
    }

    @Test
    public void testPipeDocumentIntoScopeDocument() {
        writer.writeStartDocument();
        writer.writeInt32("i", 0);
        writer.writeEndDocument();

        byte[] bytes = buffer.toByteArray();

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        BsonBinaryReader reader1 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));

        newWriter.writeStartDocument();
        newWriter.writeJavaScriptWithScope("js", "i++");
        newWriter.pipe(reader1);
        newWriter.writeEndDocument();

        BsonBinaryReader reader2 = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(newBuffer
                                                                                                                     .toByteArray()))));

        //checking what writer piped
        reader2.readStartDocument();
        reader2.readJavaScriptWithScope("js");
        reader2.readStartDocument();
        assertEquals(0, reader2.readInt32("i"));
        reader2.readEndDocument();
        reader2.readEndDocument();
    }

    @Test
    public void testPipeOfDocumentWithInvalidSize() {
        byte[] bytes = {4, 0, 0, 0};  // minimum document size is 5;

        BasicOutputBuffer newBuffer = new BasicOutputBuffer();
        BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);
        try {
            BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
            try {
                newWriter.pipe(reader);
                fail("Pipe is expected to fail with document size is < 5");
            } catch (BsonSerializationException e) {
                // expected
            }
            finally {
                reader.close();
            }
        } finally {
            newWriter.close();
        }

    }

    // CHECKSTYLE:OFF
    @Test
    public void testMarkAndReset() throws IOException {
        writer.writeStartDocument();
        writer.writeStartArray("a");
        {
            writer.writeStartDocument();
            writer.writeInt32("i", 1);
            writer.writeEndDocument();
        }
        writer.mark();
        {
            writer.writeStartDocument();
            writer.writeInt32("i", 2);
            writer.writeEndDocument();
        }
        writer.reset();
        {
            writer.writeStartDocument();
            writer.writeInt32("i", 3);
            writer.writeEndDocument();
        }
        writer.writeEndArray();
        writer.writeEndDocument();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);

        ByteBufferBsonInput basicInputBuffer = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray())));

        BsonBinaryReader reader = new BsonBinaryReader(basicInputBuffer);

        try {
            reader.readStartDocument();
            reader.readName("a");
            reader.readStartArray();
            {
                reader.readStartDocument();
                assertEquals(1, reader.readInt32("i"));
                reader.readEndDocument();
            }
            {
                reader.readStartDocument();
                assertEquals(3, reader.readInt32("i"));
                reader.readEndDocument();
            }
            reader.readEndArray();
            reader.readEndDocument();
        } finally

        {
            reader.close();
        }
    }
    // CHECKSTYLE:ON

    private BsonBinaryReader createReaderForBytes(final byte[] bytes) {
        return new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
    }
}
