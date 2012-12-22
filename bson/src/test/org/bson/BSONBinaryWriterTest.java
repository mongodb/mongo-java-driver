/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import org.bson.io.ByteBufferInput;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BSONBinaryWriterTest {

    BSONBinaryWriter writer;
    BasicOutputBuffer buffer;
    BSONBinaryReader reader;


    @Before
    public void setup() {
        buffer = new BasicOutputBuffer();

        writer = new BSONBinaryWriter(new BsonWriterSettings(100), new BinaryWriterSettings(1024 * 1024), buffer);
    }

    @Test
    public void testWriterStates() {
        {
            try {
                writer.writeBoolean("b1", true);
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeStartArray();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeNull();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }
        }

        writer.writeStartDocument();

        {
            try {
                writer.writeString("somestring");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeEndArray();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }
        }

        writer.writeName("f1");

        {
            try {
                writer.writeName("i2");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeEndDocument();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeDouble(100);

        {
            try {
                writer.writeString("i2");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeJavaScript("var i");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeStartArray("f2");

        {
            try {
                writer.writeName("i3");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeEndDocument();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeStartDocument();

        {
            try {
                writer.writeEndArray();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeEndDocument();

        {
            try {
                writer.writeName("i3");
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeStartArray();
        writer.writeStartArray();
        writer.writeStartArray();

        {
            try {
                writer.writeInt64("i4", 10);
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeUndefined();
        writer.writeMaxKey();


        writer.writeEndArray();
        writer.writeEndArray();
        writer.writeEndArray();
        writer.writeEndArray();

        {
            try {
                writer.writeEndArray();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

        }

        writer.writeJavaScriptWithScope("js1", "var i = 1");

        {
            try {
                writer.writeBoolean("b4", true);
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeBinaryData(new Binary(new byte[]{0, 0, 1, 0}));
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeStartArray();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }

            try {
                writer.writeEndDocument();
                fail("Exception not thrown");
            } catch (Exception e) {
                assertThat(e, instanceOf(InvalidOperationException.class));
            }
        }

        writer.writeStartDocument();
        writer.writeEndDocument();


        writer.writeEndDocument();
    }

    @Test
    public void testWriteBoolean() {
        writer.writeStartDocument();
        {
            writer.writeBoolean("b1", true);
            writer.writeBoolean("b2", false);
        }
        writer.writeEndDocument();

        byte[] expecteds = {15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());

    }

    @Test
    public void testWriteString() {
        writer.writeStartDocument();
        {
            writer.writeString("s1", "");
            writer.writeString("s2", "danke");
            writer.writeString("s3", ",+\\\"<>;[]{}@#$%^&*()+_");
            writer.writeString("s5", "a\u00e9\u3042\u0430\u0432\u0431\u0434");
        }
        writer.writeEndDocument();

        byte[] expecteds = {82, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 2, 115, 50, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 2, 115, 51, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 2, 115, 53, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());

    }

    @Test
    public void testWriteNumbers() {

        writer.writeStartDocument();
        {
            writer.writeInt32("i1", -12);
            writer.writeInt32("i2", Integer.MIN_VALUE);
            writer.writeInt64("i3", Long.MAX_VALUE);
            writer.writeInt64("i4", 0);
        }
        writer.writeEndDocument();

        byte[] expecteds = {45, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 16, 105, 50, 0, 0, 0, 0, -128, 18, 105, 51, 0, -1, -1, -1, -1, -1, -1, -1, 127, 18, 105, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteArray() {

        writer.writeStartDocument();
        {
            writer.writeStartArray("a1");
            writer.writeEndArray();
            writer.writeStartArray("a2");
            {
                writer.writeStartArray();
                writer.writeEndArray();
            }
            writer.writeEndArray();
        }
        writer.writeEndDocument();

        byte[] expecteds = {31, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0, 0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5, 0, 0, 0, 0, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteNull() {

        writer.writeStartDocument();
        {
            writer.writeNull("n1");
            writer.writeName("n2");
            writer.writeNull();
        }
        writer.writeEndDocument();

        byte[] expecteds = {13, 0, 0, 0, 10, 110, 49, 0, 10, 110, 50, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteUndefined() {

        writer.writeStartDocument();
        {
            writer.writeName("u1");
            writer.writeUndefined();
            writer.writeUndefined("u2");
        }
        writer.writeEndDocument();

        byte[] expecteds = {13, 0, 0, 0, 6, 117, 49, 0, 6, 117, 50, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteObjectId() {

        ObjectId id = ObjectId.massageToObjectId("50d3332018c6a1d8d1662b61");

        writer.writeStartDocument();
        {
            writer.writeObjectId("_id", id);
        }
        writer.writeEndDocument();

        byte[] expecteds = {22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteJavaScript() {

        writer.writeStartDocument();
        {
            writer.writeJavaScript("js1", "var i = 0");
            writer.writeJavaScriptWithScope("js2", "i++");
            writer.writeStartDocument();
            {
                writer.writeInt32("x", 1);
            }
            writer.writeEndDocument();
        }
        writer.writeEndDocument();

        byte[] expecteds = {53, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteMinMaxKeys() {

        writer.writeStartDocument();
        {
            writer.writeMaxKey("k1");
            writer.writeMinKey("k2");
            writer.writeName("k3");
            writer.writeMaxKey();
        }
        writer.writeEndDocument();

        for (byte b : buffer.toByteArray()) {
            System.out.print(b + ", ");
        }

        byte[] expecteds = {17, 0, 0, 0, 127, 107, 49, 0, -1, 107, 50, 0, 127, 107, 51, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteBinary() {

        writer.writeStartDocument();
        {
            writer.writeBinaryData("b1", new Binary(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
            writer.writeBinaryData("b2", new Binary(BSON.B_BINARY, new byte[]{1, 1, 1, 1, 1}));
            writer.writeBinaryData("b3", new Binary(BSON.B_FUNC, new byte[]{}));
        }
        writer.writeEndDocument();

        byte[] expecteds = {49, 0, 0, 0, 5, 98, 49, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 98, 50, 0, 9, 0, 0, 0, 2, 5, 0, 0, 0, 1, 1, 1, 1, 1, 5, 98, 51, 0, 0, 0, 0, 0, 1, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteRegularExpression() {

        writer.writeStartDocument();
        {
            writer.writeRegularExpression("r1", new RegularExpression("([01]?[0-9][0-9]?)"));
            writer.writeRegularExpression("r2", new RegularExpression("[ \\t]+$","i"));
        }
        writer.writeEndDocument();

        byte[] expecteds = {43, 0, 0, 0, 11, 114, 49, 0, 40, 91, 48, 49, 93, 63, 91, 48, 45, 57, 93, 91, 48, 45, 57, 93, 63, 41, 0, 0, 11, 114, 50, 0, 91, 32, 92, 116, 93, 43, 36, 0, 105, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }

    @Test
    public void testWriteTimestamp() {

        writer.writeStartDocument();
        {
            writer.writeTimestamp("t1", 123999401);
        }
        writer.writeEndDocument();

        byte[] expecteds = {17, 0, 0, 0, 17, 116, 49, 0, -87, 20, 100, 7, 0, 0, 0, 0, 0};
        assertArrayEquals(expecteds, buffer.toByteArray());
    }


    @Test
    public void testWriteRead() throws IOException {
        final ObjectId oid1 = new ObjectId();

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

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);

        final ByteBufferInput byteBufferInput = new ByteBufferInput(ByteBuffer.wrap(baos.toByteArray()));

        final BSONBinaryReader reader = new BSONBinaryReader(new BsonReaderSettings(), byteBufferInput);

        assertEquals(BsonType.DOCUMENT, reader.getNextBsonType());
        reader.readStartDocument();
        {
            assertEquals("b1", reader.readName());
            assertEquals(BsonType.BOOLEAN, reader.getNextBsonType());
            assertEquals(true, reader.readBoolean());

            assertEquals("b2", reader.readName());
            assertEquals(BsonType.BOOLEAN, reader.getNextBsonType());
            assertEquals(false, reader.readBoolean());

            assertEquals("a1", reader.readName());
            assertEquals(BsonType.ARRAY, reader.getNextBsonType());
            reader.readStartArray();
            {
                assertEquals(BsonType.STRING, reader.getNextBsonType());
                assertEquals("danke", reader.readString());

                assertEquals(BsonType.STRING, reader.getNextBsonType());
                assertEquals("", reader.readString());
            }
            assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
            reader.readEndArray();
            assertEquals("d1", reader.readName());
            reader.readStartDocument();
            {
                assertEquals("do", reader.readName());
                assertEquals(BsonType.DOUBLE, reader.getNextBsonType());
                assertEquals(60, reader.readDouble(), 0);

                assertEquals("i32", reader.readName());
                assertEquals(BsonType.INT32, reader.getNextBsonType());
                assertEquals(40, reader.readInt32());

                assertEquals("i64", reader.readName());
                assertEquals(BsonType.INT64, reader.getNextBsonType());
                assertEquals(Long.MAX_VALUE, reader.readInt64());
            }
            assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
            reader.readEndDocument();

            assertEquals("js1", reader.readName());
            assertEquals(BsonType.JAVASCRIPT_WITH_SCOPE, reader.getNextBsonType());
            assertEquals("print x", reader.readJavaScriptWithScope());

            reader.readStartDocument();
            {
                assertEquals("x", reader.readName());
                assertEquals(BsonType.INT32, reader.getNextBsonType());
                assertEquals(1, reader.readInt32());
            }
            assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
            reader.readEndDocument();

            assertEquals("oid1", reader.readName());
            assertEquals(BsonType.OBJECT_ID, reader.getNextBsonType());
            assertEquals(oid1, reader.readObjectId());

            assertEquals(BsonType.END_OF_DOCUMENT, reader.readBsonType());
            reader.readEndDocument();

        }


    }
}
