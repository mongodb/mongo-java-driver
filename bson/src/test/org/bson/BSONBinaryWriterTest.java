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

package org.bson;

import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests BSONBinaryWriter
 */
public class BSONBinaryWriterTest {

    private BSONBinaryWriter writer;
    private BasicOutputBuffer buffer;

    @Before
    public void setup() {
        buffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(new BSONWriterSettings(100), new BSONBinaryWriterSettings(1024 * 1024), buffer, true);
    }

    @After
    public void tearDown() {
        writer.close();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument() {
        writer.writeBoolean("b1", true);
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionForArrayWhenWritingBeforeStartingDocument() {
        writer.writeStartArray();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionForNullWhenWritingBeforeStartingDocument() {
        writer.writeNull();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionForStringWhenStateIsValue() {
        writer.writeStartDocument();
        writer.writeString("SomeString");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue() {
        writer.writeStartDocument();
        writer.writeEndArray();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionWhenWritingASecondName() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeName("i2");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeEndDocument();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenTryingToWriteASecondValue() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeString("i2");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenTryingToWriteJavaScript() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeJavaScript("var i");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenWritingANameInAnArray() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeName("i3");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeEndDocument();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenEndingAnArrayInASubDocument() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndArray();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray() {
        //Does this test even make sense?
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndDocument();
        writer.writeName("i3");
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowExceptionWhenWritingObjectsIntoNestedArrays() {
        //This test seem redundant?
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartArray();
        writer.writeStartArray();
        writer.writeStartArray();
        writer.writeInt64("i4", 10);
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeEndArray();
        writer.writeEndArray();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope1() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeBoolean("b4", true);
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope2() {
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeBinaryData(new Binary(new byte[]{0, 0, 1, 0}));
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope3() {
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeStartArray();
    }

    @Test(expected = BSONInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope4() {
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeEndDocument();
    }

    @Test
    public void shouldNotThrowAnExceptionIfCorrectlyStartingAndEndingDocumentsAndSubdocuments() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeStartDocument();
        writer.writeEndDocument();

        writer.writeEndDocument();
    }

    @Test
    public void testWriteBoolean() {
        writer.writeStartDocument();
        writer.writeBoolean("b1", true);
        writer.writeBoolean("b2", false);
        writer.writeEndDocument();

        final byte[] expectedValues = {15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());

    }

    @Test
    public void testWriteString() {
        writer.writeStartDocument();

        writer.writeString("s1", "");
        writer.writeString("s2", "danke");
        writer.writeString("s3", ",+\\\"<>;[]{}@#$%^&*()+_");
        writer.writeString("s5", "a\u00e9\u3042\u0430\u0432\u0431\u0434");

        writer.writeEndDocument();

        final byte[] expectedValues = {82, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 2, 115, 50,
                                       0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 2, 115, 51, 0, 23,
                                       0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35,
                                       36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 2, 115, 53, 0, 15, 0,
                                       0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0,
                                       0};
        assertArrayEquals(expectedValues, buffer.toByteArray());

    }

    @Test
    public void testWriteNumbers() {

        writer.writeStartDocument();

        writer.writeInt32("i1", -12);
        writer.writeInt32("i2", Integer.MIN_VALUE);
        writer.writeInt64("i3", Long.MAX_VALUE);
        writer.writeInt64("i4", 0);

        writer.writeEndDocument();

        final byte[] expectedValues = {45, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 16, 105, 50, 0, 0, 0, 0, -128, 18,
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

        final byte[] expectedValues = {31, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0, 0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5,
                                       0,
                                       0, 0, 0, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteNull() {

        writer.writeStartDocument();

        writer.writeNull("n1");
        writer.writeName("n2");
        writer.writeNull();

        writer.writeEndDocument();

        final byte[] expectedValues = {13, 0, 0, 0, 10, 110, 49, 0, 10, 110, 50, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteUndefined() {

        writer.writeStartDocument();

        writer.writeName("u1");
        writer.writeUndefined();
        writer.writeUndefined("u2");

        writer.writeEndDocument();

        final byte[] expectedValues = {13, 0, 0, 0, 6, 117, 49, 0, 6, 117, 50, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteObjectId() {

        final ObjectId id = new ObjectId("50d3332018c6a1d8d1662b61");

        writer.writeStartDocument();

        writer.writeObjectId("_id", id);

        writer.writeEndDocument();

        final byte[] expectedValues = {22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102,
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

        final byte[] expectedValues = {53, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32,
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

        for (final byte b : buffer.toByteArray()) {
            System.out.print(b + ", ");
        }

        final byte[] expectedValues = {17, 0, 0, 0, 127, 107, 49, 0, -1, 107, 50, 0, 127, 107, 51, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteBinary() {

        writer.writeStartDocument();

        writer.writeBinaryData("b1", new Binary(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        writer.writeBinaryData("b2", new Binary(BSONBinarySubType.OldBinary, new byte[]{1, 1, 1, 1, 1}));
        writer.writeBinaryData("b3", new Binary(BSONBinarySubType.Function, new byte[]{}));

        writer.writeEndDocument();

        final byte[] expectedValues = {49, 0, 0, 0, 5, 98, 49, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 98, 50, 0,
                                       9, 0,
                                       0, 0, 2, 5, 0, 0, 0, 1, 1, 1, 1, 1, 5, 98, 51, 0, 0, 0, 0, 0, 1, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteRegularExpression() {

        writer.writeStartDocument();

        writer.writeRegularExpression("r1", new RegularExpression("([01]?[0-9][0-9]?)"));
        writer.writeRegularExpression("r2", new RegularExpression("[ \\t]+$", "i"));

        writer.writeEndDocument();

        final byte[] expectedValues = {43, 0, 0, 0, 11, 114, 49, 0, 40, 91, 48, 49, 93, 63, 91, 48, 45, 57, 93, 91, 48,
                                       45,
                                       57, 93, 63, 41, 0, 0, 11, 114, 50, 0, 91, 32, 92, 116, 93, 43, 36, 0, 105, 0, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    public void testWriteTimestamp() {
        writer.writeStartDocument();

        writer.writeTimestamp("t1", new BSONTimestamp(123999401, 44332));

        writer.writeEndDocument();

        final byte[] expectedValues = {17, 0, 0, 0, 17, 116, 49, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0};
        assertArrayEquals(expectedValues, buffer.toByteArray());
    }

    @Test
    //CHECKSTYLE:OFF
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

        final BasicInputBuffer basicInputBuffer = new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray())));

        final BSONBinaryReader reader = new BSONBinaryReader(new BSONReaderSettings(), basicInputBuffer, false);

        try {
            assertEquals(BSONType.DOCUMENT, reader.readBSONType());
            reader.readStartDocument();
            {
                assertEquals(BSONType.BOOLEAN, reader.readBSONType());
                assertEquals("b1", reader.readName());
                assertEquals(true, reader.readBoolean());

                assertEquals(BSONType.BOOLEAN, reader.readBSONType());
                assertEquals("b2", reader.readName());
                assertEquals(false, reader.readBoolean());

                assertEquals(BSONType.ARRAY, reader.readBSONType());
                assertEquals("a1", reader.readName());
                reader.readStartArray();
                {
                    assertEquals(BSONType.STRING, reader.readBSONType());
                    assertEquals("danke", reader.readString());

                    assertEquals(BSONType.STRING, reader.readBSONType());
                    assertEquals("", reader.readString());
                }
                assertEquals(BSONType.END_OF_DOCUMENT, reader.readBSONType());
                reader.readEndArray();
                assertEquals("d1", reader.readName());
                reader.readStartDocument();
                {
                    assertEquals(BSONType.DOUBLE, reader.readBSONType());
                    assertEquals("do", reader.readName());
                    assertEquals(60, reader.readDouble(), 0);

                    assertEquals(BSONType.INT32, reader.readBSONType());
                    assertEquals("i32", reader.readName());
                    assertEquals(40, reader.readInt32());

                    assertEquals(BSONType.INT64, reader.readBSONType());
                    assertEquals("i64", reader.readName());
                    assertEquals(Long.MAX_VALUE, reader.readInt64());
                }
                assertEquals(BSONType.END_OF_DOCUMENT, reader.readBSONType());
                reader.readEndDocument();

                assertEquals(BSONType.JAVASCRIPT_WITH_SCOPE, reader.readBSONType());
                assertEquals("js1", reader.readName());
                assertEquals("print x", reader.readJavaScriptWithScope());

                reader.readStartDocument();
                {
                    assertEquals(BSONType.INT32, reader.readBSONType());
                    assertEquals("x", reader.readName());
                    assertEquals(1, reader.readInt32());
                }
                assertEquals(BSONType.END_OF_DOCUMENT, reader.readBSONType());
                reader.readEndDocument();

                assertEquals(BSONType.OBJECT_ID, reader.readBSONType());
                assertEquals("oid1", reader.readName());
                assertEquals(oid1, reader.readObjectId());

                assertEquals(BSONType.END_OF_DOCUMENT, reader.readBSONType());
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

        byte[] bytes = writer.getBuffer().toByteArray();

        BSONBinaryWriter newWriter = new BSONBinaryWriter(new BasicOutputBuffer(), true);
        try {
            final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(bytes))), true);
            try {
                newWriter.pipe(reader);
            } finally {
                reader.close();
            }
        } finally {
            newWriter.close();
        }
        assertArrayEquals(bytes, newWriter.getBuffer().toByteArray());
    }

    @Test
    public void testPipeNestedDocument() {
        writer.writeStartDocument();
        writer.writeStartDocument("value");
        writer.writeBoolean("a", true);
        writer.writeEndDocument();
        writer.writeInt32("b", 2);
        writer.writeEndDocument();

        byte[] bytes = writer.getBuffer().toByteArray();

        BSONBinaryWriter newWriter = new BSONBinaryWriter(new BasicOutputBuffer(), true);
        final BSONBinaryReader reader1 = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(bytes))), true);
        reader1.readStartDocument();
        reader1.readName();

        newWriter.pipe(reader1); //pipe {'a':true} to writer

        assertEquals(BSONType.INT32, reader1.readBSONType()); //continue reading from the same reader
        assertEquals("b", reader1.readName());
        assertEquals(2, reader1.readInt32());

        final BSONBinaryReader reader2 = new BSONBinaryReader(
                new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(newWriter.getBuffer().toByteArray()))),
                true
        );

        reader2.readStartDocument(); //checking what writer piped
        assertEquals(BSONType.BOOLEAN, reader2.readBSONType());
        assertEquals("a", reader2.readName());
        assertEquals(true, reader2.readBoolean());
        reader2.readEndDocument();
    }

}
