/**
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
 *
 */

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferInput;
import org.bson.types.ObjectId;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BSONBinaryWriterTest extends Assert {

    BSONBinaryWriter writer;
    BasicOutputBuffer buffer;

    @BeforeTest
    public void setup() {
        buffer = new BasicOutputBuffer();

        writer = new BSONBinaryWriter(
                new BsonWriterSettings(100), new BinaryWriterSettings(1024 * 1024),
                buffer);
    }


    @Test
    public void test() throws IOException {
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

        ByteBufferInput byteBufferInput = new ByteBufferInput(ByteBuffer.wrap(baos.toByteArray()));

        BSONBinaryReader reader = new BSONBinaryReader(new BsonReaderSettings(), byteBufferInput);

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

//
//                assertEquals(BsonType., reader.getNextBsonType());
//                assertEquals(, reader.read);
//
//                assertEquals(BsonType., reader.getNextBsonType());
//                assertEquals(, reader.read);
//
//                assertEquals(BsonType., reader.getNextBsonType());
//                assertEquals(, reader.read);
//
//                assertEquals(BsonType., reader.getNextBsonType());
//                assertEquals(, reader.read);
//
//                assertEquals(BsonType., reader.getNextBsonType());
//                assertEquals(, ) reader.read;

        }


    }
}
