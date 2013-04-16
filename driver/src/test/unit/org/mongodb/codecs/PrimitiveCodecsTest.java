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

import org.bson.BSONBinaryReader;
import org.bson.BSONBinarySubType;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.Decoder;
import org.mongodb.json.JSONReader;
import org.mongodb.json.JSONWriter;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class PrimitiveCodecsTest {
    private final PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();

    @Test
    public void shouldBeAbleToEncodeString() {
        assertThat(primitiveCodecs.canEncode(String.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeObjectId() {
        assertThat(primitiveCodecs.canEncode(ObjectId.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeInteger() {
        System.out.println(Integer.class);
        assertThat(primitiveCodecs.canEncode(Integer.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeLong() {
        assertThat(primitiveCodecs.canEncode(Long.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeDouble() {
        assertThat(primitiveCodecs.canEncode(Double.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeBinary() {
        assertThat(primitiveCodecs.canEncode(Binary.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeDate() {
        assertThat(primitiveCodecs.canEncode(Date.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeTimestamp() {
        assertThat(primitiveCodecs.canEncode(BSONTimestamp.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeBoolean() {
        assertThat(primitiveCodecs.canEncode(Boolean.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodePattern() {
        assertThat(primitiveCodecs.canEncode(Pattern.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeMinKey() {
        assertThat(primitiveCodecs.canEncode(MinKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeMaxKey() {
        assertThat(primitiveCodecs.canEncode(MaxKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeCode() {
        assertThat(primitiveCodecs.canEncode(Code.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeNull() {
        assertThat(primitiveCodecs.canEncode(null), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeFloat() {
        assertThat(primitiveCodecs.canEncode(Float.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeShort() {
        assertThat(primitiveCodecs.canEncode(Short.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeByte() {
        assertThat(primitiveCodecs.canEncode(Byte.class), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeByteArray() {
        assertThat(primitiveCodecs.canEncode(byte[].class), is(true));
    }

    @Test
    public void testOtherDecoder() {
        PrimitiveCodecs codecs = PrimitiveCodecs.builder(primitiveCodecs).otherDecoder(BSONType.BINARY, new Decoder() {
            @Override
            public Object decode(final BSONReader reader) {
                return reader.readBinaryData().getData();
            }
        }).build();
        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer);
        final Binary binaryValue = new Binary(BSONBinarySubType.Binary, new byte[]{1, 2, 3});
        bsonWriter.writeStartDocument();
        bsonWriter.writeBinaryData("binary", binaryValue);
        bsonWriter.writeEndDocument();
        BSONReader bsonReader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(outputBuffer.toByteArray())));
        bsonReader.readStartDocument();
        bsonReader.readName();
        assertArrayEquals(binaryValue.getData(), (byte[]) codecs.decode(bsonReader));
    }

    @Test
    public void testOtherDecoder1() {
        PrimitiveCodecs codecs = PrimitiveCodecs.builder(primitiveCodecs).otherDecoder(BSONType.BINARY, new Decoder() {
            @Override
            public Object decode(final BSONReader reader) {
                return reader.readBinaryData().getData();
            }
        }).build();
        final StringWriter stringWriter = new StringWriter();
        BSONWriter bsonWriter = new JSONWriter(stringWriter);
        final Binary binaryValue = new Binary(BSONBinarySubType.Binary, new byte[]{1, 2, 3});
        bsonWriter.writeStartDocument();
        bsonWriter.writeBinaryData("binary", binaryValue);
        bsonWriter.writeEndDocument();
        BSONReader bsonReader = new JSONReader(stringWriter.toString());
        bsonReader.readStartDocument();
        bsonReader.readName();
        assertArrayEquals(binaryValue.getData(), (byte[]) codecs.decode(bsonReader));
    }

//    @Test
//    public void testOtherDecoder2() {
//        PrimitiveCodecs codecs = PrimitiveCodecs.builder(primitiveCodecs).otherDecoder(BSONType.BINARY, new Decoder() {
//            @Override
//            public Object decode(final BSONReader reader) {
//                return reader.readBinaryData().getData();
//            }
//        }).build();
//        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
//        BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer);
//        final Binary binaryValue = new Binary(BSONBinarySubType.Binary, new byte[]{1, 2, 3});
//        bsonWriter.writeBinaryData(binaryValue);
//        BSONReader bsonReader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(outputBuffer.toByteArray())));
//        assertArrayEquals(binaryValue.getData(), (byte[]) codecs.decode(bsonReader));
//    }
//
//    @Test
//    public void testOtherDecoder3() {
//        PrimitiveCodecs codecs = PrimitiveCodecs.builder(primitiveCodecs).otherDecoder(BSONType.BINARY, new Decoder() {
//            @Override
//            public Object decode(final BSONReader reader) {
//                return reader.readBinaryData().getData();
//            }
//        }).build();
//        final StringWriter stringWriter = new StringWriter();
//        BSONWriter bsonWriter = new JSONWriter(stringWriter);
//        final Binary binaryValue = new Binary(BSONBinarySubType.Binary, new byte[]{1, 2, 3});
//        bsonWriter.writeBinaryData(binaryValue);
//        System.out.println(stringWriter.toString());
//        BSONReader bsonReader = new JSONReader(stringWriter.toString());
//        assertArrayEquals(binaryValue.getData(), (byte[]) codecs.decode(bsonReader));
//    }
}
