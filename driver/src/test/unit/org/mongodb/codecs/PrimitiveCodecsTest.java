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

package org.mongodb.codecs;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.DBRef;
import org.mongodb.Decoder;
import org.mongodb.json.JSONReader;
import org.mongodb.json.JSONWriter;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
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
    public void shouldBeAbleToDecodeString() {
        assertThat(primitiveCodecs.canDecode(String.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeObjectId() {
        assertThat(primitiveCodecs.canDecode(ObjectId.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeInteger() {
        assertThat(primitiveCodecs.canDecode(Integer.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeLong() {
        assertThat(primitiveCodecs.canDecode(Long.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeDouble() {
        assertThat(primitiveCodecs.canDecode(Double.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeDate() {
        assertThat(primitiveCodecs.canDecode(Date.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeTimestamp() {
        assertThat(primitiveCodecs.canDecode(BSONTimestamp.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeBoolean() {
        assertThat(primitiveCodecs.canDecode(Boolean.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodePattern() {
        assertThat(primitiveCodecs.canDecode(Pattern.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeMinKey() {
        assertThat(primitiveCodecs.canDecode(MinKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeMaxKey() {
        assertThat(primitiveCodecs.canDecode(MaxKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeCode() {
        assertThat(primitiveCodecs.canDecode(Code.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeNull() {
        assertThat(primitiveCodecs.canDecode(null), is(true));
    }

    //these are classes that have encoders but not decoders, not symmetrical
    @Test
    public void shouldNotBeAbleToDecodeByteArray() {
        assertThat(primitiveCodecs.canDecode(byte[].class), is(false));
    }

    @Test
    public void shouldNotBeAbleToDecodeShort() {
        assertThat(primitiveCodecs.canDecode(Short.class), is(false));
    }

    @Test
    public void shouldNotBeAbleToDecodeBinary() {
        assertThat(primitiveCodecs.canDecode(Binary.class), is(false));
    }

    @Test
    public void shouldNotBeAbleToDecodeFloat() {
        assertThat(primitiveCodecs.canDecode(Float.class), is(false));
    }

    @Test
    public void shouldNotBeAbleToDecodeByte() {
        assertThat(primitiveCodecs.canDecode(Byte.class), is(false));
    }

    @Test
    public void shouldBeAbleToDecodeDBPointer() {
        byte[] bytes = {26, 0, 0, 0, 12, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                        -42, -60, -29, -116, -7, 111, -1, -36, 0
        };
        BSONReader reader = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(bytes))), true);

        reader.readStartDocument();
        reader.readName();

        Object object = primitiveCodecs.decode(reader);

        assertThat(object, instanceOf(DBRef.class));
        DBRef reference = (DBRef) object;
        assertThat(reference.getRef(), is("b"));
        assertThat(reference.getId(), instanceOf(ObjectId.class));
        assertThat((ObjectId) reference.getId(), is(new ObjectId("5209296cd6c4e38cf96fffdc")));
    }

    @Test
    public void testOtherDecoderMethod() {
        @SuppressWarnings("rawtypes")
        PrimitiveCodecs codecs = PrimitiveCodecs.builder(primitiveCodecs).otherDecoder(BSONType.BINARY, new Decoder() {
            @Override
            public Object decode(final BSONReader reader) {
                return reader.readBinaryData().getData();
            }
        }).build();
        StringWriter stringWriter = new StringWriter();
        BSONWriter bsonWriter = new JSONWriter(stringWriter);
        Binary binaryValue = new Binary(BSONBinarySubType.BINARY, new byte[]{1, 2, 3});
        bsonWriter.writeStartDocument();
        bsonWriter.writeBinaryData("binary", binaryValue);
        bsonWriter.writeEndDocument();
        BSONReader bsonReader = new JSONReader(stringWriter.toString());
        bsonReader.readStartDocument();
        bsonReader.readName();
        assertArrayEquals(binaryValue.getData(), (byte[]) codecs.decode(bsonReader));
    }
}
