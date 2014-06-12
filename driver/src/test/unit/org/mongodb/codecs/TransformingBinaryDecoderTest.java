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

import org.bson.BsonBinarySubType;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.types.Binary;
import org.junit.Test;

import java.io.StringWriter;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TransformingBinaryDecoderTest {

    @Test
    public void testDecode() {
        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = new JsonWriter(stringWriter);
        writer.writeStartDocument();
        writer.writeBinaryData("subtype0", new Binary(BsonBinarySubType.BINARY, new byte[]{0}));
        writer.writeBinaryData("subtype1", new Binary(BsonBinarySubType.FUNCTION, new byte[]{1}));
        writer.writeBinaryData("subtype2", new Binary(BsonBinarySubType.OLD_BINARY, new byte[]{2}));

        writer.writeName("subtype3");
        new UUIDCodec().encode(writer, UUID.randomUUID());

        writer.writeBinaryData("subtype4", new Binary(BsonBinarySubType.UUID_STANDARD, new byte[]{4}));
        writer.writeBinaryData("subtype5", new Binary(BsonBinarySubType.MD5, new byte[]{5}));
        writer.writeBinaryData("subtype80", new Binary(BsonBinarySubType.USER_DEFINED, new byte[]{(byte) 0x80}));
        writer.writeEndDocument();

        JsonReader reader = new JsonReader(stringWriter.toString());
        TransformingBinaryDecoder decoder = new TransformingBinaryDecoder();
        reader.readStartDocument();

        reader.readName("subtype0");
        Object decoded = decoder.decode(reader);
        assertEquals(byte[].class, decoded.getClass());

        reader.readName("subtype1");
        decoded = decoder.decode(reader);
        assertEquals(Binary.class, decoded.getClass());

        reader.readName("subtype2");
        decoded = decoder.decode(reader);
        assertEquals(byte[].class, decoded.getClass());

        reader.readName("subtype3");
        decoded = decoder.decode(reader);
        assertEquals(UUID.class, decoded.getClass());

        reader.readName("subtype4");
        decoded = decoder.decode(reader);
        assertEquals(Binary.class, decoded.getClass());

        reader.readName("subtype5");
        decoded = decoder.decode(reader);
        assertEquals(Binary.class, decoded.getClass());

        reader.readName("subtype80");
        decoded = decoder.decode(reader);
        assertEquals(Binary.class, decoded.getClass());

        reader.readEndDocument();
    }
}
