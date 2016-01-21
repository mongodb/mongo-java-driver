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

import org.bson.io.ByteBufferBsonInput;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BsonBinaryReaderTest {

    @Test
    public void testReadDBPointer() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 12, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                                                                  -42, -60, -29, -116, -7, 111, -1, -36, 0});

        reader.readStartDocument();
        assertThat(reader.readBsonType(), is(BsonType.DB_POINTER));
        BsonDbPointer dbPointer = reader.readDBPointer();
        assertThat(dbPointer.getNamespace(), is("b"));
        assertThat(dbPointer.getId(), is(new ObjectId("5209296cd6c4e38cf96fffdc")));
        reader.readEndDocument();
        reader.close();
    }

    @Test
    public void testInvalidBsonType() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 22, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                -42, -60, -29, -116, -7, 111, -1, -36, 0});

        reader.readStartDocument();
        try {
            reader.readBsonType();
            fail("Should have thrown BsonSerializationException");
        } catch (BsonSerializationException e) {
            assertEquals("Detected unknown BSON type \"\\x16\" for fieldname \"a\". Are you using the latest driver version?",
                    e.getMessage());
        }
    }

    @Test
    public void testInvalidBsonTypeFollowedByInvalidCString() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 22, 97, 98});

        reader.readStartDocument();
        try {
            reader.readBsonType();
            fail("Should have thrown BsonSerializationException");
        } catch (BsonSerializationException e) {
            assertEquals("While decoding a BSON document 1 bytes were required, but only 0 remain", e.getMessage());
        }
    }

    private BsonBinaryReader createReaderForBytes(final byte[] bytes) {
        return new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
    }
}
