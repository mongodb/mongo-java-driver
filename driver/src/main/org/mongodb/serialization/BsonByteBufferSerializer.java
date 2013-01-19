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
 *
 */

package org.mongodb.serialization;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.io.InputBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A simple ByteBuffer serializer which assumes that the ByteBuffer contains valid BSON.  It works by casting
 * the BSONWriter to a BSONBinaryWriter and directly copies the bytes into the underlying output buffer.
 * <p>
 * For deserialization, it does the reverse: Casts the BSONReader to a BSONBinaryReader and copies a single
 * document into a ByteBuffer.
 * </p>
 * <p>
 * This should even be usable as a nested document by adding an instance of it to a PrimitiveSerializers instance.
 * </p>
 */
// TODO: Handle case where reader or writer is not Binary?
// TODO: Integrate with a ByteBuffer cache?
public class BsonByteBufferSerializer implements CollectibleSerializer<ByteBuffer> {
    @Override
    public void serialize(final BSONWriter bsonWriter, final ByteBuffer value) {
        if (!(bsonWriter instanceof BSONBinaryWriter)) {
            throw new IllegalArgumentException("Must be a BSONBinaryWriter");
        }
        BSONBinaryWriter bsonBinaryWriter = (BSONBinaryWriter) bsonWriter;
        bsonBinaryWriter.getBuffer().write(value.array(), value.arrayOffset() + value.position(), value.remaining());
    }

    @Override
    public ByteBuffer deserialize(final BSONReader reader) {
        if (!(reader instanceof BSONBinaryReader)) {
            throw new IllegalArgumentException("Must be a BSONBinaryReader");
        }
        BSONBinaryReader bsonBinaryReader = (BSONBinaryReader) reader;

        InputBuffer buffer = bsonBinaryReader.getBuffer();
        int size = buffer.readInt32();
        ByteBuffer retVal = ByteBuffer.allocate(size);
        retVal.order(ByteOrder.LITTLE_ENDIAN);
        retVal.putInt(size);
        retVal.put(buffer.readBytes(size - 4));
        return retVal;
    }

    @Override
    public Class<ByteBuffer> getSerializationClass() {
        return ByteBuffer.class;
    }

    @Override
    public Object getId(final ByteBuffer document) {
        return null;
    }
}
