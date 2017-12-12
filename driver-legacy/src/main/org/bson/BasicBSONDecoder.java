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

import org.bson.io.Bits;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Basic implementation of BSONDecoder interface that creates BasicBSONObject instances
 */
public class BasicBSONDecoder implements BSONDecoder {

    @Override
    public BSONObject readObject(final byte[] bytes) {
        BSONCallback bsonCallback = new BasicBSONCallback();
        decode(bytes, bsonCallback);
        return (BSONObject) bsonCallback.get();
    }

    @Override
    public BSONObject readObject(final InputStream in) throws IOException {
        return readObject(readFully(in));
    }

    @Override
    public int decode(final byte[] bytes, final BSONCallback callback) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
        try {
            BsonWriter writer = new BSONCallbackAdapter(new BsonWriterSettings(), callback);
            writer.pipe(reader);
            return reader.getBsonInput().getPosition(); //TODO check this.
        } finally {
            reader.close();
        }
    }

    @Override
    public int decode(final InputStream in, final BSONCallback callback) throws IOException {
        return decode(readFully(in), callback);
    }

    private byte[] readFully(final InputStream input) throws IOException {
        byte[] sizeBytes = new byte[4];
        Bits.readFully(input, sizeBytes);
        int size = Bits.readInt(sizeBytes);

        byte[] buffer = new byte[size];
        System.arraycopy(sizeBytes, 0, buffer, 0, 4);
        Bits.readFully(input, buffer, 4, size - 4);
        return buffer;
    }
}
