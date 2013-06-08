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

package com.mongodb.codecs;

import com.mongodb.DBCollection;
import com.mongodb.DBDecoder;
import com.mongodb.DBObject;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReader;
import org.mongodb.Decoder;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DBDecoderAdapter implements Decoder<DBObject> {
    private final DBDecoder decoder;
    private final DBCollection collection;
    private BufferProvider bufferProvider;

    public DBDecoderAdapter(final DBDecoder decoder, final DBCollection collection, final BufferProvider bufferProvider) {
        this.decoder = decoder;
        this.collection = collection;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public DBObject decode(final BSONReader reader) {
        BSONBinaryWriter binaryWriter = new BSONBinaryWriter(new PooledByteBufferOutputBuffer(bufferProvider), true);
        try {
            binaryWriter.pipe(reader);
            final BufferExposingByteArrayOutputStream byteArrayOutputStream =
                    new BufferExposingByteArrayOutputStream(binaryWriter.getBuffer().size());
            binaryWriter.getBuffer().pipe(byteArrayOutputStream);
            return decoder.decode(byteArrayOutputStream.getInternalBytes(), collection);
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("impossible", e);
        } finally {
            binaryWriter.close();
        }
    }

    // Just so we don't have to copy the buffer
    private static class BufferExposingByteArrayOutputStream extends ByteArrayOutputStream {
        BufferExposingByteArrayOutputStream(final int size) {
            super(size);
        }

        byte[] getInternalBytes() {
            return buf;
        }
    }
}
