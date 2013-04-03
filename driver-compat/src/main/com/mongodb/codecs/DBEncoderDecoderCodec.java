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
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.MongoInternalException;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DBEncoderDecoderCodec implements Codec<DBObject> {

    private DBEncoder encoder;
    private final DBDecoder decoder;
    private final DBCollection collection;
    private BufferPool<ByteBuffer> bufferPool;

    public DBEncoderDecoderCodec(final DBEncoder encoder, final DBDecoder decoder,
                                 final DBCollection collection, final BufferPool<ByteBuffer> bufferPool) {
        this.encoder = encoder;
        this.decoder = decoder;
        this.collection = collection;
        this.bufferPool = bufferPool;
    }

    // TODO: this can be optimized to reduce copying of buffers.  For that we'd need an InputBuffer that could iterate
    //       over an array of ByteBuffer instances from a PooledByteBufferOutputBuffer
    @Override
    public void encode(final BSONWriter bsonWriter, final DBObject value) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try {
            encoder.writeObject(buffer, value);
            bsonWriter.pipe(new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(buffer.toByteArray()))));
        } finally {
            buffer.close();
        }
    }

    @Override
    public DBObject decode(final BSONReader reader) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        try {
            BSONBinaryWriter binaryWriter = new BSONBinaryWriter(buffer);
            binaryWriter.pipe(reader);
            final BufferExposingByteArrayOutputStream byteArrayOutputStream =
                    new BufferExposingByteArrayOutputStream(binaryWriter.getBuffer().size());
            binaryWriter.getBuffer().pipe(byteArrayOutputStream);
            return decoder.decode(new ByteArrayInputStream(byteArrayOutputStream.getInternalBytes()), collection);
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("impossible", e);
        } finally {
            buffer.close();
        }
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
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
