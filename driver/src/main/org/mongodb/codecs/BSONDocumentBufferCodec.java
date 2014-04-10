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
import org.bson.BSONBinaryWriter;
import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.io.BasicInputBuffer;
import org.mongodb.BSONDocumentBuffer;
import org.mongodb.CollectibleCodec;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ByteBufferOutputBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A simple BSONDocumentBuffer codec.  It does not attempt to validate the contents of the underlying ByteBuffer. It assumes that it
 * contains a single encoded BSON document.
 * <p/>
 * This should even be usable as a nested document codec by adding an instance of it to a PrimitiveCodecs instance.
 */
public class BSONDocumentBufferCodec implements CollectibleCodec<BSONDocumentBuffer> {
    private final BufferProvider bufferProvider;
    private final PrimitiveCodecs primitiveCodecs;

    public BSONDocumentBufferCodec(final BufferProvider bufferProvider,
                                   final PrimitiveCodecs primitiveCodecs) {
        this.bufferProvider = bufferProvider;
        this.primitiveCodecs = primitiveCodecs;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final BSONDocumentBuffer value) {
        BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(value.getByteBuffer()), true);
        try {
            bsonWriter.pipe(reader);
        } finally {
            reader.close();
        }
    }

    @Override
    public BSONDocumentBuffer decode(final BSONReader reader) {
        BSONBinaryWriter writer = new BSONBinaryWriter(new ByteBufferOutputBuffer(bufferProvider), true);
        try {
            writer.pipe(reader);
            BufferExposingByteArrayOutputStream byteArrayOutputStream = new BufferExposingByteArrayOutputStream(writer.getBuffer().size());
            writer.getBuffer().pipe(byteArrayOutputStream);
            return new BSONDocumentBuffer(byteArrayOutputStream.getInternalBytes());
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("impossible", e);
        } finally {
            writer.close();
        }
    }

    @Override
    public Class<BSONDocumentBuffer> getEncoderClass() {
        return BSONDocumentBuffer.class;
    }

    @Override
    public Object getId(final BSONDocumentBuffer document) {
        BSONReader reader = new BSONBinaryReader(new BasicInputBuffer(document.getByteBuffer()), true);
        try {
            reader.readStartDocument();
            while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
                String name = reader.readName();
                if (name.equals("_id")) {
                    return primitiveCodecs.decode(reader);
                } else {
                    reader.skipValue();
                }
            }
            return null;
        } finally {
            reader.close();
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

