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
import org.bson.BsonType;
import org.bson.io.ByteBufferInput;
import org.bson.util.BufferPool;
import org.mongodb.BsonDocumentBuffer;
import org.mongodb.MongoInternalException;
import org.mongodb.io.PooledByteBufferOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A simple BsonDocumentBuffer serializer.  It does not attempt to validate the contents of the underlying ByteBuffer.
 * It assumes that it contains a single, serialized BSON document.
 * <p>
 * This should even be usable as a nested document serializer by adding an instance of it to a PrimitiveSerializers instance.
 */
public class BsonDocumentBufferSerializer implements CollectibleSerializer<BsonDocumentBuffer> {
    private final BufferPool<ByteBuffer> bufferPool;
    private final PrimitiveSerializers primitiveSerializers;

    public BsonDocumentBufferSerializer(final BufferPool<ByteBuffer> bufferPool,
                                        final PrimitiveSerializers primitiveSerializers) {
        this.bufferPool = bufferPool;
        this.primitiveSerializers = primitiveSerializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final BsonDocumentBuffer value) {
        bsonWriter.pipe(new BSONBinaryReader(new ByteBufferInput(value.getByteBuffer())));
    }

    @Override
    public BsonDocumentBuffer deserialize(final BSONReader reader) {
        try {
            BSONBinaryWriter binaryWriter = new BSONBinaryWriter(new PooledByteBufferOutput(bufferPool));
            binaryWriter.pipe(reader);
            final BufferExposingByteArrayOutputStream byteArrayOutputStream =
                    new BufferExposingByteArrayOutputStream(binaryWriter.getBuffer().size());
            binaryWriter.getBuffer().pipe(byteArrayOutputStream);
            return new BsonDocumentBuffer(byteArrayOutputStream.getInternalBytes());
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("impossible", e);

        }
    }

    @Override
    public Class<BsonDocumentBuffer> getSerializationClass() {
        return BsonDocumentBuffer.class;
    }

    @Override
    public Object getId(final BsonDocumentBuffer document) {
        BSONReader reader = new BSONBinaryReader(new ByteBufferInput(document.getByteBuffer()));
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (name.equals("_id")) {
                return primitiveSerializers.deserialize(reader);  // TODO: handle non-primitive identifiers
            }
            else {
                reader.skipValue();
            }
        }
        return null;
    }

    // Just so we don't have to copy the buffer
    private     static class BufferExposingByteArrayOutputStream extends ByteArrayOutputStream {
        BufferExposingByteArrayOutputStream(final int size) {
            super(size);
        }

        byte[] getInternalBytes() {
            return buf;
        }
    }
}

