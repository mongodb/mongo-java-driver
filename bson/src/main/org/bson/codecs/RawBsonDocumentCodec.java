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

package org.bson.codecs;

import org.bson.BSONException;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A simple BSONDocumentBuffer codec.  It does not attempt to validate the contents of the underlying ByteBuffer. It assumes that it
 * contains a single encoded BSON document.
 *
 * @since 3.0
 */
public class RawBsonDocumentCodec implements Codec<RawBsonDocument> {

    /**
     * Constructs a new instance.
     */
    public RawBsonDocumentCodec() {
    }

    @Override
    public void encode(final BsonWriter writer, final RawBsonDocument value, final EncoderContext encoderContext) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(value.getByteBuffer()));
        try {
            writer.pipe(reader);
        } finally {
            reader.close();
        }
    }

    @Override
    public RawBsonDocument decode(final BsonReader reader, final DecoderContext decoderContext) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        try {
            writer.pipe(reader);
            BufferExposingByteArrayOutputStream byteArrayOutputStream = new BufferExposingByteArrayOutputStream(writer.getBsonOutput()
                                                                                                                      .getSize());
            buffer.pipe(byteArrayOutputStream);
            return new RawBsonDocument(byteArrayOutputStream.getInternalBytes());
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new BSONException("impossible", e);
        } finally {
            writer.close();
        }
    }

    @Override
    public Class<RawBsonDocument> getEncoderClass() {
        return RawBsonDocument.class;
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

