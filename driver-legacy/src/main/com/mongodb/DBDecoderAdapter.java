/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ByteBufferBsonOutput;
import org.bson.BsonBinaryWriter;
import org.bson.BsonReader;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

class DBDecoderAdapter implements Decoder<DBObject> {
    private final DBDecoder decoder;
    private final DBCollection collection;
    private final BufferProvider bufferProvider;

    DBDecoderAdapter(final DBDecoder decoder, final DBCollection collection, final BufferProvider bufferProvider) {
        this.decoder = decoder;
        this.collection = collection;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public DBObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(bufferProvider);
        BsonBinaryWriter binaryWriter = new BsonBinaryWriter(bsonOutput);
        try {
            binaryWriter.pipe(reader);
            BufferExposingByteArrayOutputStream byteArrayOutputStream =
                new BufferExposingByteArrayOutputStream(binaryWriter.getBsonOutput().getSize());
            bsonOutput.pipe(byteArrayOutputStream);
            return decoder.decode(byteArrayOutputStream.getInternalBytes(), collection);
        } catch (IOException e) {
            // impossible with a byte array output stream
            throw new MongoInternalException("An unlikely IOException thrown.", e);
        } finally {
            binaryWriter.close();
            bsonOutput.close();
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
