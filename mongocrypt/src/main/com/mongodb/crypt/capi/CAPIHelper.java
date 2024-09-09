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
 *
 */

package com.mongodb.crypt.capi;

import com.mongodb.crypt.capi.CAPI.mongocrypt_binary_t;
import com.sun.jna.Pointer;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;

import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_new_from_data;
import static java.lang.String.format;

final class CAPIHelper {

    private static final CodecRegistry CODEC_REGISTRY = CodecRegistries.fromProviders(new BsonValueCodecProvider());

    @SuppressWarnings("unchecked")
    static BinaryHolder toBinary(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        ((Codec<BsonDocument>) CODEC_REGISTRY.get(document.getClass())).encode(writer, document, EncoderContext.builder().build());

        DisposableMemory memory = new DisposableMemory(buffer.size());
        memory.write(0, buffer.getInternalBuffer(), 0, buffer.size());

        return new BinaryHolder(memory, mongocrypt_binary_new_from_data(memory, buffer.getSize()));
    }

    static RawBsonDocument toDocument(final mongocrypt_binary_t binary) {
        ByteBuffer byteBuffer = toByteBuffer(binary);
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new RawBsonDocument(bytes);
    }

    static BinaryHolder toBinary(final ByteBuffer buffer) {
        byte[] message = new byte[buffer.remaining()];
        buffer.get(message, 0, buffer.remaining());

        DisposableMemory memory = new DisposableMemory(message.length);
        memory.write(0, message, 0, message.length);

        return new BinaryHolder(memory, mongocrypt_binary_new_from_data(memory, message.length));
    }

    static ByteBuffer toByteBuffer(final mongocrypt_binary_t binary) {
        Pointer pointer = binary.data();
        int length = binary.len();
        return pointer.getByteBuffer(0, length);
    }

    static byte[] toByteArray(final mongocrypt_binary_t binary) {
        ByteBuffer byteBuffer = toByteBuffer(binary);
        byte[] byteArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(byteArray);
        return byteArray;
    }

    static void writeByteArrayToBinary(final mongocrypt_binary_t binary, final byte[] bytes) {
        if (binary.len() < bytes.length) {
            throw new IllegalArgumentException(format("mongocrypt binary of length %d is not large enough to hold %d bytes",
                    binary.len(), bytes.length));
        }
        Pointer outPointer = binary.data();
        outPointer.write(0, bytes, 0, bytes.length);
    }

    private CAPIHelper() {
    }
}
