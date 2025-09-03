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

package org.bson;

import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.bson.assertions.Assertions.notNull;

/**
 * Basic implementation of BSONDecoder interface that creates BasicBSONObject instances
 */
public class BasicBSONDecoder implements BSONDecoder {

    /**
     * Sets the global (JVM-wide) {@link UuidRepresentation} to use when decoding BSON binary values with subtypes of either
     * {@link BsonBinarySubType#UUID_STANDARD} or {@link BsonBinarySubType#UUID_LEGACY}.
     *
     * <p>
     * If the {@link BsonBinarySubType} of the value to be decoded matches the binary subtype of the {@link UuidRepresentation},
     * then the value will be decoded to an instance of {@link java.util.UUID}, according to the semantics of the
     * {@link UuidRepresentation}.  Otherwise, it will be decoded to an instance of {@link org.bson.types.Binary}.
     * </p>
     *
     * <p>
     * Defaults to {@link UuidRepresentation#JAVA_LEGACY}. If set to {@link UuidRepresentation#UNSPECIFIED}, attempting to decode any
     * UUID will throw a {@link BSONException}.
     * </p>
     *
     * @param uuidRepresentation the uuid representation, which may not be null
     * @see BSONCallback#gotUUID(String, long, long)
     * @see BasicBSONEncoder#setDefaultUuidRepresentation(UuidRepresentation)
     * @since 4.7
     */
    public static void setDefaultUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        defaultUuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
    }

    /**
     * Gets the default {@link UuidRepresentation} to use when decoding BSON binary values.
     *
     * <p>
     * If unset, the default is {@link UuidRepresentation#JAVA_LEGACY}.
     * </p>
     *
     * @return the uuid representation, which may not be null
     * @see BSONCallback#gotUUID(String, long, long)
     * @see BasicBSONEncoder#setDefaultUuidRepresentation(UuidRepresentation)
     * @since 4.7
     */
    public static UuidRepresentation getDefaultUuidRepresentation() {
        return defaultUuidRepresentation;
    }

    private static volatile UuidRepresentation defaultUuidRepresentation = UuidRepresentation.JAVA_LEGACY;

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
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))))) {
            BsonWriter writer = new BSONCallbackAdapter(new BsonWriterSettings(), callback);
            writer.pipe(reader);
            return reader.getBsonInput().getPosition(); //TODO check this.
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
