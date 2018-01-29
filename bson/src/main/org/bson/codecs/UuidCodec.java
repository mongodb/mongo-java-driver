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

package org.bson.codecs;

import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonSerializationException;
import org.bson.BsonWriter;
import org.bson.UuidRepresentation;

import java.util.UUID;

import static org.bson.codecs.UuidCodecHelper.reverseByteArray;

/**
 * Encodes and decodes {@code UUID} objects.
 *
 * @since 3.0
 */
public class UuidCodec implements Codec<UUID> {

    private final UuidRepresentation encoderUuidRepresentation;
    private final UuidRepresentation decoderUuidRepresentation;

    /**
     * The default UUIDRepresentation is JAVA_LEGACY to be compatible with existing documents
     *
     * @param uuidRepresentation the representation of UUID
     * @see org.bson.UuidRepresentation
     */
    public UuidCodec(final UuidRepresentation uuidRepresentation) {
        this.encoderUuidRepresentation = uuidRepresentation;
        this.decoderUuidRepresentation = uuidRepresentation;
    }

    /**
     * The constructor for UUIDCodec, default is JAVA_LEGACY
     */
    public UuidCodec() {
        this.encoderUuidRepresentation = UuidRepresentation.JAVA_LEGACY;
        this.decoderUuidRepresentation = UuidRepresentation.JAVA_LEGACY;
    }

    @Override
    public void encode(final BsonWriter writer, final UUID value, final EncoderContext encoderContext) {
        byte[] binaryData = new byte[16];
        writeLongToArrayBigEndian(binaryData, 0, value.getMostSignificantBits());
        writeLongToArrayBigEndian(binaryData, 8, value.getLeastSignificantBits());
        switch (encoderUuidRepresentation) {
            case C_SHARP_LEGACY:
                UuidCodecHelper.reverseByteArray(binaryData, 0, 4);
                UuidCodecHelper.reverseByteArray(binaryData, 4, 2);
                UuidCodecHelper.reverseByteArray(binaryData, 6, 2);
                break;
            case JAVA_LEGACY:
                UuidCodecHelper.reverseByteArray(binaryData, 0, 8);
                UuidCodecHelper.reverseByteArray(binaryData, 8, 8);
                break;
            case PYTHON_LEGACY:
            case STANDARD:
                break;
            default:
                throw new BSONException("Unexpected UUID representation");
        }
        // changed the default subtype to STANDARD since 3.0
        if (encoderUuidRepresentation == UuidRepresentation.STANDARD) {
            writer.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_STANDARD, binaryData));
        } else {
            writer.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_LEGACY, binaryData));
        }
    }

    @Override
    public UUID decode(final BsonReader reader, final DecoderContext decoderContext) {
        byte subType = reader.peekBinarySubType();

        if (subType != BsonBinarySubType.UUID_LEGACY.getValue() && subType != BsonBinarySubType.UUID_STANDARD.getValue()) {
            throw new BSONException("Unexpected BsonBinarySubType");
        }

        byte[] bytes = reader.readBinaryData().getData();

        if (bytes.length != 16) {
            throw new BsonSerializationException(String.format("Expected length to be 16, not %d.", bytes.length));
        }

        if (subType == BsonBinarySubType.UUID_LEGACY.getValue()) {
            switch (decoderUuidRepresentation) {
                case C_SHARP_LEGACY:
                    reverseByteArray(bytes, 0, 4);
                    reverseByteArray(bytes, 4, 2);
                    reverseByteArray(bytes, 6, 2);
                    break;
                case JAVA_LEGACY:
                    reverseByteArray(bytes, 0, 8);
                    reverseByteArray(bytes, 8, 8);
                    break;
                case PYTHON_LEGACY:
                case STANDARD:
                    break;
                default:
                    throw new BSONException("Unexpected UUID representation");
            }
        }

        return new UUID(readLongFromArrayBigEndian(bytes, 0), readLongFromArrayBigEndian(bytes, 8));
    }

    @Override
    public Class<UUID> getEncoderClass() {
        return UUID.class;
    }

    private static void writeLongToArrayBigEndian(final byte[] bytes, final int offset, final long x) {
        bytes[offset + 7] = (byte) (0xFFL & (x));
        bytes[offset + 6] = (byte) (0xFFL & (x >> 8));
        bytes[offset + 5] = (byte) (0xFFL & (x >> 16));
        bytes[offset + 4] = (byte) (0xFFL & (x >> 24));
        bytes[offset + 3] = (byte) (0xFFL & (x >> 32));
        bytes[offset + 2] = (byte) (0xFFL & (x >> 40));
        bytes[offset + 1] = (byte) (0xFFL & (x >> 48));
        bytes[offset] = (byte) (0xFFL & (x >> 56));
    }

    private static long readLongFromArrayBigEndian(final byte[] bytes, final int offset) {
        long x = 0;
        x |= (0xFFL & bytes[offset + 7]);
        x |= (0xFFL & bytes[offset + 6]) << 8;
        x |= (0xFFL & bytes[offset + 5]) << 16;
        x |= (0xFFL & bytes[offset + 4]) << 24;
        x |= (0xFFL & bytes[offset + 3]) << 32;
        x |= (0xFFL & bytes[offset + 2]) << 40;
        x |= (0xFFL & bytes[offset + 1]) << 48;
        x |= (0xFFL & bytes[offset]) << 56;
        return x;
    }

}
