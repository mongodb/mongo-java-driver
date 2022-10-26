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

package org.bson.internal;

import org.bson.BSONException;
import org.bson.BsonBinarySubType;
import org.bson.BsonSerializationException;
import org.bson.UuidRepresentation;

import java.util.Arrays;
import java.util.UUID;

/**
 * Utilities for encoding and decoding UUID into binary.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class UuidHelper {
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

    // reverse elements in the subarray data[start:start+length]
    private static void reverseByteArray(final byte[] data, final int start, final int length) {
        for (int left = start, right = start + length - 1; left < right; left++, right--) {
            // swap the values at the left and right indices
            byte temp = data[left];
            data[left]  = data[right];
            data[right] = temp;
        }
    }

    public static byte[] encodeUuidToBinary(final UUID uuid, final UuidRepresentation uuidRepresentation) {
        byte[] binaryData = new byte[16];
        writeLongToArrayBigEndian(binaryData, 0, uuid.getMostSignificantBits());
        writeLongToArrayBigEndian(binaryData, 8, uuid.getLeastSignificantBits());
        switch(uuidRepresentation) {
            case C_SHARP_LEGACY:
                reverseByteArray(binaryData, 0, 4);
                reverseByteArray(binaryData, 4, 2);
                reverseByteArray(binaryData, 6, 2);
                break;
            case JAVA_LEGACY:
                reverseByteArray(binaryData, 0, 8);
                reverseByteArray(binaryData, 8, 8);
                break;
            case PYTHON_LEGACY:
            case STANDARD:
                break;
            default:
                throw new BSONException("Unexpected UUID representation: " + uuidRepresentation);
        }

        return binaryData;
    }

    // This method will NOT modify the contents of the byte array
    public static UUID decodeBinaryToUuid(final byte[] data, final byte type, final UuidRepresentation uuidRepresentation) {
        if (data.length != 16) {
            throw new BsonSerializationException(String.format("Expected length to be 16, not %d.", data.length));
        }

        byte[] localData = data;

        if (type == BsonBinarySubType.UUID_LEGACY.getValue()) {
            switch(uuidRepresentation) {
                case C_SHARP_LEGACY:
                    localData = Arrays.copyOf(data, 16);

                    reverseByteArray(localData, 0, 4);
                    reverseByteArray(localData, 4, 2);
                    reverseByteArray(localData, 6, 2);
                    break;
                case JAVA_LEGACY:
                    localData = Arrays.copyOf(data, 16);

                    reverseByteArray(localData, 0, 8);
                    reverseByteArray(localData, 8, 8);
                    break;
                case PYTHON_LEGACY:
                    break;
                case STANDARD:
                    throw new BSONException("Can not decode a subtype 3 (UUID legacy) BSON binary when the decoder is configured to use "
                            + "the standard UUID representation");
                default:
                    throw new BSONException("Unexpected UUID representation: " + uuidRepresentation);
            }
        }

        return new UUID(readLongFromArrayBigEndian(localData, 0), readLongFromArrayBigEndian(localData, 8));
    }

    private UuidHelper() {
    }
}
