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

import org.bson.BSONBinarySubType;
import org.bson.BSONWriter;
import org.bson.types.Binary;
import org.bson.codecs.Encoder;

import java.util.UUID;

public class UUIDEncoder implements Encoder<UUID> {
    @Override
    public void encode(final BSONWriter bsonWriter, final UUID value) {
        byte[] bytes = new byte[16];

        writeLongToArrayLittleEndian(bytes, 0, value.getMostSignificantBits());
        writeLongToArrayLittleEndian(bytes, 8, value.getLeastSignificantBits());

        bsonWriter.writeBinaryData(new Binary(BSONBinarySubType.UUID_LEGACY, bytes));
    }

    @Override
    public Class<UUID> getEncoderClass() {
        return UUID.class;
    }

    private static void writeLongToArrayLittleEndian(final byte[] bytes, final int offset, final long x) {
        bytes[offset] = (byte) (0xFFL & (x));
        bytes[offset + 1] = (byte) (0xFFL & (x >> 8));
        bytes[offset + 2] = (byte) (0xFFL & (x >> 16));
        bytes[offset + 3] = (byte) (0xFFL & (x >> 24));
        bytes[offset + 4] = (byte) (0xFFL & (x >> 32));
        bytes[offset + 5] = (byte) (0xFFL & (x >> 40));
        bytes[offset + 6] = (byte) (0xFFL & (x >> 48));
        bytes[offset + 7] = (byte) (0xFFL & (x >> 56));
    }
}
