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

package com.mongodb.codecs;

import org.bson.BsonBinary;
import org.mongodb.BinaryTransformer;

import java.util.UUID;

/**
 * A transformer from Binary to UUID.
 *
 * @since 3.0
 */
public class BinaryToUUIDTransformer implements BinaryTransformer<UUID> {
    @Override
    public UUID transform(final BsonBinary binary) {
        return new UUID(readLongFromArrayLittleEndian(binary.getData(), 0), readLongFromArrayLittleEndian(binary.getData(), 8));
    }

    private static long readLongFromArrayLittleEndian(final byte[] bytes, final int offset) {
        long x = 0;
        x |= (0xFFL & bytes[offset]);
        x |= (0xFFL & bytes[offset + 1]) << 8;
        x |= (0xFFL & bytes[offset + 2]) << 16;
        x |= (0xFFL & bytes[offset + 3]) << 24;
        x |= (0xFFL & bytes[offset + 4]) << 32;
        x |= (0xFFL & bytes[offset + 5]) << 40;
        x |= (0xFFL & bytes[offset + 6]) << 48;
        x |= (0xFFL & bytes[offset + 7]) << 56;
        return x;
    }
}