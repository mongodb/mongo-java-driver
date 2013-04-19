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
 */

package org.mongodb.codecs;

import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.Binary;
import org.mongodb.Codec;

import java.util.UUID;

public class UUIDCodec implements Codec<UUID> {
    @Override
    public UUID decode(final BSONReader reader) {
        final Binary binary = reader.readBinaryData();
        final byte[] binaryData = binary.getData();
        return new UUID(readLongFromArrayLittleEndian(binaryData, 0), readLongFromArrayLittleEndian(binaryData, 8));
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final UUID value) {
        final byte[] bytes = new byte[16];

        writeLongToArrayLittleEndian(bytes, 0, value.getMostSignificantBits());
        writeLongToArrayLittleEndian(bytes, 8, value.getLeastSignificantBits());

        bsonWriter.writeBinaryData(new Binary(BSONBinarySubType.UuidLegacy, bytes));
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

    // Stuff from 2.x driver
    //    _put( BINARY , name );
    //    _buf.writeInt( 16 );
    //    _buf.write( B_UUID );
    //    _buf.writeLong( val.getMostSignificantBits());
    //    _buf.writeLong( val.getLeastSignificantBits());

    //    write( (byte)(0xFFL & ( x >> 0 ) ) );
    //    write( (byte)(0xFFL & ( x >> 8 ) ) );
    //    write( (byte)(0xFFL & ( x >> 16 ) ) );
    //    write( (byte)(0xFFL & ( x >> 24 ) ) );
    //    write( (byte)(0xFFL & ( x >> 32 ) ) );
    //    write( (byte)(0xFFL & ( x >> 40 ) ) );
    //    write( (byte)(0xFFL & ( x >> 48 ) ) );
    //    write( (byte)(0xFFL & ( x >> 56 ) ) );

}
