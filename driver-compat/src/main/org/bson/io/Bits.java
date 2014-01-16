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

package org.bson.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class Bits {

    public static void readFully(final InputStream in, final byte[] buffer)
        throws IOException {
        readFully(in, buffer, buffer.length);
    }

    public static void readFully(final InputStream in, final byte[] buffer, final int length)
        throws IOException {
        readFully(in, buffer, 0, length);
    }

    public static void readFully(final InputStream in, final byte[] buffer, final int offset, final int length)
        throws IOException {
        if (buffer.length < length + offset) {
            throw new IllegalArgumentException("Buffer is too small");
        }

        int arrayOffset = offset;
        int bytesToRead = length;
        while (bytesToRead > 0) {
            int bytesRead = in.read(buffer, arrayOffset, bytesToRead);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            bytesToRead -= bytesRead;
            arrayOffset += bytesRead;
        }
    }

    public static int readInt(final InputStream in) throws IOException {
        return readInt(in, new byte[4]);
    }

    public static int readInt(final InputStream in, final byte[] buffer) throws IOException {
        readFully(in, buffer, 4);
        return readInt(buffer);
    }

    public static int readInt(final byte[] buffer) {
        return readInt(buffer, 0);
    }

    public static int readInt(final byte[] buffer, final int offset) {
        int x = 0;
        x |= (0xFF & buffer[offset + 0]) << 0;
        x |= (0xFF & buffer[offset + 1]) << 8;
        x |= (0xFF & buffer[offset + 2]) << 16;
        x |= (0xFF & buffer[offset + 3]) << 24;
        return x;
    }

    public static int readIntBE(final byte[] buffer, final int offset) {
        int x = 0;
        x |= (0xFF & buffer[offset + 0]) << 24;
        x |= (0xFF & buffer[offset + 1]) << 16;
        x |= (0xFF & buffer[offset + 2]) << 8;
        x |= (0xFF & buffer[offset + 3]) << 0;
        return x;
    }

    public static long readLong(final InputStream in) throws IOException {
        return readLong(in, new byte[8]);
    }


    public static long readLong(final InputStream in, final byte[] buffer) throws IOException {
        readFully(in, buffer, 8);
        return readLong(buffer);
    }

    public static long readLong(final byte[] bytes) {
        return readLong(bytes, 0);
    }

    public static long readLong(final byte[] buffer, final int offset) {
        long x = 0;
        x |= (0xFFL & buffer[offset + 0]) << 0;
        x |= (0xFFL & buffer[offset + 1]) << 8;
        x |= (0xFFL & buffer[offset + 2]) << 16;
        x |= (0xFFL & buffer[offset + 3]) << 24;
        x |= (0xFFL & buffer[offset + 4]) << 32;
        x |= (0xFFL & buffer[offset + 5]) << 40;
        x |= (0xFFL & buffer[offset + 6]) << 48;
        x |= (0xFFL & buffer[offset + 7]) << 56;
        return x;
    }
}
