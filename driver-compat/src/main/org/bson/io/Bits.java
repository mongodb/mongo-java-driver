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

package org.bson.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class Bits {
    public static void readFully(final InputStream in, final byte[] b)
            throws IOException {
        readFully(in, b, b.length);
    }

    public static void readFully(final InputStream in, final byte[] b, final int length)
            throws IOException {
        readFully(in, b, 0, length);
    }

    public static void readFully(final InputStream in, final byte[] b, final int startOffset, final int length)
            throws IOException {

        if (b.length - startOffset > length) {
            throw new IllegalArgumentException("Buffer is too small");
        }

        int offset = startOffset;
        int toRead = length;
        while (toRead > 0) {
            int bytesRead = in.read(b, offset, toRead);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            toRead -= bytesRead;
            offset += bytesRead;
        }
    }

    public static int readInt(final InputStream in)
            throws IOException {
        return readInt(in, new byte[4]);
    }

    public static int readInt(final InputStream in, byte[] data)
            throws IOException {
        readFully(in, data, 4);
        return readInt(data);
    }

    public static int readInt(final byte[] data) {
        return readInt(data, 0);
    }

    public static int readInt(final byte[] data, final int offset) {
        int x = 0;
        x |= (0xFF & data[offset + 0]) << 0;
        x |= (0xFF & data[offset + 1]) << 8;
        x |= (0xFF & data[offset + 2]) << 16;
        x |= (0xFF & data[offset + 3]) << 24;
        return x;
    }

    public static int readIntBE(final byte[] data, final int offset) {
        int x = 0;
        x |= (0xFF & data[offset + 0]) << 24;
        x |= (0xFF & data[offset + 1]) << 16;
        x |= (0xFF & data[offset + 2]) << 8;
        x |= (0xFF & data[offset + 3]) << 0;
        return x;
    }

    public static long readLong(final InputStream in)
            throws IOException {
        return readLong(in, new byte[8]);
    }


    public static long readLong(final InputStream in, final byte[] data)
            throws IOException {
        readFully(in, data, 8);
        return readLong(data);
    }

    public static long readLong(final byte[] data) {
        return readLong(data, 0);
    }

    public static long readLong(final byte[] data, final int offset) {
        long x = 0;
        x |= (0xFFL & data[offset + 0]) << 0;
        x |= (0xFFL & data[offset + 1]) << 8;
        x |= (0xFFL & data[offset + 2]) << 16;
        x |= (0xFFL & data[offset + 3]) << 24;
        x |= (0xFFL & data[offset + 4]) << 32;
        x |= (0xFFL & data[offset + 5]) << 40;
        x |= (0xFFL & data[offset + 6]) << 48;
        x |= (0xFFL & data[offset + 7]) << 56;
        return x;
    }
}
