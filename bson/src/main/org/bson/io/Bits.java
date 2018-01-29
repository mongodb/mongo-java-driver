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

package org.bson.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class for reading values from an input stream.
 */
public class Bits {

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer. The equivalent of calling
     * {@link #readFully(java.io.InputStream, byte[], int, int)} with an offset of zero and a length equal to the length of the buffer.
     *
     * @param inputStream the input stream to read from
     * @param buffer      the buffer into which the data is read.
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static void readFully(final InputStream inputStream, final byte[] buffer)
    throws IOException {
        readFully(inputStream, buffer, buffer.length);
    }

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer. The equivalent of calling
     * {@link #readFully(java.io.InputStream, byte[], int, int)} with an offset of zero.
     *
     * @param inputStream the input stream to read from
     * @param buffer      the buffer into which the data is read.
     * @param length      the maximum number of bytes to read.
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static void readFully(final InputStream inputStream, final byte[] buffer, final int length)
    throws IOException {
        readFully(inputStream, buffer, 0, length);
    }

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer.
     *
     * @param inputStream the input stream to read from
     * @param buffer      the buffer into which the data is read.
     * @param offset      the start offset in array {@code buffer} at which the data is written.
     * @param length      the maximum number of bytes to read.
     * @throws IOException if there's an error reading from the {@code inputStream}
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public static void readFully(final InputStream inputStream, final byte[] buffer, final int offset, final int length)
    throws IOException {
        if (buffer.length < length + offset) {
            throw new IllegalArgumentException("Buffer is too small");
        }

        int arrayOffset = offset;
        int bytesToRead = length;
        while (bytesToRead > 0) {
            int bytesRead = inputStream.read(buffer, arrayOffset, bytesToRead);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            bytesToRead -= bytesRead;
            arrayOffset += bytesRead;
        }
    }

    /**
     * Reads and returns a single integer value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the integer value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static int readInt(final InputStream inputStream) throws IOException {
        return readInt(inputStream, new byte[4]);
    }

    /**
     * Reads and returns a single integer value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @param buffer the buffer to write the input stream bytes into
     * @return the integer value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static int readInt(final InputStream inputStream, final byte[] buffer) throws IOException {
        readFully(inputStream, buffer, 4);
        return readInt(buffer);
    }

    /**
     * Reads and returns a single integer value from the buffer. The equivalent of calling {@link #readInt(byte[], int)}
     * with an offset of zero.
     *
     * @param buffer the buffer to read from
     * @return the integer value
     */
    public static int readInt(final byte[] buffer) {
        return readInt(buffer, 0);
    }

    /**
     * Reads and returns a single integer value from the buffer.
     *
     * @param buffer the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the integer value
     */
    public static int readInt(final byte[] buffer, final int offset) {
        int x = 0;
        x |= (0xFF & buffer[offset + 0]) << 0;
        x |= (0xFF & buffer[offset + 1]) << 8;
        x |= (0xFF & buffer[offset + 2]) << 16;
        x |= (0xFF & buffer[offset + 3]) << 24;
        return x;
    }

    /**
     * Reads and returns a single big-endian integer value
     *
     * @param buffer the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the integer value
     */
    public static int readIntBE(final byte[] buffer, final int offset) {
        int x = 0;
        x |= (0xFF & buffer[offset + 0]) << 24;
        x |= (0xFF & buffer[offset + 1]) << 16;
        x |= (0xFF & buffer[offset + 2]) << 8;
        x |= (0xFF & buffer[offset + 3]) << 0;
        return x;
    }

    /**
     * Reads and returns a single long value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the long value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static long readLong(final InputStream inputStream) throws IOException {
        return readLong(inputStream, new byte[8]);
    }

    /**
     * Reads and returns a single long value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @param buffer the buffer to write the input stream bytes into
     * @return the long value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    public static long readLong(final InputStream inputStream, final byte[] buffer) throws IOException {
        readFully(inputStream, buffer, 8);
        return readLong(buffer);
    }

    /**
     * Reads and returns a single long value from the buffer. The equivalent of called {@link #readLong(byte[], int)} with an offset of
     * zero.
     *
     * @param buffer the buffer to read from
     * @return the long value
     */
    public static long readLong(final byte[] buffer) {
        return readLong(buffer, 0);
    }

    /**
     * Reads and returns a single long value from the buffer.
     *
     * @param buffer the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the long value
     */
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
