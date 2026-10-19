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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static java.lang.String.format;

/**
 * Utility class for reading values from an input stream.
 */
class Bits {

    private static final int MIN_BSON_DOCUMENT_SIZE = 5;
    private static final int INITIAL_BUFFER_SIZE = 4096;

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer. The equivalent of calling
     * {@link #readFully(java.io.InputStream, byte[], int, int)} with an offset of zero and a length equal to the length of the buffer.
     *
     * @param inputStream the input stream to read from
     * @param buffer      the buffer into which the data is read.
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    static void readFully(final InputStream inputStream, final byte[] buffer)
    throws IOException {
        readFully(inputStream, buffer, 0, buffer.length);
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
    static void readFully(final InputStream inputStream, final byte[] buffer, final int offset, final int length)
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
     * Reads a BSON document of {@code size} bytes from the input stream, where the first {@code prefix.length} bytes
     * (the size prefix) have already been read into {@code prefix}. The destination buffer grows only as bytes actually
     * arrive, so a bogus declared size cannot force a large up-front allocation.
     *
     * <p>A truncated stream is reported as an {@link EOFException}, consistent with
     * {@link #readFully(InputStream, byte[], int, int)}. A {@code size} below the minimum BSON document size is malformed
     * content rather than an I/O failure, so it is reported as a {@link BsonSerializationException}.</p>
     *
     * @param inputStream the input stream to read from
     * @param prefix      the already-read leading bytes (typically the 4-byte size), copied to the start of the result
     * @param size        the total number of bytes in the document, including {@code prefix}
     * @return a byte array of length {@code size} containing the full document
     * @throws EOFException                if the stream ends before {@code size} bytes are available
     * @throws IOException                 if there's an error reading from the {@code inputStream}
     * @throws BsonSerializationException  if {@code size} is below the minimum BSON document size
     */
    static byte[] readFully(final InputStream inputStream, final byte[] prefix, final int size) throws IOException {
        if (size < MIN_BSON_DOCUMENT_SIZE) {
            throw new BsonSerializationException(format(
                    "While decoding a BSON document found a size of %d that is less than the minimum of %d bytes",
                    size, MIN_BSON_DOCUMENT_SIZE));
        }
        byte[] buffer = new byte[Math.min(size, INITIAL_BUFFER_SIZE)];
        System.arraycopy(prefix, 0, buffer, 0, prefix.length);
        int position = prefix.length;
        while (position < size) {
            if (position == buffer.length) {
                int newCapacity = (int) Math.min((long) size, (long) buffer.length * 2);
                buffer = Arrays.copyOf(buffer, newCapacity);
            }
            int bytesRead = inputStream.read(buffer, position, buffer.length - position);
            if (bytesRead < 0) {
                throw new EOFException(format(
                        "While decoding a BSON document the declared size of %d bytes exceeds the %d bytes that were "
                                + "available in the stream", size, position));
            }
            position += bytesRead;
        }
        return buffer;
    }

    /**
     * Reads and returns a single integer value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @param buffer the buffer to write the input stream bytes into
     * @return the integer value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    static int readInt(final InputStream inputStream, final byte[] buffer) throws IOException {
        readFully(inputStream, buffer, 0, 4);
        return readInt(buffer);
    }

    /**
     * Reads and returns a single integer value from the buffer. The equivalent of calling {@link #readInt(byte[], int)}
     * with an offset of zero.
     *
     * @param buffer the buffer to read from
     * @return the integer value
     */
    static int readInt(final byte[] buffer) {
        return readInt(buffer, 0);
    }

    /**
     * Reads and returns a single integer value from the buffer.
     *
     * @param buffer the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the integer value
     */
    static int readInt(final byte[] buffer, final int offset) {
        int x = 0;
        x |= (0xFF & buffer[offset]) << 0;
        x |= (0xFF & buffer[offset + 1]) << 8;
        x |= (0xFF & buffer[offset + 2]) << 16;
        x |= (0xFF & buffer[offset + 3]) << 24;
        return x;
    }

    /**
     * Reads and returns a single long value from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the long value
     * @throws IOException if there's an error reading from the {@code inputStream}
     */
    static long readLong(final InputStream inputStream) throws IOException {
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
    static long readLong(final InputStream inputStream, final byte[] buffer) throws IOException {
        readFully(inputStream, buffer, 0, 8);
        return readLong(buffer);
    }

    /**
     * Reads and returns a single long value from the buffer. The equivalent of called {@link #readLong(byte[], int)} with an offset of
     * zero.
     *
     * @param buffer the buffer to read from
     * @return the long value
     */
    static long readLong(final byte[] buffer) {
        return readLong(buffer, 0);
    }

    /**
     * Reads and returns a single long value from the buffer.
     *
     * @param buffer the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the long value
     */
    static long readLong(final byte[] buffer, final int offset) {
        long x = 0;
        x |= (0xFFL & buffer[offset]) << 0;
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
