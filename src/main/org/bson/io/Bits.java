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

/**
 * Utility class for reading values from an input stream.
 */
public class Bits {

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer. The equivalent of calling {@link
     * #readFully(java.io.InputStream, byte[], int, int)} with an offset of zero and a length equal to the length of the buffer.
     *
     * @param in the input stream to read from
     * @param b  the buffer into which the data is read.
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static void readFully(InputStream in, byte[] b) throws IOException {
        readFully(in, b, b.length);
    }

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer. The equivalent of calling {@link
     * #readFully(java.io.InputStream, byte[], int, int)} with an offset of zero.
     *
     * @param in     the input stream to read from
     * @param b      the buffer into which the data is read.
     * @param length the maximum number of bytes to read.
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static void readFully(InputStream in, byte[] b, int length) throws IOException {
        readFully(in, b, 0, length);
    }

    /**
     * Reads bytes from the input stream and puts them into the given byte buffer.
     *
     * @param in          the input stream to read from
     * @param b           the buffer into which the data is read.
     * @param startOffset the start offset in array {@code b} at which the data is written.
     * @param length      the maximum number of bytes to read.
     * @throws IOException if there's an error reading from the {@code in}
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public static void readFully(InputStream in, byte[] b, int startOffset, int length)
    throws IOException {

        if (b.length < length + startOffset) {
            throw new IllegalArgumentException("Buffer is too small");
        }

        int offset = startOffset;
        int toRead = length;
        while ( toRead > 0 ){
            int bytesRead = in.read( b, offset , toRead );
            if ( bytesRead < 0 )
                throw new EOFException();
            toRead -= bytesRead;
            offset += bytesRead;
        }
    }

    /**
     * Reads and returns a single integer value from the input stream.
     *
     * @param in the input stream to read from
     * @return the integer value
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static int readInt(InputStream in) throws IOException {
        return readInt(in, new byte[4]);
    }

    /**
     * Reads and returns a single integer value from the input stream.
     *
     * @param in   the input stream to read from
     * @param data the buffer to write the input stream bytes into
     * @return the integer value
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static int readInt(InputStream in, byte[] data)
    throws IOException {
        readFully(in, data, 4);
        return readInt(data);
    }

    /**
     * Reads and returns a single integer value from the buffer. The equivalent of calling {@link #readInt(byte[], int)} with an offset of
     * zero.
     *
     * @param data the buffer to read from
     * @return the integer value
     */
    public static int readInt(byte[] data) {
        return readInt(data, 0);
    }

    /**
     * Reads and returns a single integer value from the buffer.
     *
     * @param data   the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the integer value
     */
    public static int readInt(byte[] data, int offset) {
        int x = 0;
        x |= ( 0xFF & data[offset+0] ) << 0;
        x |= ( 0xFF & data[offset+1] ) << 8;
        x |= ( 0xFF & data[offset+2] ) << 16;
        x |= ( 0xFF & data[offset+3] ) << 24;
        return x;
    }

    /**
     * Reads and returns a single big-endian integer value
     *
     * @param data   the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the integer value
     */
    public static int readIntBE(byte[] data, int offset) {
        int x = 0;
        x |= ( 0xFF & data[offset+0] ) << 24;
        x |= ( 0xFF & data[offset+1] ) << 16;
        x |= ( 0xFF & data[offset+2] ) << 8;
        x |= ( 0xFF & data[offset+3] ) << 0;
        return x;
    }

    /**
     * Reads and returns a single long value from the input stream.
     *
     * @param in the input stream to read from
     * @return the long value
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static long readLong(InputStream in) throws IOException {
        return readLong(in, new byte[8]);
    }

    /**
     * Reads and returns a single long value from the input stream.
     *
     * @param in   the input stream to read from
     * @param data the buffer to write the input stream bytes into
     * @return the long value
     * @throws IOException if there's an error reading from the {@code in}
     */
    public static long readLong(InputStream in, byte[] data) throws IOException {
        readFully(in, data, 8);
        return readLong(data);
    }

    /**
     * Reads and returns a single long value from the buffer. The equivalent of called {@link #readLong(byte[], int)} with an offset of
     * zero.
     *
     * @param data the buffer to read from
     * @return the long value
     */
    public static long readLong(byte[] data) {
        return readLong(data, 0);
    }

    /**
     * Reads and returns a single long value from the buffer.
     *
     * @param data   the buffer to read from
     * @param offset the position to start reading from the buffer
     * @return the long value
     */
    public static long readLong(byte[] data, int offset) {
        long x = 0;
        x |= ( 0xFFL & data[offset+0] ) << 0;
        x |= ( 0xFFL & data[offset+1] ) << 8;
        x |= ( 0xFFL & data[offset+2] ) << 16;
        x |= ( 0xFFL & data[offset+3] ) << 24;
        x |= ( 0xFFL & data[offset+4] ) << 32;
        x |= ( 0xFFL & data[offset+5] ) << 40;
        x |= ( 0xFFL & data[offset+6] ) << 48;
        x |= ( 0xFFL & data[offset+7] ) << 56;
        return x;
    }
}
