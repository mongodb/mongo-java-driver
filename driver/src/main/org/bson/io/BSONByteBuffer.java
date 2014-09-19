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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

/**
 * Pseudo byte buffer, delegates as it is too hard to properly override / extend the ByteBuffer API
 */
public class BSONByteBuffer {

    protected final ByteBuffer buf;

    private BSONByteBuffer(final ByteBuffer buf) {
        this.buf = buf;
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Factory method for creating new instances.
     *
     * @param bytes  The array that will back the new buffer
     * @param offset The offset of the subarray to be used; must be non-negative and no larger than {@code array.length}.
     * @param length The length of the subarray to be used; must be non-negative and no larger than {@code array.length - offset}.
     * @return The new byte buffer
     * @see java.nio.ByteBuffer#wrap(byte[], int, int)
     */
    public static BSONByteBuffer wrap(final byte[] bytes, final int offset, final int length) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes, offset, length));
    }

    /**
     * Factory method for creating new instances.
     *
     * @param bytes The array that will back the new buffer
     * @return The new byte buffer
     * @see java.nio.ByteBuffer#wrap(byte[])
     */
    public static BSONByteBuffer wrap(final byte[] bytes) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes));
    }

    /**
     * Reads the byte at the given index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @see java.nio.ByteBuffer#get(int)
     */
    public byte get(final int index) {
        return buf.get(index);
    }

    /**
     * This method transfers bytes from this buffer into the given destination array.
     *
     * @param bytes  The array into which bytes are to be written
     * @param offset The offset within the array of the first byte to be written;
     * @param length The maximum number of bytes to be written to the given array;
     * @return This buffer
     * @see java.nio.ByteBuffer#get(byte[], int, int)
     */
    public ByteBuffer get(final byte[] bytes, final int offset, final int length) {
        return buf.get(bytes, offset, length);
    }

    /**
     * This method transfers bytes from this buffer into the given destination array.
     *
     * @param bytes The array into which bytes are to be written
     * @return This buffer
     * @see java.nio.ByteBuffer#get(byte[])
     */
    public ByteBuffer get(final byte[] bytes) {
        return buf.get(bytes);
    }

    /**
     * Returns the byte array that backs this buffer. Modifications to this buffer's content will cause the returned array's content to be
     * modified, and vice versa.
     *
     * @return The array that backs this buffer
     * @see java.nio.ByteBuffer#array()
     */
    public byte[] array() {
        return buf.array();
    }

    @Override
    public String toString() {
        return buf.toString();
    }

    @Override
    public int hashCode() {
        return buf.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BSONByteBuffer that = (BSONByteBuffer) o;

        if (buf != null ? !buf.equals(that.buf) : that.buf != null) {
            return false;
        }

        return true;
    }

    /**
     * Gets a Little Endian Integer
     *
     * @param i Index to read from
     * @return the integer value
     */
    public int getInt(final int i) {
        return getIntLE(i);
    }

    /**
     * Gets a Little Endian Integer
     *
     * @param i index to read from
     * @return the integer value
     */
    public int getIntLE(final int i) {
        int x = 0;
        x |= (0xFF & buf.get(i + 0)) << 0;
        x |= (0xFF & buf.get(i + 1)) << 8;
        x |= (0xFF & buf.get(i + 2)) << 16;
        x |= (0xFF & buf.get(i + 3)) << 24;
        return x;
    }

    /**
     * Gets a Big Endian Integer
     *
     * @param i index to read from
     * @return the integer value
     */
    public int getIntBE(final int i) {
        int x = 0;
        x |= (0xFF & buf.get(i + 0)) << 24;
        x |= (0xFF & buf.get(i + 1)) << 16;
        x |= (0xFF & buf.get(i + 2)) << 8;
        x |= (0xFF & buf.get(i + 3)) << 0;
        return x;
    }

    /**
     * Gets a Long
     *
     * @param i index to read from
     * @return the long value
     */
    public long getLong(final int i) {
        return buf.getLong(i);
    }

    /**
     * Gets a String value from the buffer
     *
     * @param offset the position to read from
     * @return the String value
     */
    public String getCString(final int offset) {
        int end = offset;
        while (get(end) != 0) {
            ++end;
        }
        int len = end - offset;
        return new String(array(), offset, len, Charset.forName("UTF-8"));
    }

    /**
     * Gets a UTF8 String value from the buffer
     *
     * @param offset the position to read from
     * @return the String value
     */
    public String getUTF8String(final int offset) {
        int size = getInt(offset) - 1;
        return new String(array(), offset + 4, size, Charset.forName("UTF-8"));
    }

    /**
     * Sets this buffer's position.  If the mark is defined and larger than the new position then it is discarded.
     *
     * @param newPosition The new position value; must be non-negative and no larger than the current limit
     * @return This buffer
     * @see java.nio.ByteBuffer#position(int)
     */
    public Buffer position(final int newPosition) {
        return buf.position(newPosition);
    }

    /**
     * Resets the buffer.
     *
     * @return the underlying {@code ByteBuffer}
     */
    public Buffer reset() {
        return buf.reset();
    }

    /**
     * Return the size of the BSON message.
     *
     * @return the size of the mesage
     */
    public int size() {
        return getInt(0);
    }
}
