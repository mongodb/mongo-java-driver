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
 *
 * @author brendan
 */
public class BSONByteBuffer {

    protected final ByteBuffer buf;

    private BSONByteBuffer(final ByteBuffer buf) {
        this.buf = buf;
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    public static BSONByteBuffer wrap(final byte[] bytes, final int offset, final int length) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes, offset, length));
    }

    public static BSONByteBuffer wrap(final byte[] bytes) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes));
    }

    public byte get(final int i) {
        return buf.get(i);
    }

    public ByteBuffer get(final byte[] bytes, final int offset, final int length) {
        return buf.get(bytes, offset, length);
    }

    public ByteBuffer get(final byte[] bytes) {
        return buf.get(bytes);
    }

    public byte[] array() {
        return buf.array();
    }

    public String toString() {
        return buf.toString();
    }

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

    public String getCString(final int offset) {
        int end = offset;
        while (get(end) != 0) {
            ++end;
        }
        int len = end - offset;
        return new String(array(), offset, len, Charset.forName("UTF-8"));
    }

    public String getUTF8String(final int valueOffset) {
        int size = getInt(valueOffset) - 1;
        return new String(array(), valueOffset + 4, size, Charset.forName("UTF-8"));
    }

    public Buffer position(final int i) {
        return buf.position(i);
    }

    public Buffer reset() {
        return buf.reset();
    }

    public int size() {
        return getInt(0);
    }
}
