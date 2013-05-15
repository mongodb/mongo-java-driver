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

import org.bson.BSONException;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Pseudo byte buffer, delegates as it is too hard to properly override / extend the ByteBuffer API
 *
 * @author brendan
 */
public class BSONByteBuffer {

    private final ByteBuffer buffer;


    private BSONByteBuffer(final ByteBuffer buffer) {
        this.buffer = buffer;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public static BSONByteBuffer wrap(final byte[] bytes, final int offset, final int length) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes, offset, length));
    }

    public static BSONByteBuffer wrap(final byte[] bytes) {
        return new BSONByteBuffer(ByteBuffer.wrap(bytes));
    }

    public byte get(final int i) {
        return buffer.get(i);
    }

    public ByteBuffer get(final byte[] bytes, final int offset, final int length) {
        return buffer.get(bytes, offset, length);
    }

    public ByteBuffer get(final byte[] bytes) {
        return buffer.get(bytes);
    }

    public byte[] array() {
        return buffer.array();
    }

    public String toString() {
        return buffer.toString();
    }

    public int hashCode() {
        return buffer.hashCode();
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

        if (buffer != null ? !buffer.equals(that.buffer) : that.buffer != null) {
            return false;
        }

        return true;
    }

    /**
     * Gets a Little Endian Integer
     *
     * @param i Index to read from
     * @return Integer
     */
    public int getInt(final int i) {
        return getIntLE(i);
    }

    public int getIntLE(final int i) {
        int x = 0;
        x |= (0xFF & buffer.get(i + 0)) << 0;
        x |= (0xFF & buffer.get(i + 1)) << 8;
        x |= (0xFF & buffer.get(i + 2)) << 16;
        x |= (0xFF & buffer.get(i + 3)) << 24;
        return x;
    }

    public int getIntBE(int i) {
        int x = 0;
        x |= (0xFF & buffer.get(i + 0)) << 24;
        x |= (0xFF & buffer.get(i + 1)) << 16;
        x |= (0xFF & buffer.get(i + 2)) << 8;
        x |= (0xFF & buffer.get(i + 3)) << 0;
        return x;
    }

    public long getLong(final int i) {
        return buffer.getLong(i);
    }

    public String getCString(final int offset) {
        int end = offset;
        while (get(end) != 0) {
            ++end;
        }
        int len = end - offset;
        return new String(array(), offset, len);
    }

    public String getUTF8String(final int valueOffset) {
        int size = getInt(valueOffset) - 1;
        try {
            return new String(array(), valueOffset + 4, size, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new BSONException("Cannot decode string as UTF-8.");
        }
    }

    public Buffer position(final int i) {
        return buffer.position(i);
    }

    public Buffer reset() {
        return buffer.reset();
    }

    public int size() {
        return getInt(0);
    }
}
