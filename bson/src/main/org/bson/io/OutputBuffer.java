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

import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static java.lang.String.format;

/**
 * An abstract base class for classes implementing {@code BsonOutput}.
 */
public abstract class OutputBuffer extends OutputStream implements BsonOutput {

    @Override
    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void close() {
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
        writeBytes(bytes, offset, length);
    }

    @Override
    public void writeBytes(final byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeInt32(final int value) {
        write(value >> 0);
        write(value >> 8);
        write(value >> 16);
        write(value >> 24);
    }

    @Override
    public void writeInt32(final int position, final int value) {
        write(position, value >> 0);
        write(position + 1, value >> 8);
        write(position + 2, value >> 16);
        write(position + 3, value >> 24);
    }

    @Override
    public void writeInt64(final long value) {
        write((byte) (0xFFL & (value >> 0)));
        write((byte) (0xFFL & (value >> 8)));
        write((byte) (0xFFL & (value >> 16)));
        write((byte) (0xFFL & (value >> 24)));
        write((byte) (0xFFL & (value >> 32)));
        write((byte) (0xFFL & (value >> 40)));
        write((byte) (0xFFL & (value >> 48)));
        write((byte) (0xFFL & (value >> 56)));
    }

    @Override
    public void writeDouble(final double x) {
        writeLong(Double.doubleToRawLongBits(x));
    }

    @Override
    public void writeString(final String str) {
        writeInt(0); // making space for size
        int strLen = writeCharacters(str, false);
        writeInt32(getPosition() - strLen - 4, strLen);
    }

    @Override
    public void writeCString(final String value) {
        writeCharacters(value, true);
    }

    @Override
    public void writeObjectId(final ObjectId value) {
        write(value.toByteArray());
    }

    /**
     * Gets the output size in bytes.
     * @return the size
     */
    public int size() {
        return getSize();
    }

    /**
     * Pipe the contents of this output buffer into the given output stream
     *
     * @param out the stream to pipe to
     * @return number of bytes written to the stream
     * @throws java.io.IOException if the stream throws an exception
     */
    public abstract int pipe(OutputStream out) throws IOException;

    /**
     * Get a list of byte buffers that are prepared to be read from; in other words, whose position is 0 and whose limit is the number of
     * bytes that should read. <p> Note that the byte buffers may be read-only. </p>
     *
     * @return the non-null list of byte buffers.
     */
    public abstract List<ByteBuf> getByteBuffers();

    @Override
    public abstract void truncateToPosition(int newPosition);

    /**
     * Gets a copy of the buffered bytes.
     *
     * @return the byte array
     * @see org.bson.io.OutputBuffer#pipe(java.io.OutputStream)
     */
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream(size());
            pipe(bout);
            return bout.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException("should be impossible", ioe);
        }
    }

    @Override
    public void write(final int value) {
        writeByte(value);
    }

    /**
     * Writes the given integer value to the buffer.
     *
     * @param value the value to write
     * @see #writeInt32
     */
    public void writeInt(final int value) {
        writeInt32(value);
    }

    @Override
    public String toString() {
        return getClass().getName() + " size: " + size() + " pos: " + getPosition();
    }

    /**
     * Write the specified byte at the specified position.
     *
     * @param position the position, which must be greater than equal to 0 and at least 4 less than the stream size
     * @param value the value to write.  The 24 high-order bits of the value are ignored.
     */
    protected abstract void write(int position, int value);

    /**
     * Writes the given long value to the buffer.
     *
     * @param value the value to write
     * @see #writeInt64
     */
    public void writeLong(final long value) {
        writeInt64(value);
    }

    private int writeCharacters(final String str, final boolean checkForNullCharacters) {
        int len = str.length();
        int total = 0;

        for (int i = 0; i < len;) {
            int c = Character.codePointAt(str, i);

            if (checkForNullCharacters && c == 0x0) {
                throw new BsonSerializationException(format("BSON cstring '%s' is not valid because it contains a null character "
                                                            + "at index %d", str, i));
            }
            if (c < 0x80) {
                write((byte) c);
                total += 1;
            } else if (c < 0x800) {
                write((byte) (0xc0 + (c >> 6)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 2;
            } else if (c < 0x10000) {
                write((byte) (0xe0 + (c >> 12)));
                write((byte) (0x80 + ((c >> 6) & 0x3f)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 3;
            } else {
                write((byte) (0xf0 + (c >> 18)));
                write((byte) (0x80 + ((c >> 12) & 0x3f)));
                write((byte) (0x80 + ((c >> 6) & 0x3f)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 4;
            }

            i += Character.charCount(c);
        }

        write((byte) 0);
        total++;
        return total;
    }
}
