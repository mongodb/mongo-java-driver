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

public abstract class OutputBuffer extends OutputStream implements BsonOutput {

    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void close() {
    }

    public void write(final byte[] bytes, final int offset, final int length) {
        writeBytes(bytes, offset, length);
    }

    public void write(final int value) {
        writeByte(value);
    }

    @Override
    public void writeBytes(final byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
    }

    public abstract int getPosition();

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

    public void writeDouble(final double x) {
        writeLong(Double.doubleToRawLongBits(x));
    }

    public void writeString(final String str) {
        writeInt(0); // making space for size
        int strLen = writeCharacters(str, false);
        writeInt32(getPosition() - strLen - 4, strLen);
    }

    public void writeCString(final String value) {
        writeCharacters(value, true);
    }


    @Override
    public void writeObjectId(final ObjectId value) {
        write(value.toByteArray());
    }

    /**
     * @return size of data so far
     */
    public int size() {
        return getSize();
    }

    /**
     * Pipe the contents of this output buffer into the given output stream
     *
     * @return number of bytes written to the stream
     */
    public abstract int pipe(OutputStream out) throws IOException;

    /**
     * Get a list of byte buffers that are prepared to be read from; in other words, whose position is 0 and whose limit is the number of
     * bytes that should read. <p> Note that the byte buffers may be read-only. </p>
     *
     * @return the non-null list of byte buffers.
     */
    public abstract List<ByteBuf> getByteBuffers();

    /**
     * Truncates the buffer to the given new position, which must be greater than or equal to zero and less than or equal to the current
     * size of this buffer.
     *
     * @param newPosition the position to truncate this buffer to
     */
    public abstract void truncateToPosition(int newPosition);

    /**
     * mostly for testing
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

    public void writeInt(final int value) {
        writeInt32(value);
    }

    /**
     * Write the specified byte at the specified position
     * @param position the position, which must be greater than equal to 0 and less than or equal to the stream size
     * @param value the value to write.  The 24 high-order bits of the value are ignored.
     */
    protected abstract void write(final int position, final int value);

    public void writeLong(final long value) {
        writeInt64(value);
    }

    private int writeCharacters(final String str, final boolean checkForNullCharacters) {
        int len = str.length();
        int total = 0;

        for (int i = 0; i < len;/*i gets incremented*/) {
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

    public String toString() {
        return getClass().getName() + " size: " + size() + " pos: " + getPosition();
    }
}
