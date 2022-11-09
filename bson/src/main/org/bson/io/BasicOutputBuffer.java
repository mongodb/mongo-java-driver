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

import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * A BSON output stream that stores the output in a single, un-pooled byte array.
 */
public class BasicOutputBuffer extends OutputBuffer {
    private byte[] buffer;
    private int position;

    /**
     * Construct an instance with a default initial byte array size.
     */
    public BasicOutputBuffer() {
        this(1024);
    }

    /**
     * Construct an instance with the specified initial byte array size.
     *
     * @param initialSize the initial size of the byte array
     */
    public BasicOutputBuffer(final int initialSize) {
        buffer = new byte[initialSize];
    }

    /**
     * Gets the internal buffer.
     *
     * @return the internal buffer
     * @since 3.3
     */
    public byte[] getInternalBuffer() {
        return buffer;
    }

    @Override
    public void write(final byte[] b) {
        ensureOpen();
        write(b, 0, b.length);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
        ensureOpen();

        ensure(length);
        System.arraycopy(bytes, offset, buffer, position, length);
        position += length;
    }

    @Override
    public void writeByte(final int value) {
        ensureOpen();

        ensure(1);
        buffer[position++] = (byte) (0xFF & value);
    }

    @Override
    protected void write(final int absolutePosition, final int value) {
        ensureOpen();

        if (absolutePosition < 0) {
            throw new IllegalArgumentException(format("position must be >= 0 but was %d", absolutePosition));
        }
        if (absolutePosition > position - 1) {
            throw new IllegalArgumentException(format("position must be <= %d but was %d", position - 1, absolutePosition));
        }

        buffer[absolutePosition] = (byte) (0xFF & value);
    }

    @Override
    public int getPosition() {
        ensureOpen();
        return position;
    }

    /**
     * @return size of data so far
     */
    @Override
    public int getSize() {
        ensureOpen();
        return position;
    }

    @Override
    public int pipe(final OutputStream out) throws IOException {
        ensureOpen();
        out.write(buffer, 0, position);
        return position;
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        ensureOpen();
        if (newPosition > position || newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        ensureOpen();
        return Arrays.asList(new ByteBufNIO(ByteBuffer.wrap(buffer, 0, position).duplicate().order(LITTLE_ENDIAN)));
    }

    @Override
    public void close() {
        buffer = null;
    }

    private void ensureOpen() {
        if (buffer == null) {
            throw new IllegalStateException("The output is closed");
        }
    }

    private void ensure(final int more) {
        int need = position + more;
        if (need <= buffer.length) {
            return;
        }

        int newSize = buffer.length * 2;
        if (newSize < need) {
            newSize = need + 128;
        }

        byte[] n = new byte[newSize];
        System.arraycopy(buffer, 0, n, 0, position);
        buffer = n;
    }

}
