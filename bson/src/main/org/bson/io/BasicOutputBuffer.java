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

import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class BasicOutputBuffer extends OutputBuffer {
    private int position;
    private byte[] buffer = new byte[1024];

    @Override
    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
        ensure(length);
        System.arraycopy(bytes, offset, buffer, position, length);
        position += length;
    }

    @Override
    public void writeByte(final int value) {
        ensure(1);
        buffer[position++] = (byte) (0xFF & value);
    }

    @Override
    protected void write(final int absolutePosition, final int value) {
        if (absolutePosition < 0) {
            throw new IllegalArgumentException(String.format("position must be >= 0 but was %d", absolutePosition));
        }
        if (absolutePosition > position) {
            throw new IllegalArgumentException(String.format("position must be <= %d but was %d", position, absolutePosition));
        }

        if (absolutePosition == position) {
            ensure(1);
        }

        buffer[absolutePosition] = (byte) (0xFF & value);
    }

    @Override
    public int getPosition() {
        return position;
    }

    /**
     * @return size of data so far
     */
    @Override
    public int getSize() {
        return position;
    }

    @Override
    public int pipe(final OutputStream out) throws IOException {
        out.write(buffer, 0, position);
        return position;
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        if (newPosition > position || newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        return Arrays.<ByteBuf>asList(new ByteBufNIO(ByteBuffer.wrap(buffer, 0, position).duplicate()));
    }

    private void ensure(final int more) {
        int need = position + more;
        if (need < buffer.length) {
            return;
        }

        int newSize = buffer.length * 2;
        if (newSize <= need) {
            newSize = need + 128;
        }

        byte[] n = new byte[newSize];
        System.arraycopy(buffer, 0, n, 0, position);
        buffer = n;
    }

    private void setPosition(final int position) {
        this.position = position;
    }
}
