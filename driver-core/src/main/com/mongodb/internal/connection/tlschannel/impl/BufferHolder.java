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
 *
 * Original Work: MIT License, Copyright (c) [2015-2018] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.impl;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.tlschannel.BufferAllocator;
import org.bson.ByteBuf;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import static com.mongodb.internal.connection.tlschannel.impl.TlsChannelImpl.MAX_TLS_PACKET_SIZE;
import static java.lang.String.format;

public class BufferHolder {

    private static final Logger LOGGER = Loggers.getLogger("connection.tls");
    // Round to next highest power of two to account for PowerOfTwoBufferPool allocation style
    private static final byte[] ZEROS = new byte[roundUpToNextHighestPowerOfTwo(MAX_TLS_PACKET_SIZE)];

    public final String name;
    public final BufferAllocator allocator;
    public final boolean plainData;
    public final int maxSize;
    public final boolean opportunisticDispose;

    private ByteBuf byteBuf;
    public ByteBuffer buffer;
    public int lastSize;

    public BufferHolder(final String name, final BufferAllocator allocator, final int initialSize,
                        final int maxSize, final boolean plainData, final boolean opportunisticDispose) {
        this.name = name;
        this.allocator = allocator;
        this.buffer = null;
        this.maxSize = maxSize;
        this.plainData = plainData;
        this.opportunisticDispose = opportunisticDispose;
        this.lastSize = initialSize;
    }

    public void prepare() {
        if (buffer == null) {
            byteBuf = allocator.allocate(lastSize);
            buffer = byteBuf.asNIO();
        }
    }

    public boolean release() {
        if (opportunisticDispose && buffer.position() == 0) {
            return dispose();
        } else {
            return false;
        }
    }

    public boolean dispose() {
        if (buffer != null) {
            allocator.free(byteBuf);
            buffer = null;
            return true;
        } else {
            return false;
        }
    }

    public void resize(final int newCapacity) {
        if (newCapacity > maxSize) {
            throw new IllegalArgumentException(format("new capacity (%s) bigger than absolute max size (%s)", newCapacity, maxSize));
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("resizing buffer %s, increasing from %s to %s (manual sizing)", name, buffer.capacity(), newCapacity));
        }
        resizeImpl(newCapacity);
    }

    public void enlarge() {
        if (buffer.capacity() >= maxSize) {
            throw new IllegalStateException(
                    format("%s buffer insufficient despite having capacity of %d", name, buffer.capacity()));
        }
        int newCapacity = Math.min(lastSize * 2, maxSize);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("enlarging buffer %s, increasing from %s to %s (automatic enlarge)", name, buffer.capacity(), newCapacity));
        }
        resizeImpl(newCapacity);
    }

    private void resizeImpl(final int newCapacity) {
        ByteBuf newByteBuf = allocator.allocate(newCapacity);
        ByteBuffer newBuffer = newByteBuf.asNIO();
        ((Buffer) buffer).flip();
        newBuffer.put(buffer);
        if (plainData) {
            zero();
        }
        allocator.free(byteBuf);
        byteBuf = newByteBuf;
        buffer = newBuffer;
        lastSize = newCapacity;
    }

    /**
     * Fill with zeros the remaining of the supplied buffer. This method does
     * not change the buffer position.
     * <p>
     * Typically used for security reasons, with buffers that contains
     * now-unused plaintext.
     */
    public void zeroRemaining() {
        ((Buffer) buffer).mark();
        buffer.put(ZEROS, 0, buffer.remaining());
        ((Buffer) buffer).reset();
    }

    /**
     * Fill the buffer with zeros. This method does not change the buffer position.
     * <p>
     * Typically used for security reasons, with buffers that contains
     * now-unused plaintext.
     */
    public void zero() {
        ((Buffer) buffer).mark();
        ((Buffer) buffer).position(0);
        buffer.put(ZEROS, 0, buffer.remaining());
        ((Buffer) buffer).reset();
    }

    public boolean nullOrEmpty() {
        return buffer == null || buffer.position() == 0;
    }

    private static int roundUpToNextHighestPowerOfTwo(final int size) {
        int v = size;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }
}
