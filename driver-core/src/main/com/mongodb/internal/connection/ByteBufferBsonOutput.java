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

package com.mongodb.internal.connection;

import com.mongodb.connection.BufferProvider;
import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class should not be considered as part of the public API, and it may change or be removed at any time.
 *
 */
public class ByteBufferBsonOutput extends OutputBuffer {

    private static final int MAX_SHIFT = 31;
    private static final int INITIAL_SHIFT = 10;
    public static final int INITIAL_BUFFER_SIZE = 1 << INITIAL_SHIFT;
    public static final int MAX_BUFFER_SIZE = 1 << 24;

    private final BufferProvider bufferProvider;
    private final List<ByteBuf> bufferList = new ArrayList<ByteBuf>();
    private int curBufferIndex = 0;
    private int position = 0;
    private boolean closed;

    /**
     * Construct an instance that uses the given buffer provider to allocate byte buffers as needs as it grows.
     *
     * @param bufferProvider the non-null buffer provider
     */
    public ByteBufferBsonOutput(final BufferProvider bufferProvider) {
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
        ensureOpen();

        int currentOffset = offset;
        int remainingLen = length;
        while (remainingLen > 0) {
            ByteBuf buf = getCurrentByteBuffer();
            int bytesToPutInCurrentBuffer = Math.min(buf.remaining(), remainingLen);
            buf.put(bytes, currentOffset, bytesToPutInCurrentBuffer);
            remainingLen -= bytesToPutInCurrentBuffer;
            currentOffset += bytesToPutInCurrentBuffer;
        }
        position += length;
    }

    @Override
    public void writeByte(final int value) {
        ensureOpen();

        getCurrentByteBuffer().put((byte) value);
        position++;
    }

    private ByteBuf getCurrentByteBuffer() {
        ByteBuf curByteBuffer = getByteBufferAtIndex(curBufferIndex);
        if (curByteBuffer.hasRemaining()) {
            return curByteBuffer;
        }

        curBufferIndex++;
        return getByteBufferAtIndex(curBufferIndex);
    }

    private ByteBuf getByteBufferAtIndex(final int index) {
        if (bufferList.size() < index + 1) {
            bufferList.add(bufferProvider.getBuffer(index >= (MAX_SHIFT - INITIAL_SHIFT)
                                                            ? MAX_BUFFER_SIZE
                                                            : Math.min(INITIAL_BUFFER_SIZE << index, MAX_BUFFER_SIZE)));
        }
        return bufferList.get(index);
    }

    @Override
    public int getPosition() {
        ensureOpen();
        return position;
    }

    @Override
    public int getSize() {
        ensureOpen();
        return position;
    }

    protected void write(final int absolutePosition, final int value) {
        ensureOpen();

        if (absolutePosition < 0) {
            throw new IllegalArgumentException(String.format("position must be >= 0 but was %d", absolutePosition));
        }
        if (absolutePosition > position - 1) {
            throw new IllegalArgumentException(String.format("position must be <= %d but was %d", position - 1, absolutePosition));
        }

        BufferPositionPair bufferPositionPair = getBufferPositionPair(absolutePosition);
        ByteBuf byteBuffer = getByteBufferAtIndex(bufferPositionPair.bufferIndex);
        byteBuffer.put(bufferPositionPair.position++, (byte) value);
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        ensureOpen();

        List<ByteBuf> buffers = new ArrayList<ByteBuf>(bufferList.size());
        for (final ByteBuf cur : bufferList) {
            buffers.add(cur.duplicate().order(ByteOrder.LITTLE_ENDIAN).flip());
        }
        return buffers;
    }


    @Override
    public int pipe(final OutputStream out) throws IOException {
        ensureOpen();

        byte[] tmp = new byte[INITIAL_BUFFER_SIZE];

        int total = 0;
        for (final ByteBuf cur : getByteBuffers()) {
            ByteBuf dup = cur.duplicate();
            while (dup.hasRemaining()) {
                int numBytesToCopy = Math.min(dup.remaining(), tmp.length);
                dup.get(tmp, 0, numBytesToCopy);
                out.write(tmp, 0, numBytesToCopy);
            }
            total += dup.limit();
        }
        return total;
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        ensureOpen();

        if (newPosition > position || newPosition < 0) {
            throw new IllegalArgumentException();
        }

        BufferPositionPair bufferPositionPair = getBufferPositionPair(newPosition);

        bufferList.get(bufferPositionPair.bufferIndex).position(bufferPositionPair.position);

        while (bufferList.size() > bufferPositionPair.bufferIndex + 1) {
            ByteBuf buffer = bufferList.remove(bufferList.size() - 1);
            buffer.release();
        }

        curBufferIndex = bufferPositionPair.bufferIndex;
        position = newPosition;
    }

    @Override
    public void close() {
        for (final ByteBuf cur : bufferList) {
            cur.release();
        }
        bufferList.clear();
        closed = true;
    }

    private BufferPositionPair getBufferPositionPair(final int absolutePosition) {
        int positionInBuffer = absolutePosition;
        int bufferIndex = 0;
        int bufferSize = INITIAL_BUFFER_SIZE;
        int startPositionOfBuffer = 0;
        while (startPositionOfBuffer + bufferSize <= absolutePosition) {
            bufferIndex++;
            startPositionOfBuffer += bufferSize;
            positionInBuffer -= bufferSize;
            bufferSize = bufferList.get(bufferIndex).limit();
        }

        return new BufferPositionPair(bufferIndex, positionInBuffer);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("The output is closed");
        }
    }

    private static final class BufferPositionPair {
        private final int bufferIndex;
        private int position;

        BufferPositionPair(final int bufferIndex, final int position) {
            this.bufferIndex = bufferIndex;
            this.position = position;
        }
    }
}
