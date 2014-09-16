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

package com.mongodb.connection;

import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class should not be considered as part of the public API, and it may change or be removed at any time.
 * @since 3.0
 */
public class ByteBufferOutputBuffer extends OutputBuffer {

    public static final int INITIAL_BUFFER_SIZE = 1024;
    public static final int MAX_BUFFER_SIZE = 1 << 24;

    private final BufferProvider bufferProvider;
    private final List<ByteBuf> bufferList = new ArrayList<ByteBuf>();
    private int curBufferIndex = 0;
    private int position = 0;

    /**
     * Construct an instance that uses the given buffer provider to allocate byte buffers as needs as it grows.
     *
     * @param bufferProvider the non-null buffer provider
     */
    public ByteBufferOutputBuffer(final BufferProvider bufferProvider) {
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
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
            bufferList.add(bufferProvider.getBuffer(Math.min(INITIAL_BUFFER_SIZE << index, MAX_BUFFER_SIZE)));
        }
        return bufferList.get(index);
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public int getSize() {
        return position;
    }

    protected void write(final int absolutePosition, final int value) {
        if (absolutePosition < 0) {
            throw new IllegalArgumentException(String.format("position must be >= 0 but was %d", absolutePosition));
        }
        if (absolutePosition > position) {
            throw new IllegalArgumentException(String.format("position must be <= %d but was %d", position, absolutePosition));
        }

        getBufferPositionPair(absolutePosition).put((byte) value);
    }

    @Override
    public int size() {
        return position;
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        List<ByteBuf> buffers = new ArrayList<ByteBuf>(bufferList.size());
        for (final ByteBuf cur : bufferList) {
            buffers.add(cur.duplicate().flip());
        }
        return buffers;
    }


    @Override
    public int pipe(final OutputStream out) throws IOException {
        int total = 0;
        for (final ByteBuf cur : bufferList) {
            cur.flip();
            out.write(cur.array(), 0, cur.limit());
            total += cur.limit();
        }
        return total;
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        if (newPosition > position || newPosition < 0) {
            throw new IllegalArgumentException();
        }

        BufferPositionPair bufferPositionPair = getBufferPositionPair(newPosition);

        bufferList.get(bufferPositionPair.bufferIndex).position(bufferPositionPair.position);

        while (bufferList.size() > bufferPositionPair.bufferIndex + 1) {
            ByteBuf buffer = bufferList.remove(bufferList.size() - 1);
            buffer.close();
        }

        curBufferIndex = bufferPositionPair.bufferIndex;
        position = newPosition;
    }

    @Override
    public void close() {
        for (final ByteBuf cur : bufferList) {
            cur.close();
        }
        bufferList.clear();
    }

    // TODO: desperately seeking unit test
    private void backpatchSizeWithOffset(final int size, final int additionalOffset) {
        getBufferPositionPair(position - size - additionalOffset).putInt(size);
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

    class BufferPositionPair {
        private int bufferIndex;
        private int position;

        BufferPositionPair(final int bufferIndex, final int position) {
            this.bufferIndex = bufferIndex;
            this.position = position;
        }

        public void putInt(final int val) {
            put((byte) (val));
            put((byte) (val >> 8));
            put((byte) (val >> 16));
            put((byte) (val >> 24));
        }

        void put(final byte b) {
            ByteBuf byteBuffer = getByteBufferAtIndex(bufferIndex);
            byteBuffer.put(position++, b);

            if (position >= byteBuffer.capacity()) {
                bufferIndex++;
                position = 0;
            }
        }
    }

}
