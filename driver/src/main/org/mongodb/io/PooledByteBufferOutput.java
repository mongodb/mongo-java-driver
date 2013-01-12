/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.io;

import org.bson.io.OutputBuffer;
import org.bson.util.BufferPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class PooledByteBufferOutput extends OutputBuffer {

    public static final int INITIAL_BUFFER_SIZE = 1024;
    private final BufferPool<ByteBuffer> pool;
    private final List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
    private int curBufferIndex = 0;
    private int position = 0;

    public PooledByteBufferOutput(final BufferPool<ByteBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public void write(final byte[] b, final int offset, final int len) {
        int currentOffset = offset;
        int remainingLen = len;
        while (remainingLen > 0) {
            final ByteBuffer buf = getCurrentByteBuffer();
            final int bytesToPutInCurrentBuffer = Math.min(buf.remaining(), remainingLen);
            buf.put(b, currentOffset, bytesToPutInCurrentBuffer);
            remainingLen -= bytesToPutInCurrentBuffer;
            currentOffset += bytesToPutInCurrentBuffer;
        }
        position += len;
    }

    @Override
    public void write(final int b) {
        getCurrentByteBuffer().put((byte) b);
        position++;
    }

    private ByteBuffer getCurrentByteBuffer() {
        final ByteBuffer curByteBuffer = getByteBufferAtIndex(curBufferIndex);
        if (curByteBuffer.hasRemaining()) {
            return curByteBuffer;
        }

        curBufferIndex++;
        return getByteBufferAtIndex(curBufferIndex);
    }

    private ByteBuffer getByteBufferAtIndex(final int index) {
        if (bufferList.size() < index + 1) {
            bufferList.add(pool.get(INITIAL_BUFFER_SIZE << index));
        }
        return bufferList.get(index);
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public void writeInt(final int pos, final int x) {
        // TODO: ditch this method
        throw new UnsupportedOperationException();
    }

    /**
     * Backpatches the size of a document or message by writing the size into the four bytes starting at getPosition() -
     * size.
     *
     * @param size the size of the document or message
     */
    @Override
    public void backpatchSize(final int size) {
        backpatchSizeWithOffset(size, 0);
    }

    @Override
    protected void backpatchSize(final int size, final int additionalOffset) {
        backpatchSizeWithOffset(size, additionalOffset);
    }

    @Override
    public int size() {
        return position;
    }

    @Override // TODO: Implement this
    public int pipe(final OutputStream out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pipe(final SocketChannel socketChannel) throws IOException {
        for (final ByteBuffer cur : bufferList) {
            cur.flip();
        }

        for (long bytesRead = 0; bytesRead < size(); ) {
            bytesRead += socketChannel.write(bufferList.toArray(new ByteBuffer[bufferList.size()]), 0,
                                             bufferList.size());
        }
    }

    @Override
    public void close() {
        for (final ByteBuffer cur : bufferList) {
            pool.done(cur);
        }
        bufferList.clear();
    }

    // TODO: go backwards instead of forwards?  Probably doesn't matter with power of two
    // TODO: desperately seeking unit test
    private void backpatchSizeWithOffset(final int size, final int additionalOffset) {
        final int backpatchPosition = position - size - additionalOffset;
        int backpatchPositionInBuffer = backpatchPosition;
        int bufferIndex = 0;
        int bufferSize = INITIAL_BUFFER_SIZE;
        int startPositionOfBuffer = 0;
        while (startPositionOfBuffer + bufferSize <= backpatchPosition) {
            bufferIndex++;
            startPositionOfBuffer += bufferSize;
            backpatchPositionInBuffer -= bufferSize;
            bufferSize <<= 1;
        }

        new BufferPositionPair(bufferIndex, backpatchPositionInBuffer).putInt(size);
    }

    class BufferPositionPair {
        int bufferIndex;
        int position;

        BufferPositionPair(final int bufferIndex, final int position) {
            this.bufferIndex = bufferIndex;
            this.position = position;
        }

        public void putInt(final int val) {
            put((byte) (val >> 0));
            put((byte) (val >> 8));
            put((byte) (val >> 16));
            put((byte) (val >> 24));
        }

        void put(byte b) {
            ByteBuffer byteBuffer = getByteBufferAtIndex(bufferIndex);
            byteBuffer.put(position++, b);

            if (position >= byteBuffer.capacity()) {
                bufferIndex++;
                position = 0;
            }
        }
    }

}
