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

package org.mongodb.connection;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PooledByteBufferOutputBuffer extends ChannelAwareOutputBuffer {

    public static final int INITIAL_BUFFER_SIZE = 1024;
    private final BufferPool<ByteBuffer> pool;
    private final List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
    private int curBufferIndex = 0;
    private int position = 0;

    public PooledByteBufferOutputBuffer(final BufferPool<ByteBuffer> pool) {
        if (pool == null) {
            throw new IllegalArgumentException("pool can not be null");
        }
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
            bufferList.add(pool.get(Math.min(INITIAL_BUFFER_SIZE << index, pool.getMaximumPooledSize())));
        }
        return bufferList.get(index);
    }

    @Override
    public int getPosition() {
        return position;
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

    @Override
    public void pipe(final OutputStream out) throws IOException {
        for (final ByteBuffer cur : bufferList) {
            cur.flip();
            out.write(cur.array(), 0, cur.limit());
        }
    }

    @Override
    public void pipeAndClose(final SocketChannel socketChannel) throws IOException {
        for (final ByteBuffer cur : bufferList) {
            cur.flip();
        }

        for (long bytesRead = 0; bytesRead < size();/*bytesRead incremented elsewhere*/) {
            bytesRead += socketChannel.write(bufferList.toArray(new ByteBuffer[bufferList.size()]), 0,
                    bufferList.size());
        }
        close();
    }


    @Override
    public void pipeAndClose(final Socket socket) throws IOException {
        for (final ByteBuffer cur : bufferList) {
            cur.flip();
            socket.getOutputStream().write(cur.array(), 0, cur.limit());
        }

        close();
    }

    @Override
    public void pipeAndClose(final AsyncWritableByteChannel channel, final AsyncCompletionHandler handler) {
        final Iterator<ByteBuffer> iter = bufferList.iterator();
        pipeOneBuffer(channel, iter.next(), new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                if (iter.hasNext()) {
                    pipeOneBuffer(channel, iter.next(), this);
                }
                else {
                    close();
                    handler.completed(size());
                }
            }

            @Override
            public void failed(final Throwable t) {
                close();
                handler.failed(t);
            }
        });
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        if (newPosition > position || newPosition < 0) {
            throw new IllegalArgumentException();
        }

        BufferPositionPair bufferPositionPair = getBufferPositionPair(newPosition);

        bufferList.get(bufferPositionPair.bufferIndex).position(bufferPositionPair.position);

        while (bufferList.size() > bufferPositionPair.bufferIndex + 1) {
            ByteBuffer buffer = bufferList.remove(bufferList.size() - 1);
            pool.release(buffer);
        }

        curBufferIndex = bufferPositionPair.bufferIndex;
        position = newPosition;
    }

    private void pipeOneBuffer(final AsyncWritableByteChannel channel, final ByteBuffer byteBuffer,
                               final AsyncCompletionHandler outerHandler) {
        byteBuffer.flip();
        channel.write(byteBuffer, new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                if (byteBuffer.hasRemaining()) {
                    channel.write(byteBuffer, this);
                }
                else {
                    outerHandler.completed(byteBuffer.limit());
                }
            }

            @Override
            public void failed(final Throwable t) {
                outerHandler.failed(t);
            }
        });
    }

    @Override
    public void close() {
        for (final ByteBuffer cur : bufferList) {
            pool.release(cur);
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
            final ByteBuffer byteBuffer = getByteBufferAtIndex(bufferIndex);
            byteBuffer.put(position++, b);

            if (position >= byteBuffer.capacity()) {
                bufferIndex++;
                position = 0;
            }
        }
    }

}
