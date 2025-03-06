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

import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ByteBufferBsonOutput extends OutputBuffer {

    private static final int MAX_SHIFT = 31;
    private static final int INITIAL_SHIFT = 10;
    public static final int INITIAL_BUFFER_SIZE = 1 << INITIAL_SHIFT;
    public static final int MAX_BUFFER_SIZE = 1 << 24;

    private final BufferProvider bufferProvider;
    private final List<ByteBuf> bufferList = new ArrayList<>();
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

    /**
     * Creates a new empty {@link ByteBufferBsonOutput.Branch},
     * which gets merged into this {@link ByteBufferBsonOutput} on {@link ByteBufferBsonOutput.Branch#close()}
     * by appending its data without copying it.
     * If multiple branches are created, they are merged in the order they are {@linkplain ByteBufferBsonOutput.Branch#close() closed}.
     * {@linkplain #close() Closing} this {@link ByteBufferBsonOutput} does not {@linkplain ByteBufferBsonOutput.Branch#close() close} the branch.
     *
     * @return A new {@link ByteBufferBsonOutput.Branch}.
     */
    public ByteBufferBsonOutput.Branch branch() {
        return new ByteBufferBsonOutput.Branch(this);
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
    public void writeInt32(final int value) {
        ensureOpen();
        ByteBuf buf = getCurrentByteBuffer();
        if (buf.remaining() >= 4) {
            buf.putInt(value);
            position += 4;
        } else {
            // fallback for edge cases
            super.writeInt32(value);
        }
    }


    @Override
    public void writeInt32(final int absolutePosition, final int value) {
        ensureOpen();

        if (absolutePosition < 0) {
            throw new IllegalArgumentException(String.format("position must be >= 0 but was %d", absolutePosition));
        }

        if (absolutePosition  + 3 > position - 1) {
            throw new IllegalArgumentException(String.format("Cannot write 4 bytes starting at position %d: current size is %d bytes",
                    position - 1,
                    absolutePosition + 3));
        }

        BufferPositionPair bufferPositionPair = getBufferPositionPair(absolutePosition);
        ByteBuf byteBuffer = getByteBufferAtIndex(bufferPositionPair.bufferIndex);
        int capacity = byteBuffer.position() - bufferPositionPair.position;

        if (capacity >= 4) {
            byteBuffer.putInt(bufferPositionPair.position, value);
        } else {
            // fallback for edge cases
            int valueToWrite = value;
            int pos = bufferPositionPair.position;
            int bufferIndex = bufferPositionPair.bufferIndex;

            for (int i = 0; i < 4; i++) {
                byteBuffer.put(pos++, (byte) valueToWrite);
                valueToWrite = valueToWrite >> 8;
                if (--capacity == 0) {
                    byteBuffer = getByteBufferAtIndex(++bufferIndex);
                    pos = 0;
                    capacity = byteBuffer.position();
                }
            }
        }
    }

    @Override
    public void writeDouble(final double value) {
        ByteBuf buf = getCurrentByteBuffer();
        if (buf.remaining() >= 8) {
            ensureOpen();
            buf.putDouble(value);
            position += 8;
        } else {
            // fallback for edge cases
            writeInt64(Double.doubleToRawLongBits(value));
        }
    }

    @Override
    public void writeInt64(final long value) {
        ByteBuf buf = getCurrentByteBuffer();
        if (buf.remaining() >= 8) {
            ensureOpen();
            buf.putLong(value);
            position += 8;
        } else {
            // fallback for edge cases
            super.writeInt64(value);
        }
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

        List<ByteBuf> buffers = new ArrayList<>(bufferList.size());
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
        if (newPosition == position) {
            return;
        }
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

    /**
     * The {@link #flush()} method of {@link ByteBufferBsonOutput} and of its subclasses does nothing.</p>
     */
    @Override
    public final void flush() throws IOException {
    }

    /**
     * {@inheritDoc}
     * <p>
     * Idempotent.</p>
     */
    @Override
    public void close() {
        if (isOpen()) {
            for (final ByteBuf cur : bufferList) {
                cur.release();
            }
            bufferList.clear();
            closed = true;
        }
    }

    private BufferPositionPair getBufferPositionPair(final int absolutePosition) {
        int positionInBuffer = absolutePosition;
        int bufferIndex = 0;
        int bufferSize = bufferList.get(bufferIndex).position();
        int startPositionOfBuffer = 0;
        while (startPositionOfBuffer + bufferSize <= absolutePosition) {
            bufferIndex++;
            startPositionOfBuffer += bufferSize;
            positionInBuffer -= bufferSize;
            bufferSize = bufferList.get(bufferIndex).position();
        }

        return new BufferPositionPair(bufferIndex, positionInBuffer);
    }

    private void ensureOpen() {
        if (!isOpen()) {
            throw new IllegalStateException("The output is closed");
        }
    }

    boolean isOpen() {
        return !closed;
    }

    /**
     * @see #branch()
     */
    private void merge(final ByteBufferBsonOutput branch) {
        assertTrue(branch instanceof ByteBufferBsonOutput.Branch);
        branch.bufferList.forEach(ByteBuf::retain);
        bufferList.addAll(branch.bufferList);
        curBufferIndex += branch.curBufferIndex + 1;
        position += branch.position;
    }

    public static final class Branch extends ByteBufferBsonOutput {
        private final ByteBufferBsonOutput parent;

        private Branch(final ByteBufferBsonOutput parent) {
            super(parent.bufferProvider);
            this.parent = parent;
        }

        /**
         * @see #branch()
         */
        @Override
        public void close() {
            if (isOpen()) {
                try {
                    assertTrue(parent.isOpen());
                    parent.merge(this);
                } finally {
                    super.close();
                }
            }
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
