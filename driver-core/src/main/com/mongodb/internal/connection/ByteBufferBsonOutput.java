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

import com.mongodb.internal.connection.netty.NettyByteBuf;
import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

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

    @Override
    protected int writeCharacters(final String str, final boolean checkForNullCharacters) {
        ensureOpen();
        ByteBuf buf = getCurrentByteBuffer();
        if ((buf.remaining() >= str.length() + 1)) {
            if (buf.hasArray()) {
                return writeCharactersOnArray(str, checkForNullCharacters, buf);
            } else if (buf instanceof NettyByteBuf) {
                return writeCharactersOnNettyByteBuf(str, checkForNullCharacters, buf);
            }
        }
        return super.writeCharacters(str, 0, checkForNullCharacters);
    }

    private static void validateNoNullSingleByteChars(String str, long chars, int i) {
        long tmp = (chars & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | chars | 0x7F7F7F7F7F7F7F7FL);
        if (tmp != 0) {
            int firstZero = Long.numberOfTrailingZeros(tmp) >>> 3;
            throw new BsonSerializationException(format("BSON cstring '%s' is not valid because it contains a null character "
                    + "at index %d", str, i + firstZero));
        }
    }

    private static void validateNoNullAsciiCharacters(String str, long asciiChars, int i) {
        // simplified Hacker's delight search for zero with ASCII chars i.e. which doesn't use the MSB
        long tmp = asciiChars + 0x7F7F7F7F7F7F7F7FL;
        // MSB is 0 iff the byte is 0x00, 1 otherwise
        tmp = ~tmp & 0x8080808080808080L;
        // MSB is 1 iff the byte is 0x00, 0 otherwise
        if (tmp != 0) {
            // there's some 0x00 in the word
            int firstZero = Long.numberOfTrailingZeros(tmp) >> 3;
            throw new BsonSerializationException(format("BSON cstring '%s' is not valid because it contains a null character "
                    + "at index %d", str, i + firstZero));
        }
    }

    private int writeCharactersOnNettyByteBuf(String str, boolean checkForNullCharacters, ByteBuf buf) {
        int i = 0;
        io.netty.buffer.ByteBuf nettyBuffer = ((NettyByteBuf) buf).asByteBuf();
        // readonly buffers, netty buffers and off-heap NIO ByteBuffer
        boolean slowPath = false;
        int batches = str.length() / 8;
        final int writerIndex = nettyBuffer.writerIndex();
        // this would avoid resizing the buffer while appending: ASCII length + delimiter required space
        nettyBuffer.ensureWritable(str.length() + 1);
        for (int b = 0; b < batches; b++) {
            i = b * 8;
            // read 4 chars at time to preserve the 0x0100 cases
            long evenChars = str.charAt(i) |
                    str.charAt(i + 2) << 16 |
                    (long) str.charAt(i + 4) << 32 |
                    (long) str.charAt(i + 6) << 48;
            long oddChars = str.charAt(i + 1) |
                    str.charAt(i + 3) << 16 |
                    (long) str.charAt(i + 5) << 32 |
                    (long) str.charAt(i + 7) << 48;
            // check that both the second byte and the MSB of the first byte of each pair is 0
            // needed for cases like \u0100 and \u0080
            long mergedChars = evenChars | oddChars;
            if ((mergedChars & 0xFF80FF80FF80FF80L) != 0) {
                if (allSingleByteChars(mergedChars)) {
                    i = tryWriteAsciiChars(str, checkForNullCharacters, oddChars, evenChars, nettyBuffer, writerIndex, i);
                }
                slowPath = true;
                break;
            }
            // all ASCII - compose them into a single long
            long asciiChars = oddChars << 8 | evenChars;
            if (checkForNullCharacters) {
                validateNoNullAsciiCharacters(str, asciiChars, i);
            }
            nettyBuffer.setLongLE(writerIndex + i, asciiChars);
        }
        if (!slowPath) {
            i = batches * 8;
            // do the rest, if any
            for (; i < str.length(); i++) {
                char c = str.charAt(i);
                if (checkForNullCharacters && c == 0x0) {
                    throw new BsonSerializationException(format("BSON cstring '%s' is not valid because it contains a null character "
                            + "at index %d", str, i));
                }
                if (c >= 0x80) {
                    slowPath = true;
                    break;
                }
                nettyBuffer.setByte(writerIndex + i, c);
            }
        }
        if (slowPath) {
            // ith char is not ASCII:
            position += i;
            buf.position(writerIndex + i);
            return i + super.writeCharacters(str, i, checkForNullCharacters);
        } else {
            nettyBuffer.setByte(writerIndex + str.length(), 0);
            int totalWritten = str.length() + 1;
            position += totalWritten;
            buf.position(writerIndex + totalWritten);
            return totalWritten;
        }
    }

    private static boolean allSingleByteChars(long fourChars) {
        return (fourChars & 0xFF00FF00FF00FF00L) == 0;
    }

    private static int tryWriteAsciiChars(String str, boolean checkForNullCharacters,
            long oddChars, long evenChars, io.netty.buffer.ByteBuf nettyByteBuf, int writerIndex, int i) {
        // all single byte chars
        long latinChars = oddChars << 8 | evenChars;
        if (checkForNullCharacters) {
            validateNoNullSingleByteChars(str, latinChars, i);
        }
        long msbSetForNonAscii = latinChars & 0x8080808080808080L;
        int firstNonAsciiOffset = Long.numberOfTrailingZeros(msbSetForNonAscii) >> 3;
        // that's a bit cheating :P but later phases will patch the wrongly encoded ones
        nettyByteBuf.setLongLE(writerIndex + i, latinChars);
        i += firstNonAsciiOffset;
        return i;
    }

    private int writeCharactersOnArray(String str, boolean checkForNullCharacters, ByteBuf buf) {
        int i = 0;
        byte[] array = buf.array();
        int pos = buf.position();
        int len = str.length();
        for (; i < len; i++) {
            char c = str.charAt(i);
            if (checkForNullCharacters && c == 0x0) {
                throw new BsonSerializationException(format("BSON cstring '%s' is not valid because it contains a null character "
                        + "at index %d", str, i));
            }
            if (c >= 0x80) {
                break;
            }
            array[pos + i] = (byte) c;
        }
        if (i == len) {
            int total = len + 1;
            array[pos + len] = 0;
            position += total;
            buf.position(pos + total);
            return len + 1;
        }
        // ith character is not ASCII
        if (i > 0) {
            position += i;
            buf.position(pos + i);
        }
        return i + super.writeCharacters(str, i, checkForNullCharacters);
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
