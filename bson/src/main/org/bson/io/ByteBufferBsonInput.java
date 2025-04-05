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

import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.types.ObjectId;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static java.lang.String.format;
import static org.bson.internal.PlatformUtil.isUnalignedAccessAllowed;

/**
 * An implementation of {@code BsonInput} that is backed by a {@code ByteBuf}.
 *
 * @since 3.0
 */
public class ByteBufferBsonInput implements BsonInput {

    private static final String[] ONE_BYTE_ASCII_STRINGS = new String[Byte.MAX_VALUE + 1];
    private static final boolean UNALIGNED_ACCESS_SUPPORTED = isUnalignedAccessAllowed();
    private int scratchBufferSize = 0;
    private byte[] scratchBuffer;


    static {
        for (int b = 0; b < ONE_BYTE_ASCII_STRINGS.length; b++) {
            ONE_BYTE_ASCII_STRINGS[b] = String.valueOf((char) b);
        }
    }

    private ByteBuf buffer;

    /**
     * Construct an instance with the given byte buffer.  The stream takes over ownership of the buffer and closes it when this instance is
     * closed.
     *
     * @param buffer the byte buffer
     */
    public ByteBufferBsonInput(final ByteBuf buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer can not be null");
        }
        this.buffer = buffer;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public int getPosition() {
        ensureOpen();
        return buffer.position();
    }


    @Override
    public byte readByte() {
        ensureOpen();
        ensureAvailable(1);
        return buffer.get();
    }

    @Override
    public void readBytes(final byte[] bytes) {
        ensureOpen();
        ensureAvailable(bytes.length);
        buffer.get(bytes);
    }

    @Override
    public void readBytes(final byte[] bytes, final int offset, final int length) {
        ensureOpen();
        ensureAvailable(length);
        buffer.get(bytes, offset, length);
    }

    @Override
    public long readInt64() {
        ensureOpen();
        ensureAvailable(8);
        return buffer.getLong();
    }

    @Override
    public double readDouble() {
        ensureOpen();
        ensureAvailable(8);
        return buffer.getDouble();
    }

    @Override
    public int readInt32() {
        ensureOpen();
        ensureAvailable(4);
        return buffer.getInt();
    }

    @Override
    public ObjectId readObjectId() {
        ensureOpen();
        byte[] bytes = new byte[12];
        readBytes(bytes);
        return new ObjectId(bytes);
    }

    @Override
    public String readString() {
        ensureOpen();
        int size = readInt32();
        if (size <= 0) {
            throw new BsonSerializationException(format("While decoding a BSON string found a size that is not a positive number: %d",
                    size));
        }
        ensureAvailable(size);
        return readString(size);
    }

    @Override
    public String readCString() {
        int size = computeCStringLength(buffer.position());
        return readString(size);
    }

    private String readString(final int stringSize) {
        if (stringSize == 2) {
            byte asciiByte = buffer.get();               // if only one byte in the string, it must be ascii.
            byte nullByte = buffer.get();                // read null terminator
            if (nullByte != 0) {
                throw new BsonSerializationException("Found a BSON string that is not null-terminated");
            }
            if (asciiByte < 0) {
                return StandardCharsets.UTF_8.newDecoder().replacement();
            }
            return ONE_BYTE_ASCII_STRINGS[asciiByte];  // this will throw if asciiByte is negative
        } else {
            if (buffer.isBackedByArray()) {
                int position = buffer.position();
                int arrayOffset = buffer.arrayOffset();
                int newPosition = position + stringSize;
                buffer.position(newPosition);

                byte[] array = buffer.array();
                if (array[arrayOffset + newPosition - 1] != 0) {
                    throw new BsonSerializationException("Found a BSON string that is not null-terminated");
                }
                return new String(array, arrayOffset + position, stringSize - 1, StandardCharsets.UTF_8);
            } else if (stringSize > scratchBufferSize) {
                scratchBufferSize = stringSize + (stringSize >> 1); //1.5 times the size
                scratchBuffer = new byte[scratchBufferSize];
            }

            buffer.get(scratchBuffer, 0, stringSize);
            if (scratchBuffer[stringSize - 1] != 0) {
                throw new BsonSerializationException("BSON string not null-terminated");
            }
            return new String(scratchBuffer, 0, stringSize - 1, StandardCharsets.UTF_8);
        }
    }

    @Override
    public void skipCString() {
        ensureOpen();
        int pos = buffer.position();
        int length = computeCStringLength(pos);
        buffer.position(pos + length);
    }

    public int computeCStringLength(final int prevPos) {
        ensureOpen();
        int pos = buffer.position();
        int limit = buffer.limit();

        if (UNALIGNED_ACCESS_SUPPORTED) {
            int chunks = (limit - pos) >>> 3;
            // Process 8 bytes at a time.
            for (int i = 0; i < chunks; i++) {
                long word = buffer.getLong(pos);
                long mask = word - 0x0101010101010101L;
                mask &= ~word;
                mask &= 0x8080808080808080L;
                if (mask != 0) {
                    // first null terminator found in the Little Endian long
                    int offset = Long.numberOfTrailingZeros(mask) >>> 3;
                    // Found the null at pos + offset; reset buffer's position.
                    return (pos - prevPos) + offset + 1;
                }
                pos += 8;
            }
        }

        // Process remaining bytes one-by-one.
        while (pos < limit) {
            if (buffer.get(pos++) == 0) {
                return (pos - prevPos);
            }
        }

        buffer.position(pos);
        throw new BsonSerializationException("Found a BSON string that is not null-terminated");
    }

    @Override
    public void skip(final int numBytes) {
        ensureOpen();
        buffer.position(buffer.position() + numBytes);
    }

    @Override
    public BsonInputMark getMark(final int readLimit) {
        return new BsonInputMark() {
            private final int mark = buffer.position();
            @Override
            public void reset() {
                ensureOpen();
                buffer.position(mark);
            }
        };
    }

    @Override
    public boolean hasRemaining() {
        ensureOpen();
        return buffer.hasRemaining();
    }

    @Override
    public void close() {
        buffer.release();
        buffer = null;
    }

    private void ensureOpen() {
        if (buffer == null) {
            throw new IllegalStateException("Stream is closed");
        }
    }
    private void ensureAvailable(final int bytesNeeded) {
        if (buffer.remaining() < bytesNeeded) {
            throw new BsonSerializationException(format("While decoding a BSON document %d bytes were required, "
                                                        + "but only %d remain", bytesNeeded, buffer.remaining()));
        }
    }
}
