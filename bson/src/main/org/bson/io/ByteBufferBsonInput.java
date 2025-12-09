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

/**
 * An implementation of {@code BsonInput} that is backed by a {@code ByteBuf}.
 *
 * @since 3.0
 */
public class ByteBufferBsonInput implements BsonInput {

    private static final String[] ONE_BYTE_ASCII_STRINGS = new String[Byte.MAX_VALUE + 1];
    /* A dynamically sized scratch buffer, that is reused across BSON String reads:
     * 1. Reduces garbage collection by avoiding new byte array creation.
     * 2. Improves cache utilization through temporal locality.
     * 3. Avoids JVM allocation and zeroing cost for new memory allocations.
     */
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
        ensureOpen();
        int size = computeCStringLength(buffer.position());
        return readString(size);
    }

    private String readString(final int bsonStringSize) {
        if (bsonStringSize == 2) {
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
                int newPosition = position + bsonStringSize;
                buffer.position(newPosition);

                byte[] array = buffer.array();
                if (array[arrayOffset + newPosition - 1] != 0) {
                    throw new BsonSerializationException("Found a BSON string that is not null-terminated");
                }
                return new String(array, arrayOffset + position, bsonStringSize - 1, StandardCharsets.UTF_8);
            } else if (scratchBuffer == null || bsonStringSize > scratchBuffer.length) {
                int scratchBufferSize = bsonStringSize + (bsonStringSize >>> 1); //1.5 times the size
                scratchBuffer = new byte[scratchBufferSize];
            }

            buffer.get(scratchBuffer, 0, bsonStringSize);
            if (scratchBuffer[bsonStringSize - 1] != 0) {
                throw new BsonSerializationException("BSON string not null-terminated");
            }
            return new String(scratchBuffer, 0, bsonStringSize - 1, StandardCharsets.UTF_8);
        }
    }

    @Override
    public void skipCString() {
        ensureOpen();
        int pos = buffer.position();
        int length = computeCStringLength(pos);
        buffer.position(pos + length);
    }

    /**
     * Detects the position of the first NULL (0x00) byte in a 64-bit word using SWAR technique.
     * <a href="https://en.wikipedia.org/wiki/SWAR">
     */
    private int computeCStringLength(final int prevPos) {
        int pos = prevPos;
        int limit = buffer.limit();

        // `>>> 3` means dividing without remainder by `Long.BYTES` because `Long.BYTES` is 2^3
        int chunks = (limit - pos) >>> 3;
        // `<< 3` means multiplying by `Long.BYTES` because `Long.BYTES` is 2^3
        int toPos = pos + (chunks << 3);
        for (; pos < toPos; pos += Long.BYTES) {
            long chunk = buffer.getLong(pos);
            /*
              Subtract 0x0101010101010101L to cause a borrow on 0x00 bytes.
              if original byte is 00000000, then 00000000 - 00000001 = 11111111 (borrow causes the most significant bit set to 1).
             */
            long mask = chunk - 0x0101010101010101L;
            /*
              mask will only have the most significant bit in each byte set iff it was a 0x00 byte (0x00 becomes 0xFF because of the borrow).
              ~chunk will have bits that were originally 0 set to 1.
              mask & ~chunk will have the most significant bit in each byte set iff original byte was 0x00.
             */
            mask &= ~chunk;
            /*
               0x8080808080808080:
               10000000 10000000 10000000 10000000 10000000 10000000 10000000 10000000

               mask:
               00000000 00000000 11111111 00000000 00000001 00000001 00000000 00000111

               ANDing mask with 0x8080808080808080 isolates the most significant bit in each byte where
               the original byte was 0x00, thereby setting the most significant bit to 1 in each 0x00 original byte.

               result:
               00000000 00000000 10000000 00000000 00000000 00000000 00000000 00000000
                                 ^^^^^^^^
               The most significant bit is set in each 0x00 byte, and only there.
             */
            mask &= 0x8080808080808080L;
            if (mask != 0) {
                /*
                The UTF-8 data is endian-independent and stored in left-to-right order in the buffer, with the first byte at the lowest index.
                After calling getLong() in little-endian mode, the first UTF-8 byte ends up in the least significant byte of the long (bits 0–7),
                and the last one in the most significant byte (bits 56–63).

                numberOfTrailingZeros scans from the least significant bit, which aligns with the position of the first UTF-8 byte.
                We then use >>> 3, which means dividing without remainder by Long.BYTES because Long.BYTES is 2^3, computing the byte offset
                of the NULL terminator in the original UTF-8 data.
                 */
                int offset = Long.numberOfTrailingZeros(mask) >>> 3;
                // Find the NULL terminator at pos + offset
                return (pos - prevPos) + offset + 1;
            }
        }

        // Process remaining bytes one by one.
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
