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
        int mark = buffer.position();
        skipCString();
        int size = buffer.position() - mark;
        buffer.position(mark);
        return readString(size);
    }

    private String readString(final int size) {
        if (size == 2) {
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
            byte[] bytes = new byte[size - 1];
            buffer.get(bytes);
            byte nullByte = buffer.get();
            if (nullByte != 0) {
                throw new BsonSerializationException("Found a BSON string that is not null-terminated");
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    @Override
    public void skipCString() {
        ensureOpen();
        int indexOfZero = buffer.indexOf((byte) 0);
        if (indexOfZero == -1) {
            buffer.position(buffer.limit());
            throw new BsonSerializationException("Found a BSON string that is not null-terminated");
        }
        buffer.position(indexOfZero + 1);
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
