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

import org.bson.types.ObjectId;

import java.io.Closeable;

/**
 * An input stream that is optimized for reading BSON values directly from the underlying stream.
 *
 * @since 3.0
 */
public interface BsonInput extends Closeable {
    /**
     * Gets the current position in the stream
     *
     * @return the current position
     */
    int getPosition();

    /**
     * Reads a single byte from the stream
     *
     * @return the byte value
     */
    byte readByte();

    /**
     * Reads the specified number of bytes into the given byte array. This is equivalent to to {@code readBytes(bytes, 0, bytes.length)}.
     *
     * @param bytes the byte array to write into
     */
    void readBytes(byte[] bytes);

    /**
     * Reads the specified number of bytes into the given byte array starting at the specified offset.
     *
     * @param bytes the byte array to write into
     * @param offset the offset to start writing
     * @param length the number of bytes to write
     */
    void readBytes(byte[] bytes, int offset, int length);

    /**
     * Reads a BSON Int64 value from the stream.
     *
     * @return the Int64 value
     */
    long readInt64();

    /**
     * Reads a BSON Double value from the stream.
     *
     * @return the double value
     */
    double readDouble();

    /**
     * Reads a BSON Int32 value from the stream.
     *
     * @return the Int32 value
     */
    int readInt32();

    /**
     * Reads a BSON String value from the stream.
     *
     * @return the string
     */
    String readString();

    /**
     * Reads a BSON ObjectId value from the stream.
     *
     * @return the ObjectId
     */
    ObjectId readObjectId();

    /**
     * Reads a BSON CString value from the stream.
     *
     * @return the CString
     */
    String readCString();

    /**
     * Skips a BSON CString value from the stream.
     *
     */
    void skipCString();

    /**
     * Skips the specified number of bytes in the stream.
     *
     * @param numBytes the number of bytes to skip
     */
    void skip(int numBytes);

    /**
     * Marks the current position in the stream. This method obeys the contract as specified in the same method in {@code InputStream}.
     *
     * @param readLimit the maximum limit of bytes that can be read before the mark position becomes invalid
     * @deprecated Use {@link #getMark(int)} instead
     */
    @Deprecated
    void mark(int readLimit);

    /**
     * Gets a mark for the current position in the stream.
     *
     * @param readLimit the maximum limit of bytes that can be read before the mark position becomes invalid
     * @return the mark
     * @since 3.7
     */
    BsonInputMark getMark(int readLimit);

    /**
     * Resets the stream to the current mark. This method obeys the contract as specified in the same method in {@code InputStream}.
     * @deprecated Prefer {@link #getMark(int)}
     */
    @Deprecated
    void reset();

    /**
     * Returns true if there are more bytes left in the stream.
     *
     * @return true if there are more bytes left in the stream.
     */
    boolean hasRemaining();

    @Override
    void close();
}
