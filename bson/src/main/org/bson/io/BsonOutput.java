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
 * An output stream that is optimized for writing BSON values directly to the underlying stream.
 *
 * @since 3.0
 */

public interface BsonOutput extends Closeable {

    /**
     * Gets the current position in the stream.
     *
     * @return the current position
     */
    int getPosition();

    /**
     * Gets the current size of the stream in number of bytes.
     *
     * @return the size of the stream
     */
    int getSize();

    /**
     * Truncates this stream to the new position.  After this call, both size and position will equal the new position.
     * @param newPosition the new position, which must be greater than or equal to 0 and less than the current size.
     */
    void truncateToPosition(int newPosition);

    /**
     * Writes all the bytes in the byte array to the stream.
     * @param bytes the non-null byte array
     */
    void writeBytes(byte[] bytes);

    /**
     * Writes {@code length} bytes from the byte array, starting at {@code offset}.
     * @param bytes the non-null byte array
     * @param offset the offset to start writing from
     * @param length the number of bytes to write
     */
    void writeBytes(byte[] bytes, int offset, int length);

    /**
     * Write a single byte to the stream. The byte to be written is the eight low-order bits of the specified value. The 24
     * high-order bits of the value are ignored.
     *
     * @param value the value
     */
    void writeByte(int value);

    /**
     * Writes a BSON CString to the stream.
     *
     * @param value the value
     */
    void writeCString(String value);

    /**
     * Writes a BSON String to the stream.
     *
     * @param value the value
     */
    void writeString(String value);

    /**
     * Writes a BSON double to the stream.
     *
     * @param value the value
     */
    void writeDouble(double value);

    /**
     * Writes a 32-bit BSON integer to the stream.
     *
     * @param value the value
     */
    void writeInt32(int value);

    /**
     * Writes a 32-bit BSON integer to the stream at the given position.  This is useful for patching in the size of a document once the
     * last byte of it has been encoded and its size it known.
     *
     * @param position the position to write the value, which must be greater than or equal to 0 and less than or equal to the current size
     * @param value the value
     */
    void writeInt32(int position, int value);

    /**
     * Writes a 64-bit BSON integer to the stream.
     *
     * @param value the value
     */
    void writeInt64(long value);

    /**
     * Writes a BSON ObjectId to the stream.
     *
     * @param value the value
     */
    void writeObjectId(ObjectId value);

    @Override
    void close();
}
