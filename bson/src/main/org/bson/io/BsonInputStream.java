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

package org.bson.io;

import org.bson.BsonType;
import org.bson.types.ObjectId;

import java.io.Closeable;

/**
 * An input stream that is optimized for reading BSON values directly from the underlying stream.
 *
 * @since 3.0
 */
public interface BsonInputStream extends Closeable {
    /**
     * Gets the current position in the stream
     *
     * @return the current position
     */
    int getPosition();

    /**
     * Reads a BSON boolean value from the stream.
     *
     * @return the boolean value
     */
    boolean readBoolean();

    /**
     * Reads a single byte from the stream
     *
     * @return the byte value
     */
    byte readByte();

    /**
     * Reads the specified number of bytes into a byte array.
     *
     * @param size the number of bytes
     * @return the byte array
     */
    byte[] readBytes(int size);

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
     * Reads a BSON type from the stream.
     *
     * @return the BSON type
     */
    BsonType readBSONType();

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

    @Override
    void close();
}
