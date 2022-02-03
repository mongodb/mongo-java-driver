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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import org.bson.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * A full duplex stream of bytes.
 *
 * @since 3.0
 */
public interface Stream extends BufferProvider{

    /**
     * Open the stream.
     *
     * @throws IOException if an I/O error occurs
     */
    void open() throws IOException;

    /**
     * Open the stream asynchronously.
     *
     * @param handler the completion handler for opening the stream
     */
    void openAsync(AsyncCompletionHandler<Void> handler);

    /**
     * Write each buffer in the list to the stream in order, blocking until all are completely written.
     *
     * @param buffers the buffers to write
     * @throws IOException if there are problems writing to the stream
     */
    void write(List<ByteBuf> buffers) throws IOException;

    /**
     * Read from the stream, blocking until the requested number of bytes have been read.
     *
     * @param numBytes The number of bytes to read into the returned byte buffer
     * @return a byte buffer filled with number of bytes requested
     * @throws IOException if there are problems reading from the stream
     */
    ByteBuf read(int numBytes) throws IOException;

    /**
     * Gets whether this implementation supports specifying an additional timeout for read operations
     * <p>
     * The default is to not support specifying an additional timeout
     * </p>
     *
     * @return true if this implementation supports specifying an additional timeouts for reads operations
     * @see #read(int, int)
     * @since 4.1
     */
    default boolean supportsAdditionalTimeout() {
        return false;
    }

    /**
     * Read from the stream, blocking until the requested number of bytes have been read.  If supported by the implementation,
     * adds the given additional timeout to the configured timeout for the stream.
     * <p>
     * This method should not be called unless {@link #supportsAdditionalTimeout()} returns true.
     * </p>
     * <p>
     * The default behavior is to throw an {@link UnsupportedOperationException}
     * </p>
     *
     * @param numBytes The number of bytes to read into the returned byte buffer
     * @param additionalTimeout additional timeout in milliseconds to add to the configured timeout
     * @return a byte buffer filled with number of bytes requested
     * @throws IOException if there are problems reading from the stream
     * @throws UnsupportedOperationException if this implementation does not support additional timeouts
     * @see #supportsAdditionalTimeout()
     * @since 4.1
     */
    default ByteBuf read(int numBytes, int additionalTimeout) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Write each buffer in the list to the stream in order, asynchronously.  This method should return immediately, and invoke the given
     * callback on completion.
     *
     * @param buffers the buffers to write
     * @param handler invoked when the write operation has completed
     */
    void writeAsync(List<ByteBuf> buffers, AsyncCompletionHandler<Void> handler);

    /**
     * Read from the stream, asynchronously.  This method should return immediately, and invoke the given callback when the number of
     * requested bytes have been read.
     *
     * @param numBytes the number of bytes
     * @param handler invoked when the read operation has completed
     */
    void readAsync(int numBytes, AsyncCompletionHandler<ByteBuf> handler);

    /**
     * The address that this stream is connected to.
     *
     * @return the address
     */
    ServerAddress getAddress();

    /**
     * Closes the connection.
     */
    void close();

    /**
     * Returns the closed state of the connection
     *
     * @return true if connection is closed
     */
    boolean isClosed();
}
