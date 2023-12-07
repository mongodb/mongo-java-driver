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

import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import org.bson.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * A full duplex stream of bytes.
 */
public interface Stream extends BufferProvider {

    /**
     * Open the stream.
     *
     * @param operationContext the operation context
     * @throws IOException if an I/O error occurs
     */
    void open(OperationContext operationContext) throws IOException;

    /**
     * Open the stream asynchronously.
     *
     * @param operationContext the operation context
     * @param handler          the completion handler for opening the stream
     */
    void openAsync(OperationContext operationContext, AsyncCompletionHandler<Void> handler);

    /**
     * Write each buffer in the list to the stream in order, blocking until all are completely written.
     *
     * @param buffers the buffers to write. The operation must not {@linkplain ByteBuf#release() release} any buffer from {@code buffers},
     * unless the operation {@linkplain ByteBuf#retain() retains} it, and releasing is meant to compensate for that.
     * @param operationContext the operation context
     * @throws IOException if there are problems writing to the stream
     */
    void write(List<ByteBuf> buffers, OperationContext operationContext) throws IOException;

    /**
     * Read from the stream, blocking until the requested number of bytes have been read.
     *
     * @param numBytes The number of bytes to read into the returned byte buffer
     * @param operationContext the operation context
     * @return a byte buffer filled with number of bytes requested
     * @throws IOException if there are problems reading from the stream
     */
    ByteBuf read(int numBytes, OperationContext operationContext) throws IOException;

    /**
     * Read from the stream, blocking until the requested number of bytes have been read.  If supported by the implementation,
     * adds the given additional timeout to the configured timeout for the stream.
     *
     * @param numBytes The number of bytes to read into the returned byte buffer
     * @param additionalTimeout additional timeout in milliseconds to add to the configured timeout
     * @param operationContext the operation context
     * @return a byte buffer filled with number of bytes requested
     * @throws IOException if there are problems reading from the stream
     */
    ByteBuf read(int numBytes, int additionalTimeout, OperationContext operationContext) throws IOException;

    /**
     * Write each buffer in the list to the stream in order, asynchronously.  This method should return immediately, and invoke the given
     * callback on completion.
     *
     * @param buffers the buffers to write. The operation must not {@linkplain ByteBuf#release() release} any buffer from {@code buffers},
     * unless the operation {@linkplain ByteBuf#retain() retains} it, and releasing is meant to compensate for that.
     * @param operationContext the operation context
     * @param handler invoked when the write operation has completed
     */
    void writeAsync(List<ByteBuf> buffers, OperationContext operationContext, AsyncCompletionHandler<Void> handler);

    /**
     * Read from the stream, asynchronously.  This method should return immediately, and invoke the given callback when the number of
     * requested bytes have been read.
     *
     * @param numBytes the number of bytes
     * @param operationContext the operation context
     * @param handler invoked when the read operation has completed
     */
    void readAsync(int numBytes, OperationContext operationContext, AsyncCompletionHandler<ByteBuf> handler);

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
