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

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface InternalConnection extends BufferProvider {

    int NOT_INITIALIZED_GENERATION = -1;

    /**
     * Gets the description of this connection.
     *
     * @return the connection description
     */
    ConnectionDescription getDescription();

    /**
     * Get the initial server description
     *
     * @return the initial server description
     */
    ServerDescription getInitialServerDescription();

    /**
     * Opens the connection so its ready for use. Will perform a handshake.
     *
     * @param operationContext the operation context
     */
    void open(OperationContext operationContext);

    /**
     * Opens the connection so its ready for use
     *
     * @param operationContext the operation context
     * @param callback         the callback to be called once the connection has been opened
     */
    void openAsync(OperationContext operationContext, SingleResultCallback<Void> callback);

    /**
     * Closes the connection.
     */
    void close();

    /**
     * Returns if the connection has been opened
     *
     * @return true if connection has been opened
     */
    boolean opened();

    /**
     * Returns the closed state of the connection
     *
     * @return true if connection is closed
     */
    boolean isClosed();

    /**
     * Gets the generation of this connection.  This can be used by connection pools to track whether the connection is stale.
     *
     * @return the generation
     */
    int getGeneration();

    /**
     * Send a command message to the server.
     *
     * @param message          the command message to send
     * @param operationContext the operation context
     */
    @Nullable
    <T> T sendAndReceive(CommandMessage message, Decoder<T> decoder, OperationContext operationContext);

    <T> void send(CommandMessage message, Decoder<T> decoder, OperationContext operationContext);

    <T> T receive(Decoder<T> decoder, OperationContext operationContext);

    boolean hasMoreToCome();

    /**
     * Send a command message to the server.
     *
     * @param message          the command message to send
     * @param callback         the callback
     */
    <T> void sendAndReceiveAsync(CommandMessage message, Decoder<T> decoder, OperationContext operationContext, SingleResultCallback<T> callback);

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send.
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param operationContext the operation context
     */
    void sendMessage(List<ByteBuf> byteBuffers, int lastRequestId, OperationContext operationContext);

    /**
     * Receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param operationContext the operation context
     * @return the response
     */
    ResponseBuffers receiveMessage(int responseTo, OperationContext operationContext);

    /**
     * Asynchronously send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param operationContext the operation context
     * @param callback      the callback to invoke on completion
     */
    void sendMessageAsync(List<ByteBuf> byteBuffers, int lastRequestId, OperationContext operationContext,
            SingleResultCallback<Void> callback);

    /**
     * Asynchronously receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param operationContext the operation context
     * @param callback the callback to invoke on completion
     */
    void receiveMessageAsync(int responseTo, OperationContext operationContext, SingleResultCallback<ResponseBuffers> callback);

    default void markAsPinned(Connection.PinningMode pinningMode) {
    }
}
