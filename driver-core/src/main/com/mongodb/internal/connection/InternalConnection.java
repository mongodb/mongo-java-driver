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

import com.mongodb.RequestContext;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;

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
     * Opens the connection so its ready for use
     */
    void open();

    /**
     * Opens the connection so its ready for use
     *
     * @param callback the callback to be called once the connection has been opened
     */
    void openAsync(SingleResultCallback<Void> callback);

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
     *  @param message   the command message to send
     * @param sessionContext the session context
     * @param requestContext the request context
     */
    <T> T sendAndReceive(CommandMessage message, Decoder<T> decoder, SessionContext sessionContext, RequestContext requestContext);

    <T> void send(CommandMessage message, Decoder<T> decoder, SessionContext sessionContext);

    <T> T receive(Decoder<T> decoder, SessionContext sessionContext);


    default boolean supportsAdditionalTimeout() {
        return false;
    }

    default <T> T receive(Decoder<T> decoder, SessionContext sessionContext, int additionalTimeout) {
        throw new UnsupportedOperationException();
    }

    boolean hasMoreToCome();

    /**
     * Send a command message to the server.
     *
     * @param message   the command message to send
     * @param sessionContext the session context
     * @param callback the callback
     */
    <T> void sendAndReceiveAsync(CommandMessage message, Decoder<T> decoder, SessionContext sessionContext, RequestContext requestContext,
                                 SingleResultCallback<T> callback);

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send.
     * @param lastRequestId the request id of the last message in byteBuffers
     */
    void sendMessage(List<ByteBuf> byteBuffers, int lastRequestId);

    /**
     * Receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @return the response
     */
    ResponseBuffers receiveMessage(int responseTo);

    /**
     * Asynchronously send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param callback      the callback to invoke on completion
     */
    void sendMessageAsync(List<ByteBuf> byteBuffers, int lastRequestId, SingleResultCallback<Void> callback);

    /**
     * Asynchronously receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param callback the callback to invoke on completion
     */
    void receiveMessageAsync(int responseTo, SingleResultCallback<ResponseBuffers> callback);

    default void markAsPinned(Connection.PinningMode pinningMode) {
    }
}
