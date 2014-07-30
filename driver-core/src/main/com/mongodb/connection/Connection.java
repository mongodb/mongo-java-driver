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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.binding.ReferenceCounted;
import org.bson.ByteBuf;

import java.util.List;

/**
 * A connection to a MongoDB server with blocking and non-blocking operations. <p> Implementations of this class are thread safe.  At a
 * minimum, they must support concurrent calls to sendMessage and receiveMessage, but at most one of each.  But multiple concurrent calls to
 * either sendMessage or receiveMessage may block. </p>
 *
 * @since 3.0
 */
@ThreadSafe
public interface Connection extends BufferProvider, ReferenceCounted {

    @Override
    Connection retain();

    /**
     * Gets the description of the server that it's connected to.
     *
     * @return the server description
     */
    ServerDescription getServerDescription();

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message. <p> This method
     * blocks until all bytes have been written.  This method is not thread safe: only one thread at a time can have an active call to this
     * method. </p>
     *
     * @param byteBuffers   the list of byte buffers to send.
     * @param lastRequestId the request id of the last message in byteBuffers
     */
    void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId);

    /**
     * Receive a response to a sent message from the server. <p> This method blocks until the entire message has been read. This method is
     * not thread safe: only one thread at a time can have an active call to this method. </p>
     *
     * @param responseTo the expected responseTo of the received message
     * @return the response
     */
    ResponseBuffers receiveMessage(final int responseTo);

    /**
     * Asynchronously send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param callback      the callback to invoke on completion
     */
    void sendMessageAsync(List<ByteBuf> byteBuffers, final int lastRequestId, SingleResultCallback<Void> callback);

    /**
     * Asynchronously receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param callback   the callback to invoke on completion
     */
    void receiveMessageAsync(final int responseTo, SingleResultCallback<ResponseBuffers> callback);

    /**
     * Gets the server address of this connection
     */
    ServerAddress getServerAddress();

    /**
     * Gets the id of the connection.  If possible, this id will correlate with the connection id that the server puts in its log messages.
     *
     * @return the id
     */
    String getId();
}
