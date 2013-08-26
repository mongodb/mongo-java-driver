/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.bson.ByteBuf;
import org.mongodb.annotations.ThreadSafe;

import java.util.List;

/**
 * A channel to a MongoDB server with blocking operations.
 * <p>
 * Implementations of this class are thread safe.  At a minimum, they must support concurrent calls to sendMessage and receiveMessage,
 * but at most one of each.  But multiple concurrent calls to either sendMessage or receiveMessage may block.
 * </p>
 *
 * @since 3.0
 */
@ThreadSafe
public interface Channel {

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     * <p>
     * This method blocks until all bytes have been written.  This method is not thread safe: only one thread at a time can have an active
     * call to this method.
     * </p>
     *
     * @param byteBuffers the list of byte buffers to send.
     */
    void sendMessage(final List<ByteBuf> byteBuffers);

    /**
     * Receive a response to a sent message from the server.
     * <p>
     * This method blocks until the entire message has been read. This method is not thread safe: only one thread at a time can have an
     * active call to this method.
     * </p>
     *
     * @param channelReceiveArgs the settings that the response should conform to.
     * @return the response
     */
    ResponseBuffers receiveMessage(final ChannelReceiveArgs channelReceiveArgs);

    /**
     * Gets the server address of this connection
     */
    ServerAddress getServerAddress();


    /**
     * Gets the id of the connection.  If possible, this id will correlate with the connection id that the server puts in its log messages.
     * @return the id
     */
    String getId();

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
