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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.BaseConnection;
import org.mongodb.connection.ResponseBuffers;

import java.util.List;

/**
 * A connection to a MongoDB server with blocking operations.
 * <p>
 * This class is not completely thread safe.  At most one thread can have an active call to sendMessage, and one thread an active call
 * to receiveMessage.
 * </p>
 *
 * @since 3.0
 */
@NotThreadSafe
interface Connection extends BaseConnection {

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
     * @return the response
     */
    ResponseBuffers receiveMessage();

    /**
     * Gets the id of the connection.  If possible, this id will correlate with the connection id that the server puts in its log messages.
     * @return the id
     */
    String getId();
}
