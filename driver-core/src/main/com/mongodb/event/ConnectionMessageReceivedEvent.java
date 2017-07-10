/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb.event;

import com.mongodb.annotations.Beta;
import com.mongodb.connection.ConnectionId;

import static org.bson.assertions.Assertions.notNull;

/**
 * An event signifying that a message has been received on a connection.
 *
 * @deprecated - No longer used
 */
@Beta
@Deprecated
public final class ConnectionMessageReceivedEvent {
    private final int responseTo;
    private final int size;
    private final ConnectionId connectionId;

    /**
     * Constructs a new instance of the event.
     *
     * @param connectionId  the connection id
     * @param responseTo    the request id that this message is in response to
     * @param size          the size of the received message
     */
    public ConnectionMessageReceivedEvent(final ConnectionId connectionId, final int responseTo, final int size) {
        this.connectionId = notNull("connectionId", connectionId);
        this.responseTo = responseTo;
        this.size = size;
    }

    /**
     * The responseTo identifier of the message.  This corresponds to the requestId of the message that this message is in reply to.
     *
     * @return the responseTo identifier
     */
    public int getResponseTo() {
        return responseTo;
    }

    /**
     * Gets the size of the received message.
     *
     * @return the size of the received message
     */
    public int getSize() {
        return size;
    }

    /**
     * Gets the identifier for this connection.
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "ConnectionMessageReceivedEvent{"
                       + "responseTo=" + responseTo
                       + ", size=" + size
                       + ", connectionId=" + connectionId
                       + '}';
    }
}
