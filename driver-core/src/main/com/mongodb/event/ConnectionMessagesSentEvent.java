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
 * An event signifying that a message has been sent on a connection.
 *
 * @deprecated - No longer used
 */
@Beta
@Deprecated
public final class ConnectionMessagesSentEvent {
    private final ConnectionId connectionId;
    private final int requestId;
    private final int size;

    /**
     * Constructs a new instance of the event.
     *
     * @param connectionId  the connection id
     * @param requestId     the request id
     * @param size          the size of the sent message
     */
    public ConnectionMessagesSentEvent(final ConnectionId connectionId,
                                       final int requestId, final int size) {
        this.connectionId = notNull("connectionId", connectionId);
        this.requestId = requestId;
        this.size = size;
    }

    /**
     * Gets the identifier for this connection.
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the request id of the message that was sent.
     *
     * @return the request id
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Gets the size of the sent message.
     *
     * @return the size of the sent message
     */
    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "ConnectionMessagesSentEvent{"
                       + "requestId=" + requestId
                       + ", size=" + size
                       + ", connectionId=" + connectionId
                       + '}';
    }
}
