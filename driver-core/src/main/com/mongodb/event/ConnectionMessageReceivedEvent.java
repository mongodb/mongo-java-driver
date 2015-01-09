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

package com.mongodb.event;

import com.mongodb.annotations.Beta;
import com.mongodb.connection.ConnectionId;

/**
 * An event signifying that a message has been received on a connection.
 */
@Beta
public class ConnectionMessageReceivedEvent extends ConnectionEvent {
    private final int responseTo;
    private final int size;

    /**
     * Constructs a new instance of the event.
     *
     * @param connectionId  the connection id
     * @param responseTo    the request id that this message is in response to
     * @param size          the size of the received message
     */
    public ConnectionMessageReceivedEvent(final ConnectionId connectionId, final int responseTo, final int size) {
        super(connectionId);
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
}
