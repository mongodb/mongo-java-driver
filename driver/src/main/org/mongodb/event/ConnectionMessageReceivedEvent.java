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

package org.mongodb.event;

import org.mongodb.connection.ServerAddress;

/**
 * An event signifying that a message has been received on a connection.
 *
 * @since 3.0
 */
public class ConnectionMessageReceivedEvent extends ConnectionEvent {
    private final int responseTo;
    private final int size;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     * @param serverAddress the server address
     * @param connectionId the connection id
     * @param responseTo the request id that this message is in response to
     * @param size the size of the received message
     */
    public ConnectionMessageReceivedEvent(final String clusterId, final ServerAddress serverAddress, final String connectionId,
                                          final int responseTo, final int size) {
        super(clusterId, serverAddress, connectionId);
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final ConnectionMessageReceivedEvent that = (ConnectionMessageReceivedEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!getServerAddress().equals(that.getServerAddress())) {
            return false;
        }
        if (!getConnectionId().equals(that.getConnectionId())) {
            return false;
        }
        if (responseTo != that.responseTo) {
            return false;
        }
        if (size != that.size) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + responseTo;
        result = 31 * result + size;
        return result;
    }
}
