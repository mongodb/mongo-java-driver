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
 * An event signifying that a message has been sent on a connection.
 *
 * @since 3.0
 */
public class ConnectionMessageSentEvent extends ConnectionEvent {
    private final int requestId;
    private final int size;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     * @param serverAddress the server address
     * @param connectionId the connection id
     * @param requestId the request id
     * @param size the size of the sent message
     */
    public ConnectionMessageSentEvent(final String clusterId, final ServerAddress serverAddress, final String connectionId,
                                      final int requestId, final int size) {
        super(clusterId, serverAddress, connectionId);
        this.requestId = requestId;
        this.size = size;
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

        final ConnectionMessageSentEvent that = (ConnectionMessageSentEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!getServerAddress().equals(that.getServerAddress())) {
            return false;
        }
        if (!getConnectionId().equals(that.getConnectionId())) {
            return false;
        }
        if (requestId != that.requestId) {
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
        result = 31 * result + requestId;
        result = 31 * result + size;
        return result;
    }
}
