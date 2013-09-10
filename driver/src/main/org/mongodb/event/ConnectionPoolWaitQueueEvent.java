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
 * An event related to the connection pool's wait queue..
 *
 * @since 3.0
 */
public class ConnectionPoolWaitQueueEvent extends ConnectionPoolEvent {
    private final long threadId;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     * @param serverAddress the server address
     * @param threadId the identifier of the waiting thread
     */
    public ConnectionPoolWaitQueueEvent(final String clusterId, final ServerAddress serverAddress, final long threadId) {
        super(clusterId, serverAddress);
        this.threadId = threadId;
    }

    /**
     * Gets the identifier of the waiting thread.
     *
     * @return the thread id
     */
    public long getThreadId() {
        return threadId;
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

        final ConnectionPoolWaitQueueEvent that = (ConnectionPoolWaitQueueEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!getServerAddress().equals(that.getServerAddress())) {
            return false;
        }
        if (threadId != that.threadId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }
}

