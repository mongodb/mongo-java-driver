/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.event;

import com.mongodb.annotations.Beta;
import com.mongodb.connection.ServerId;

/**
 * An event for entering the wait queue of the connection pool.
 */
@Beta
public final class ConnectionPoolWaitQueueEnteredEvent {
    private final ServerId serverId;
    private final long threadId;

    /**
     * Construct an instance.
     *
     * @param serverId the server id
     * @param threadId the id of the thread that's waiting
     */
    public ConnectionPoolWaitQueueEnteredEvent(final ServerId serverId, final long threadId) {
        this.serverId = serverId;
        this.threadId = threadId;
    }

    /**
     * Gets the server id.
     *
     * @return the server id
     */
    public ServerId getServerId() {
        return serverId;
    }

    /**
     * Gets the thread id
     * @return the thread id
     */
    public long getThreadId() {
        return threadId;
    }

    @Override
    public String toString() {
        return "ConnectionPoolWaitQueueEnteredEvent{"
                       + "serverId=" + serverId
                       + ", threadId=" + threadId
                       + '}';
    }
}
