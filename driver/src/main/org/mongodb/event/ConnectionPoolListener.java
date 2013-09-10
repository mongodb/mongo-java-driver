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

import java.util.EventListener;

/**
 * A listener for connection pool-related events.
 *
 * @since 3.0
 */

public interface ConnectionPoolListener extends EventListener {
    /**
     * Invoked when a connection pool is opened.
     *
     * @param event the event
     */
    void connectionPoolOpened(ConnectionPoolEvent event);

    /**
     * Invoked when a connection pool is closed.
     *
     * @param event the event
     */
    void connectionPoolClosed(ConnectionPoolEvent event);

    /**
     * Invoked when a connection is checked out of a pool.
     *
     * @param event the event
     */
    void connectionCheckedOut(ConnectionEvent event);

    /**
     * Invoked when a connection is checked in to a pool.
     *
     * @param event the event
     */
    void connectionCheckedIn(ConnectionEvent event);

    /**
     * Invoked when a connection pool's wait queue is entered.
     *
     * @param event the event
     */
    void waitQueueEntered(ConnectionPoolWaitQueueEvent event);

    /**
     * Invoked when a connection pools wait queue is exited.
     *
     * @param event the event
     */
    void waitQueueExited(ConnectionPoolWaitQueueEvent event);

    /**
     * Invoked when a connection is added to a pool.
     *
     * @param event the event
     */
    void connectionAdded(ConnectionEvent event);

    /**
     * Invoked when a connection is removed from a pool.
     *
     * @param event the event
     */
    void connectionRemoved(ConnectionEvent event);
}
