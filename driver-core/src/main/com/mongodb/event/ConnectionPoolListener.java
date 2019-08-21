/*
 * Copyright 2008-present MongoDB, Inc.
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

import java.util.EventListener;

/**
 * A listener for connection pool-related events.
 *
 * @since 3.5
 */
public interface ConnectionPoolListener extends EventListener {
    /**
     * Invoked when a connection pool is opened.
     *
     * @param event the event
     */
    default void connectionPoolOpened(ConnectionPoolOpenedEvent event) {
    }

    /**
     * Invoked when a connection pool is closed.
     *
     * @param event the event
     */
    default void connectionPoolClosed(ConnectionPoolClosedEvent event) {
    }

    /**
     * Invoked when a connection is checked out of a pool.
     *
     * @param event the event
     */
    default void connectionCheckedOut(ConnectionCheckedOutEvent event) {
    }

    /**
     * Invoked when a connection is checked in to a pool.
     *
     * @param event the event
     */
    default void connectionCheckedIn(ConnectionCheckedInEvent event) {
    }

    /**
     * Invoked when a connection pool's wait queue is entered.
     *
     * @param event the event
     * @deprecated In the next major release, this event will no longer be published
     */
    @Deprecated
    default void waitQueueEntered(ConnectionPoolWaitQueueEnteredEvent event) {
    }

    /**
     * Invoked when a connection pools wait queue is exited.
     *
     * @param event the event
     * @deprecated In the next major release, this event will no longer be published
     */
    @Deprecated
    default void waitQueueExited(ConnectionPoolWaitQueueExitedEvent event) {
    }

    /**
     * Invoked when a connection is added to a pool.
     *
     * @param event the event
     */
    default void connectionAdded(ConnectionAddedEvent event) {
    }

    /**
     * Invoked when a connection is removed from a pool.
     *
     * @param event the event
     */
    default void connectionRemoved(ConnectionRemovedEvent event) {
    }
}
