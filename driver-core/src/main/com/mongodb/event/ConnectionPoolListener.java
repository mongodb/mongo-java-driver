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
     * Invoked when a connection pool is created. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
    }

    /**
     * Invoked when a connection pool is cleared and paused. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCleared(ConnectionPoolClearedEvent event) {
    }

    /**
     * Invoked when a connection pool is ready. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.3
     */
    default void connectionPoolReady(ConnectionPoolReadyEvent event) {
    }

    /**
     * Invoked when a connection pool is closed. The default implementation does nothing.
     *
     * @param event the event
     */
    default void connectionPoolClosed(ConnectionPoolClosedEvent event) {
    }

    /**
     * Invoked when attempting to check out a connection from a pool. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCheckOutStarted(ConnectionCheckOutStartedEvent event) {
    }

    /**
     * Invoked when a connection is checked out of a pool. The default implementation does nothing.
     *
     * @param event the event
     */
    default void connectionCheckedOut(ConnectionCheckedOutEvent event) {
    }

    /**
     * Invoked when an attempt to check out a connection from a pool fails. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
    }

    /**
     * Invoked when a connection is checked in to a pool. The default implementation does nothing.
     *
     * @param event the event
     */
    default void connectionCheckedIn(ConnectionCheckedInEvent event) {
    }

    /**
     * Invoked when a connection is created. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCreated(ConnectionCreatedEvent event) {
    }

    /**
     * Invoked when a connection is ready for use. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionReady(ConnectionReadyEvent event) {
    }

    /**
     * Invoked when a connection is removed from a pool. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionClosed(ConnectionClosedEvent event) {
    }
}
