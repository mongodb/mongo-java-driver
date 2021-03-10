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
 * Methods of this interface must not throw {@link Exception}s.
 *
 * @since 3.5
 */
public interface ConnectionPoolListener extends EventListener {
    /**
     * Invoked when a connection pool is opened. The default implementation does nothing.
     *
     * @param event the event
     * @deprecated Prefer {@link #connectionPoolCreated} Implementations should NOT implement this method at all, instead relying on
     * the default no-op implementation. If an application implements both this method and connectionPoolCreated, the application risks
     * double-counting events.
     */
    @Deprecated
    default void connectionPoolOpened(ConnectionPoolOpenedEvent event) {
    }

    /**
     * Invoked when a connection pool is created. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
    }

    /**
     * Invoked when a connection pool is cleared. The default implementation does nothing.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCleared(ConnectionPoolClearedEvent event) {
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
     * Invoked when a connection is added to a pool. The default implementation does nothing.
     *
     * @param event the event
     * @deprecated Prefer {@link #connectionCreated} Implementations should NOT implement this method at all, instead relying on
     * the default no-op implementation. If an application implements both this method and connectionCreated, the application risks
     * double-counting events.
     */
    @Deprecated
    default void connectionAdded(ConnectionAddedEvent event) {
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
     * @deprecated Prefer {@link #connectionClosed} Implementations should NOT implement this method at all, instead relying on
     * the default no-op implementation. If an application implements both this method and connectionClosed, the application risks
     * double-counting events.
     */
    @Deprecated
    default void connectionRemoved(ConnectionRemovedEvent event) {
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
