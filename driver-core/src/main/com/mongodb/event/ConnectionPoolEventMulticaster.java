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

import com.mongodb.annotations.Beta;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * A multicaster for connection pool events.
 *
 * @deprecated register multiple command listeners instead
 */
@Beta
@Deprecated
public final class ConnectionPoolEventMulticaster implements ConnectionPoolListener {
    private final Set<ConnectionPoolListener> connectionPoolListeners
        = newSetFromMap(new ConcurrentHashMap<ConnectionPoolListener, Boolean>());

    /**
     * Adds the given connection pool listener to the list of listeners to invoke on connection pool events.
     *
     * @param connectionPoolListener the connection pool listener
     */
    public void add(final ConnectionPoolListener connectionPoolListener) {
        connectionPoolListeners.add(connectionPoolListener);
    }

    /**
     * Removes the given connection pool listener from the list of listeners to invoke on connection pool events.
     *
     * @param connectionPoolListener the connection pool listener
     */
    public void remove(final ConnectionPoolListener connectionPoolListener) {
        connectionPoolListeners.remove(connectionPoolListener);
    }

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionPoolOpened(event);
        }
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionPoolClosed(event);
        }
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionCheckedOut(event);
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionCheckedIn(event);
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.waitQueueEntered(event);
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.waitQueueExited(event);
        }
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionAdded(event);
        }
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        for (final ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionRemoved(event);
        }
    }
}
