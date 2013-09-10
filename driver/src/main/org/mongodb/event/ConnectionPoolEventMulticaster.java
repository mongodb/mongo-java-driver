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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A multicaster for connection pool events.
 *
 * @since 3.0
 */
public class ConnectionPoolEventMulticaster implements ConnectionPoolListener {
    private Set<ConnectionPoolListener> connectionPoolListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ConnectionPoolListener, Boolean>());

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
    public void connectionPoolOpened(final ConnectionPoolEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionPoolOpened(event);
        }
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionPoolClosed(event);
        }
    }

    @Override
    public void connectionCheckedOut(final ConnectionEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionCheckedOut(event);
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionCheckedIn(event);
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.waitQueueEntered(event);
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.waitQueueExited(event);
        }
    }

    @Override
    public void connectionAdded(final ConnectionEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionAdded(event);
        }
    }

    @Override
    public void connectionRemoved(final ConnectionEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            cur.connectionRemoved(event);
        }
    }
}
