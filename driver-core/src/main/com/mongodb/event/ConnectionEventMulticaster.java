/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 * A multicaster for connection events.
 */
@Beta
public final class ConnectionEventMulticaster implements ConnectionListener {
    private final Set<ConnectionListener> connectionListeners = newSetFromMap(new ConcurrentHashMap<ConnectionListener, Boolean>());

    /**
     * Adds the given connection listener to the list of listeners to invoke on connection events.
     *
     * @param connectionListener the connection listener
     */
    public void add(final ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    /**
     * Removes the given connection listener from the list of listeners to invoke on connection events.
     *
     * @param connectionListener the connection listener
     */
    public void remove(final ConnectionListener connectionListener) {
        connectionListeners.remove(connectionListener);
    }

    @Override
    public void connectionOpened(final ConnectionOpenedEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            cur.connectionOpened(event);
        }
    }

    @Override
    public void connectionClosed(final ConnectionClosedEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            cur.connectionClosed(event);
        }
    }

    @Override
    public void messagesSent(final ConnectionMessagesSentEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            cur.messagesSent(event);
        }
    }

    @Override
    public void messageReceived(final ConnectionMessageReceivedEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            cur.messageReceived(event);
        }
    }
}
