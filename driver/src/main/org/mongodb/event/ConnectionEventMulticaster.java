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
 * A multicaster for connection events.
 *
 * @since 3.0
 */
public class ConnectionEventMulticaster implements ConnectionListener {
    private Set<ConnectionListener> connectionListeners = Collections.newSetFromMap(new ConcurrentHashMap<ConnectionListener, Boolean>());

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
    public void connectionOpened(final ConnectionEvent event) {
        for (ConnectionListener cur : connectionListeners) {
            cur.connectionOpened(event);
        }
    }

    @Override
    public void connectionClosed(final ConnectionEvent event) {
        for (ConnectionListener cur : connectionListeners) {
            cur.connectionClosed(event);
        }
    }

    @Override
    public void messageSent(final ConnectionMessageSentEvent event) {
        for (ConnectionListener cur : connectionListeners) {
            cur.messageSent(event);
        }
    }

    @Override
    public void messageReceived(final ConnectionMessageReceivedEvent event) {
        for (ConnectionListener cur : connectionListeners) {
            cur.messageReceived(event);
        }
    }
}
