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

package com.mongodb.internal.event;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.String.format;


final class ConnectionPoolListenerMulticaster implements ConnectionPoolListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<ConnectionPoolListener> connectionPoolListeners;

    ConnectionPoolListenerMulticaster(final List<ConnectionPoolListener> connectionPoolListeners) {
        isTrue("All ConnectionPoolListener instances are non-null", !connectionPoolListeners.contains(null));
        this.connectionPoolListeners = new ArrayList<ConnectionPoolListener>(connectionPoolListeners);
    }

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolOpened(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool opened event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool closed event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckedOut(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool checked out event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckedIn(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool checked in event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.waitQueueEntered(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool wait queue entered event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.waitQueueExited(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool wait queue exited event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                LOGGER.info("Connection Pool Listener : 130 ");
                cur.connectionAdded(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection added event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionRemoved(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection removed event to listener %s", cur), e);
                }
            }
        }
    }
}
