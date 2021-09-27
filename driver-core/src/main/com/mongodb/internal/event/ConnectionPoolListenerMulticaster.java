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
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ConnectionReadyEvent;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.String.format;

@SuppressWarnings("deprecation")
final class ConnectionPoolListenerMulticaster implements ConnectionPoolListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<ConnectionPoolListener> connectionPoolListeners;

    ConnectionPoolListenerMulticaster(final List<ConnectionPoolListener> connectionPoolListeners) {
        isTrue("All ConnectionPoolListener instances are non-null", !connectionPoolListeners.contains(null));
        this.connectionPoolListeners = new ArrayList<ConnectionPoolListener>(connectionPoolListeners);
    }

    @Override
    public void connectionPoolOpened(final com.mongodb.event.ConnectionPoolOpenedEvent event) {
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
    public void connectionPoolCreated(final ConnectionPoolCreatedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolCreated(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool created event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionPoolCleared(final ConnectionPoolClearedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolCleared(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool cleared event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionPoolReady(final ConnectionPoolReadyEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolReady(event);
            } catch (RuntimeException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool ready event to listener %s", cur), e);
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
    public void connectionCheckOutStarted(final ConnectionCheckOutStartedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckOutStarted(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection check out started event to listener %s", cur), e);
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
    public void connectionCheckOutFailed(final ConnectionCheckOutFailedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckOutFailed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool check out failed event to listener %s", cur), e);
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
    public void connectionRemoved(final com.mongodb.event.ConnectionRemovedEvent event) {
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

    @Override
    public void connectionAdded(final com.mongodb.event.ConnectionAddedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionAdded(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection added event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionCreated(final ConnectionCreatedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCreated(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection created event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionReady(final ConnectionReadyEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionReady(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection ready event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionClosed(final ConnectionClosedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection removed event to listener %s", cur), e);
                }
            }
        }
    }
}
