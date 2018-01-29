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

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A multicaster for server events.
 *
 * @since 3.3
 * @deprecated register multiple server monitor listeners instead
 */
@Deprecated
public final class ServerMonitorEventMulticaster implements ServerMonitorListener {
    private static final Logger LOGGER = Loggers.getLogger("cluster.event");

    private final List<ServerMonitorListener> serverMonitorListeners;

    /**
     * Construct an instance with the given list of server monitor listeners
     *
     * @param serverMonitorListeners the non-null list of server monitor listeners, none of which may be null
     */
    public ServerMonitorEventMulticaster(final List<ServerMonitorListener> serverMonitorListeners) {
        notNull("serverMonitorListeners", serverMonitorListeners);
        isTrue("All ServerMonitorListener instances are non-null", !serverMonitorListeners.contains(null));
        this.serverMonitorListeners = new ArrayList<ServerMonitorListener>(serverMonitorListeners);
    }

    /**
     * Gets the server monitor listeners.
     *
     * @return the server monitor listeners
     */
    public List<ServerMonitorListener> getServerMonitorListeners() {
        return Collections.unmodifiableList(serverMonitorListeners);
    }

    @Override
    public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
        for (ServerMonitorListener cur : serverMonitorListeners) {
            try {
                cur.serverHearbeatStarted(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server heartbeat started event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
        for (ServerMonitorListener cur : serverMonitorListeners) {
            try {
                cur.serverHeartbeatSucceeded(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server heartbeat succeeded event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
        for (ServerMonitorListener cur : serverMonitorListeners) {
            try {
                cur.serverHeartbeatFailed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server heartbeat failed event to listener %s", cur), e);
                }
            }
        }
    }
}
