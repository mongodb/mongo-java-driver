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
 * @deprecated register multiple server listeners instead
 */
@Deprecated
public final class ServerEventMulticaster implements ServerListener {

    private static final Logger LOGGER = Loggers.getLogger("cluster.event");

    private final List<ServerListener> serverListeners;

    /**
     * Construct an instance with the given list of server listeners
     *
     * @param serverListeners the non-null list of server listeners, none of which may be null
     */
    public ServerEventMulticaster(final List<ServerListener> serverListeners) {
        notNull("serverListeners", serverListeners);
        isTrue("All ServerListener instances are non-null", !serverListeners.contains(null));
        this.serverListeners = new ArrayList<ServerListener>(serverListeners);
    }

    /**
     * Gets the server listeners.
     *
     * @return the server listeners
     */
    public List<ServerListener> getServerListeners() {
        return Collections.unmodifiableList(serverListeners);
    }

    @Override
    public void serverOpening(final ServerOpeningEvent event) {
        for (ServerListener cur : serverListeners) {
            try {
                cur.serverOpening(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server opening event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void serverClosed(final ServerClosedEvent event) {
        for (ServerListener cur : serverListeners) {
            try {
                cur.serverClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server opening event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
        for (ServerListener cur : serverListeners) {
            try {
                cur.serverDescriptionChanged(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising server description changed event to listener %s", cur), e);
                }
            }
        }
    }
}
