/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb.connection;

import static com.mongodb.assertions.Assertions.notNull;

class DefaultServerMonitorFactory implements ServerMonitorFactory {
    private final ServerId serverId;
    private final ServerSettings settings;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPool connectionPool;

    DefaultServerMonitorFactory(final ServerId serverId, final ServerSettings settings,
                                final InternalConnectionFactory internalConnectionFactory, final ConnectionPool connectionPool) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        this.internalConnectionFactory = notNull("internalConnectionFactory", internalConnectionFactory);
        this.connectionPool = notNull("connectionPool", connectionPool);
    }

    @Override
    public ServerMonitor create(final ChangeListener<ServerDescription> serverStateListener) {
        return new DefaultServerMonitor(serverId, settings, serverStateListener, internalConnectionFactory, connectionPool);
    }
}
