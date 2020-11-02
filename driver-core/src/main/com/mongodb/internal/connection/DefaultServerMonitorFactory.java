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

package com.mongodb.internal.connection;

import com.mongodb.ServerApi;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;

class DefaultServerMonitorFactory implements ServerMonitorFactory {
    private final ServerId serverId;
    private final ServerSettings settings;
    private final ClusterClock clusterClock;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPool connectionPool;
    @Nullable
    private final ServerApi serverApi;

    DefaultServerMonitorFactory(final ServerId serverId, final ServerSettings settings,
                                final ClusterClock clusterClock, final InternalConnectionFactory internalConnectionFactory,
                                final ConnectionPool connectionPool, final @Nullable ServerApi serverApi) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        this.clusterClock = notNull("clusterClock", clusterClock);
        this.internalConnectionFactory = notNull("internalConnectionFactory", internalConnectionFactory);
        this.connectionPool = notNull("connectionPool", connectionPool);
        this.serverApi = serverApi;
    }

    @Override
    public ServerMonitor create(final ChangeListener<ServerDescription> serverStateListener) {
        return new DefaultServerMonitor(serverId, settings, clusterClock, serverStateListener, internalConnectionFactory, connectionPool,
                serverApi);
    }
}
