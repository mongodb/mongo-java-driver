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

package org.mongodb.connection.impl;

import org.mongodb.connection.AsyncConnectionProviderFactory;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterableServer;
import org.mongodb.connection.ClusterableServerFactory;
import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.ConnectionProviderFactory;
import org.mongodb.connection.ServerAddress;

import java.util.concurrent.ScheduledExecutorService;

public final class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private ServerSettings settings;
    private final ConnectionProviderFactory connectionProviderFactory;
    private final AsyncConnectionProviderFactory asyncConnectionProviderFactory;
    private final ConnectionFactory heartbeatConnectionFactory;
    private final ScheduledExecutorService scheduledExecutorService;
    private final BufferProvider bufferProvider;

    public DefaultClusterableServerFactory(final ServerSettings settings,
                                           final ConnectionProviderFactory connectionProviderFactory,
                                           final AsyncConnectionProviderFactory asyncConnectionProviderFactory,
                                           final ConnectionFactory heartbeatConnectionFactory,
                                           final ScheduledExecutorService scheduledExecutorService,
                                           final BufferProvider bufferProvider) {

        this.settings = settings;
        this.connectionProviderFactory = connectionProviderFactory;
        this.asyncConnectionProviderFactory = asyncConnectionProviderFactory;
        this.heartbeatConnectionFactory = heartbeatConnectionFactory;
        this.scheduledExecutorService = scheduledExecutorService;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        return new DefaultServer(serverAddress, settings, connectionProviderFactory.create(serverAddress),
                asyncConnectionProviderFactory != null ? asyncConnectionProviderFactory.create(serverAddress) : null,
                heartbeatConnectionFactory, scheduledExecutorService, bufferProvider);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}
