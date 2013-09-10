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

package org.mongodb.connection;

import org.mongodb.MongoCredential;
import org.mongodb.event.ConnectionListener;
import org.mongodb.event.ConnectionPoolListener;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private final String clusterId;
    private ServerSettings settings;
    private final ConnectionListener connectionListener;
    private final ConnectionProviderFactory connectionProviderFactory;
    private final StreamFactory heartbeatStreamFactory;
    private final ScheduledExecutorService scheduledExecutorService;
    private final BufferProvider bufferProvider;

    public DefaultClusterableServerFactory(final String clusterId, final ServerSettings settings,
                                           final ConnectionPoolSettings connectionPoolSettings,
                                           final StreamFactory streamFactory,
                                           final StreamFactory heartbeatStreamFactory,
                                           final ScheduledExecutorService scheduledExecutorService,
                                           final List<MongoCredential> credentialList,
                                           final BufferProvider bufferProvider,
                                           final ConnectionListener connectionListener,
                                           final ConnectionPoolListener connectionPoolListener) {
        this.clusterId = clusterId;
        this.settings = settings;
        this.connectionListener = connectionListener;
        this.connectionProviderFactory = new PooledConnectionProviderFactory(clusterId, connectionPoolSettings, streamFactory,
                credentialList, bufferProvider, connectionListener, connectionPoolListener);
        this.heartbeatStreamFactory = heartbeatStreamFactory;
        this.scheduledExecutorService = scheduledExecutorService;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        return new DefaultServer(clusterId, serverAddress, settings, connectionProviderFactory.create(serverAddress),
                new InternalStreamConnectionFactory(heartbeatStreamFactory, bufferProvider, Collections.<MongoCredential>emptyList(),
                        connectionListener),
                scheduledExecutorService, bufferProvider);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}
