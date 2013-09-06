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

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private ServerSettings settings;
    private final ChannelProviderFactory channelProviderFactory;
    private final StreamFactory heartbeatStreamFactory;
    private final ScheduledExecutorService scheduledExecutorService;
    private final BufferProvider bufferProvider;

    public DefaultClusterableServerFactory(final ServerSettings settings,
                                           final ConnectionPoolSettings connectionPoolSettings,
                                           final StreamFactory streamFactory,
                                           final StreamFactory heartbeatStreamFactory,
                                           final ScheduledExecutorService scheduledExecutorService,
                                           final List<MongoCredential> credentialList,
                                           final BufferProvider bufferProvider) {

        this.settings = settings;
        this.channelProviderFactory = new DefaultChannelProviderFactory(connectionPoolSettings, streamFactory, credentialList,
                bufferProvider);
        this.heartbeatStreamFactory = heartbeatStreamFactory;
        this.scheduledExecutorService = scheduledExecutorService;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        return new DefaultServer(serverAddress, settings, channelProviderFactory.create(serverAddress),
                heartbeatStreamFactory, scheduledExecutorService, bufferProvider);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}
