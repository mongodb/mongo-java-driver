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

import com.mongodb.LoggerSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerListener;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.lang.Nullable;

import java.util.List;

import static com.mongodb.internal.event.EventListenerHelper.singleServerListener;
import static java.util.Collections.emptyList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private final ServerSettings serverSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final InternalConnectionPoolSettings internalConnectionPoolSettings;
    private final StreamFactory streamFactory;
    private final MongoCredentialWithCache credential;
    private final StreamFactory heartbeatStreamFactory;
    private final LoggerSettings loggerSettings;
    private final CommandListener commandListener;
    private final String applicationName;
    private final MongoDriverInformation mongoDriverInformation;
    private final List<MongoCompressor> compressorList;
    @Nullable
    private final ServerApi serverApi;

    public DefaultClusterableServerFactory(
            final ServerSettings serverSettings, final ConnectionPoolSettings connectionPoolSettings,
            final InternalConnectionPoolSettings internalConnectionPoolSettings,
            final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory,
            @Nullable final MongoCredential credential,
            final LoggerSettings loggerSettings,
            @Nullable final CommandListener commandListener,
            @Nullable final String applicationName, @Nullable final MongoDriverInformation mongoDriverInformation,
            final List<MongoCompressor> compressorList, @Nullable final ServerApi serverApi) {
        this.serverSettings = serverSettings;
        this.connectionPoolSettings = connectionPoolSettings;
        this.internalConnectionPoolSettings = internalConnectionPoolSettings;
        this.streamFactory = streamFactory;
        this.credential = credential == null ? null : new MongoCredentialWithCache(credential);
        this.heartbeatStreamFactory = heartbeatStreamFactory;
        this.loggerSettings = loggerSettings;
        this.commandListener = commandListener;
        this.applicationName = applicationName;
        this.mongoDriverInformation = mongoDriverInformation;
        this.compressorList = compressorList;
        this.serverApi = serverApi;
    }

    @Override
    public ClusterableServer create(final Cluster cluster, final ServerAddress serverAddress) {
        ServerId serverId = new ServerId(cluster.getClusterId(), serverAddress);
        ClusterConnectionMode clusterMode = cluster.getSettings().getMode();
        SameObjectProvider<SdamServerDescriptionManager> sdamProvider = SameObjectProvider.uninitialized();
        ServerMonitor serverMonitor = new DefaultServerMonitor(serverId, serverSettings, cluster.getClock(),
                // no credentials, compressor list, or command listener for the server monitor factory
                new InternalStreamConnectionFactory(clusterMode, true, heartbeatStreamFactory, null, applicationName,
                        mongoDriverInformation, emptyList(), loggerSettings, null, serverApi),
                clusterMode, serverApi, sdamProvider);
        ConnectionPool connectionPool = new DefaultConnectionPool(serverId,
                new InternalStreamConnectionFactory(clusterMode, streamFactory, credential, applicationName,
                        mongoDriverInformation, compressorList, loggerSettings, commandListener, serverApi),
                connectionPoolSettings, internalConnectionPoolSettings, sdamProvider);
        ServerListener serverListener = singleServerListener(serverSettings);
        SdamServerDescriptionManager sdam = new DefaultSdamServerDescriptionManager(cluster, serverId, serverListener, serverMonitor,
                connectionPool, clusterMode);
        sdamProvider.initialize(sdam);
        serverMonitor.start();
        return new DefaultServer(serverId, clusterMode, connectionPool, new DefaultConnectionFactory(), serverMonitor,
                sdam, serverListener, commandListener, cluster.getClock(), true);
    }

    @Override
    public ServerSettings getSettings() {
        return serverSettings;
    }
}
