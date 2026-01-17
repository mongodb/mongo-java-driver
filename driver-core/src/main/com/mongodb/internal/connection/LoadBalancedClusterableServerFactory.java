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
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.inject.EmptyProvider;
import com.mongodb.lang.Nullable;

import java.util.List;

import static com.mongodb.internal.event.EventListenerHelper.singleServerListener;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public class LoadBalancedClusterableServerFactory implements ClusterableServerFactory {
    private final ServerSettings serverSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final InternalMongoClientSettings internalSettings;
    private final StreamFactory streamFactory;
    private final MongoCredentialWithCache credential;
    private final LoggerSettings loggerSettings;
    private final CommandListener commandListener;
    private final List<MongoCompressor> compressorList;
    private final ServerApi serverApi;
    private final InternalOperationContextFactory operationContextFactory;

    public LoadBalancedClusterableServerFactory(final ServerSettings serverSettings,
            final ConnectionPoolSettings connectionPoolSettings,
            final InternalMongoClientSettings internalSettings,
            final StreamFactory streamFactory, @Nullable final MongoCredential credential,
            final LoggerSettings loggerSettings,
            @Nullable final CommandListener commandListener,
            final List<MongoCompressor> compressorList, @Nullable final ServerApi serverApi,
            final InternalOperationContextFactory operationContextFactory) {
        this.serverSettings = serverSettings;
        this.connectionPoolSettings = connectionPoolSettings;
        this.internalSettings = internalSettings;
        this.streamFactory = streamFactory;
        this.credential = credential == null ? null : new MongoCredentialWithCache(credential);
        this.loggerSettings = loggerSettings;
        this.commandListener = commandListener;
        this.compressorList = compressorList;
        this.serverApi = serverApi;
        this.operationContextFactory = operationContextFactory;
    }

    @Override
    public ClusterableServer create(final Cluster cluster, final ServerAddress serverAddress) {
        ConnectionPool connectionPool = new DefaultConnectionPool(new ServerId(cluster.getClusterId(), serverAddress),
                new InternalStreamConnectionFactory(ClusterConnectionMode.LOAD_BALANCED, false, streamFactory, credential, cluster.getClientMetadata(),
                        compressorList, loggerSettings, commandListener, serverApi, internalSettings.isRecordEverything()),
                connectionPoolSettings, internalSettings.getInternalConnectionPoolSettings(), EmptyProvider.instance(), operationContextFactory);
        connectionPool.ready();

        return new LoadBalancedServer(new ServerId(cluster.getClusterId(), serverAddress), connectionPool, new DefaultConnectionFactory(),
                singleServerListener(serverSettings), cluster.getClock());
    }

    @Override
    public ServerSettings getSettings() {
        return serverSettings;
    }
}
