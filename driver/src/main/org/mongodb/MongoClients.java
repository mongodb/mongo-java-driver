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

package org.mongodb;


import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.AsyncConnectionSettings;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.impl.ConnectionProviderSettings;
import org.mongodb.connection.impl.ConnectionSettings;
import org.mongodb.connection.impl.DefaultAsyncConnectionFactory;
import org.mongodb.connection.impl.DefaultAsyncConnectionProviderFactory;
import org.mongodb.connection.impl.DefaultClusterFactory;
import org.mongodb.connection.impl.DefaultClusterableServerFactory;
import org.mongodb.connection.impl.DefaultConnectionFactory;
import org.mongodb.connection.impl.DefaultConnectionProviderFactory;
import org.mongodb.connection.impl.PowerOfTwoBufferPool;
import org.mongodb.connection.impl.ServerSettings;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public final class MongoClients {
    public static MongoClient create(final ServerAddress serverAddress) {
        return create(serverAddress, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final ServerAddress serverAddress, final List<MongoCredential> credentialList) {
        return create(serverAddress, credentialList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return create(serverAddress, Collections.<MongoCredential>emptyList(), options);
    }

    public static MongoClient create(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                      final MongoClientOptions options) {
        return new MongoClientImpl(options, new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(ClusterConnectionMode.Single).hosts(Arrays.asList(serverAddress))
                        .requiredReplicaSetName(options.getRequiredReplicaSetName()).build(),
                getClusterableServerFactory(credentialList, options)));
    }

    public static MongoClient create(final List<ServerAddress> seedList) {
        return create(seedList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return new MongoClientImpl(options, new DefaultClusterFactory().create(
                ClusterSettings.builder().hosts(seedList).requiredReplicaSetName(options.getRequiredReplicaSetName()).build(),
                getClusterableServerFactory(Collections.<MongoCredential>emptyList(), options)));
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return create(mongoURI, mongoURI.getOptions());
    }

    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) throws UnknownHostException {
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, new DefaultClusterFactory().create(
                    ClusterSettings.builder()
                            .mode(ClusterConnectionMode.Single)
                            .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts().get(0))))
                            .requiredReplicaSetName(options.getRequiredReplicaSetName())
                            .build(),
                    getClusterableServerFactory(mongoURI.getCredentialList(), options)));
        }
        else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, new DefaultClusterFactory().create(
                    ClusterSettings.builder().hosts(seedList).requiredReplicaSetName(options.getRequiredReplicaSetName()).build(),
                    getClusterableServerFactory(mongoURI.getCredentialList(), options)));
        }
    }

    private MongoClients() {
    }

    private static DefaultClusterableServerFactory getClusterableServerFactory(final List<MongoCredential> credentialList,
                                                                               final MongoClientOptions options) {
        BufferProvider bufferProvider = new PowerOfTwoBufferPool();
        SSLSettings sslSettings = SSLSettings.builder().enabled(options.isSSLEnabled()).build();
        AsyncConnectionSettings asyncConnectionSettings = AsyncConnectionSettings.builder()
            .poolSize(options.getAsyncPoolSize())
            .maxPoolSize(options.getAsyncMaxPoolSize())
            .keepAliveTime(options.getAsyncKeepAliveTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
            .build();

        ConnectionProviderSettings connectionProviderSettings = ConnectionProviderSettings.builder()
                .minSize(options.getMinConnectionPoolSize())
                .maxSize(options.getMaxConnectionPoolSize())
                .maxWaitQueueSize(options.getMaxConnectionPoolSize() * options.getThreadsAllowedToBlockForConnectionMultiplier())
                .maxWaitTime(options.getMaxWaitTime(), TimeUnit.MILLISECONDS)
                .maxConnectionIdleTime(options.getMaxConnectionIdleTime(), TimeUnit.MILLISECONDS)
                .maxConnectionLifeTime(options.getMaxConnectionLifeTime(), TimeUnit.MILLISECONDS)
                .build();
        ConnectionSettings connectionSettings = ConnectionSettings.builder()
                .connectTimeoutMS(options.getConnectTimeout())
                .readTimeoutMS(options.getSocketTimeout())
                .keepAlive(options.isSocketKeepAlive())
                .build();
        ConnectionFactory connectionFactory = new DefaultConnectionFactory(connectionSettings, sslSettings, bufferProvider, credentialList);

        DefaultAsyncConnectionProviderFactory asyncConnectionProviderFactory =
                options.isAsyncEnabled()
                        ? new DefaultAsyncConnectionProviderFactory(connectionProviderSettings,
                        new DefaultAsyncConnectionFactory(asyncConnectionSettings, sslSettings, bufferProvider, credentialList))
                        : null;
        return new DefaultClusterableServerFactory(
                ServerSettings.builder().heartbeatFrequency(options.getHeartbeatFrequency(), TimeUnit.MILLISECONDS).
                        connectRetryFrequency(options.getHeartbeatConnectRetryFrequency(), TimeUnit.MILLISECONDS).build(),
                new DefaultConnectionProviderFactory(connectionProviderSettings, connectionFactory),
                asyncConnectionProviderFactory,
                new DefaultConnectionFactory(
                        ConnectionSettings.builder()
                                .connectTimeoutMS(options.getHeartbeatConnectTimeout())
                                .readTimeoutMS(options.getHeartbeatSocketTimeout())
                                .keepAlive(options.isSocketKeepAlive())
                                .build(),
                        sslSettings, bufferProvider, Collections.<org.mongodb.MongoCredential>emptyList()),
                Executors.newScheduledThreadPool(3),  // TODO: allow configuration
                bufferProvider);
    }
}
