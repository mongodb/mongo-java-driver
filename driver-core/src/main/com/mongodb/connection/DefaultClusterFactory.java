/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.MongoCredential;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionPoolListener;

import java.util.List;

/**
 * The default factory for cluster implementations.
 *
 * @since 3.0
 */
public final class DefaultClusterFactory implements ClusterFactory {

    @Override
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener) {
        return create(settings, serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList,
                      clusterListener, connectionPoolListener, connectionListener, null);
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param settings               the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param clusterListener        an optional listener for cluster-related events
     * @param connectionPoolListener an optional listener for connection pool-related events
     * @param connectionListener     an optional listener for connection-related events
     * @param commandListener        an optional listener for command-related events
     * @return the cluster
     *
     * @since 3.1
     */
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener,
                          final CommandListener commandListener) {
        return create(settings, serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList,
                clusterListener, connectionPoolListener, connectionListener, commandListener, null);
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param settings               the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param clusterListener        an optional listener for cluster-related events
     * @param connectionPoolListener an optional listener for connection pool-related events
     * @param connectionListener     an optional listener for connection-related events
     * @param commandListener        an optional listener for command-related events
     * @param applicationName        an optional application name to associate with connections to the servers in this cluster
     * @return the cluster
     *
     * @since 3.4
     */
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener,
                          final CommandListener commandListener,
                          final String applicationName) {
        if (clusterListener != null) {
            throw new IllegalArgumentException("Add cluster listener to ClusterSettings");
        }
        ClusterId clusterId = new ClusterId(settings.getDescription());
        ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(clusterId,
                                                                                            settings,
                                                                                            serverSettings,
                                                                                            connectionPoolSettings,
                                                                                            streamFactory,
                                                                                            heartbeatStreamFactory,
                                                                                            credentialList,
                                                                                            connectionListener != null ? connectionListener
                                                                                                    : new NoOpConnectionListener(),
                                                                                            connectionPoolListener != null
                                                                                                    ? connectionPoolListener
                                                                                                    : new NoOpConnectionPoolListener(),
                                                                                            commandListener,
                                                                                            applicationName);

        if (settings.getMode() == ClusterConnectionMode.SINGLE) {
            return new SingleServerCluster(clusterId, settings, serverFactory);
        } else if (settings.getMode() == ClusterConnectionMode.MULTIPLE) {
            return new MultiServerCluster(clusterId, settings, serverFactory);
        } else {
            throw new UnsupportedOperationException("Unsupported cluster mode: " + settings.getMode());
        }
    }
}
