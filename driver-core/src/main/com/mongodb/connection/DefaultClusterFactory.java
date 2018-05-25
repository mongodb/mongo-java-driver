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

package com.mongodb.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.internal.connection.ClusterableServerFactory;
import com.mongodb.internal.connection.DefaultClusterableServerFactory;
import com.mongodb.internal.connection.MultiServerCluster;
import com.mongodb.internal.connection.SingleServerCluster;

import java.util.Collections;
import java.util.List;

/**
 * The default factory for cluster implementations.
 *
 * @since 3.0
 */
@SuppressWarnings("deprecation")
public final class DefaultClusterFactory implements ClusterFactory {

    @Override
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener,
                          final ConnectionPoolListener connectionPoolListener,
                          final com.mongodb.event.ConnectionListener connectionListener) {

        return createCluster(getClusterSettings(settings, clusterListener), serverSettings,
                getConnectionPoolSettings(connectionPoolSettings, connectionPoolListener), streamFactory,
                heartbeatStreamFactory, credentialList, null, null, null,
                Collections.<MongoCompressor>emptyList());
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
     * @deprecated use {@link #createCluster(ClusterSettings, ServerSettings, ConnectionPoolSettings, StreamFactory, StreamFactory,
     * List, CommandListener, String, MongoDriverInformation, List)} instead
     */
    @Deprecated
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener,
                          final ConnectionPoolListener connectionPoolListener,
                          final com.mongodb.event.ConnectionListener connectionListener,
                          final CommandListener commandListener) {
        return createCluster(getClusterSettings(settings, clusterListener), serverSettings,
                getConnectionPoolSettings(connectionPoolSettings, connectionPoolListener), streamFactory, heartbeatStreamFactory,
                credentialList, commandListener, null, null, Collections.<MongoCompressor>emptyList());
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
     * @param mongoDriverInformation the optional driver information associate with connections to the servers in this cluster
     * @return the cluster
     *
     * @since 3.4
     * @deprecated use {@link #createCluster(ClusterSettings, ServerSettings, ConnectionPoolSettings, StreamFactory, StreamFactory,
     * List, CommandListener, String, MongoDriverInformation, List)} instead
     */
    @Deprecated
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener,
                          final ConnectionPoolListener connectionPoolListener,
                          final com.mongodb.event.ConnectionListener connectionListener,
                          final CommandListener commandListener,
                          final String applicationName,
                          final MongoDriverInformation mongoDriverInformation) {
        return createCluster(getClusterSettings(settings, clusterListener), serverSettings,
                getConnectionPoolSettings(connectionPoolSettings, connectionPoolListener), streamFactory, heartbeatStreamFactory,
                credentialList, commandListener, applicationName, mongoDriverInformation, Collections.<MongoCompressor>emptyList());
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param clusterSettings        the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param commandListener        an optional listener for command-related events
     * @param applicationName        an optional application name to associate with connections to the servers in this cluster
     * @param mongoDriverInformation the optional driver information associate with connections to the servers in this cluster
     * @return the cluster
     *
     * @since 3.5
     * @deprecated use {@link #createCluster(ClusterSettings, ServerSettings, ConnectionPoolSettings, StreamFactory, StreamFactory,
     * List, CommandListener, String, MongoDriverInformation, List)} instead

     */
    @Deprecated
    public Cluster createCluster(final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                                 final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                                 final StreamFactory heartbeatStreamFactory, final List<MongoCredential> credentialList,
                                 final CommandListener commandListener, final String applicationName,
                                 final MongoDriverInformation mongoDriverInformation) {
        return createCluster(clusterSettings, serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList,
                commandListener, applicationName, mongoDriverInformation, Collections.<MongoCompressor>emptyList());
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param clusterSettings        the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param commandListener        an optional listener for command-related events
     * @param applicationName        an optional application name to associate with connections to the servers in this cluster
     * @param mongoDriverInformation the optional driver information associate with connections to the servers in this cluster
     * @param compressorList         the list of compressors to request, in priority order
     * @return the cluster
     *
     * @since 3.6
     */
    public Cluster createCluster(final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                                 final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                                 final StreamFactory heartbeatStreamFactory, final List<MongoCredential> credentialList,
                                 final CommandListener commandListener, final String applicationName,
                                 final MongoDriverInformation mongoDriverInformation,
                                 final List<MongoCompressor> compressorList) {

        ClusterId clusterId = new ClusterId(clusterSettings.getDescription());


        ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(clusterId, clusterSettings, serverSettings,
                connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList, commandListener, applicationName,
                mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList);

        if (clusterSettings.getMode() == ClusterConnectionMode.SINGLE) {
            return new SingleServerCluster(clusterId, clusterSettings, serverFactory);
        } else if (clusterSettings.getMode() == ClusterConnectionMode.MULTIPLE) {
            return new MultiServerCluster(clusterId, clusterSettings, serverFactory);
        } else {
            throw new UnsupportedOperationException("Unsupported cluster mode: " + clusterSettings.getMode());
        }
    }

    private ClusterSettings getClusterSettings(final ClusterSettings settings, final ClusterListener clusterListener) {
        return ClusterSettings.builder(settings).addClusterListener(clusterListener).build();
    }

    private ConnectionPoolSettings getConnectionPoolSettings(final ConnectionPoolSettings connPoolSettings,
                                                             final ConnectionPoolListener connPoolListener) {
        return ConnectionPoolSettings.builder(connPoolSettings).addConnectionPoolListener(connPoolListener).build();
    }
}
