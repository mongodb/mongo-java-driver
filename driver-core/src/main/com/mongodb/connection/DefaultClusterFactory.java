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
import com.mongodb.internal.connection.DefaultDnsSrvRecordMonitorFactory;
import com.mongodb.internal.connection.DnsMultiServerCluster;
import com.mongodb.internal.connection.DnsSrvRecordMonitorFactory;
import com.mongodb.internal.connection.MultiServerCluster;
import com.mongodb.internal.connection.SingleServerCluster;

import java.util.Collections;
import java.util.List;

/**
 * The default factory for cluster implementations.
 *
 * @since 3.0
 */
@Deprecated
public final class DefaultClusterFactory implements ClusterFactory {

    @Override
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final MongoCredential credential,
                          final ClusterListener clusterListener,
                          final ConnectionPoolListener connectionPoolListener) {

        return createCluster(getClusterSettings(settings, clusterListener), serverSettings,
                getConnectionPoolSettings(connectionPoolSettings, connectionPoolListener), streamFactory,
                heartbeatStreamFactory, credential, null, null, null,
                Collections.<MongoCompressor>emptyList());
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param clusterSettings        the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credential             the credential, which may be null
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
                                 final StreamFactory heartbeatStreamFactory, final MongoCredential credential,
                                 final CommandListener commandListener, final String applicationName,
                                 final MongoDriverInformation mongoDriverInformation,
                                 final List<MongoCompressor> compressorList) {

        ClusterId clusterId = new ClusterId();


        ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(clusterId, clusterSettings, serverSettings,
                connectionPoolSettings, streamFactory, heartbeatStreamFactory, credential, commandListener, applicationName,
                mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = new DefaultDnsSrvRecordMonitorFactory(clusterId, serverSettings);

        if (clusterSettings.getMode() == ClusterConnectionMode.SINGLE) {
            return new SingleServerCluster(clusterId, clusterSettings, serverFactory);
        } else if (clusterSettings.getMode() == ClusterConnectionMode.MULTIPLE) {
            if (clusterSettings.getSrvHost() == null) {
                return new MultiServerCluster(clusterId, clusterSettings, serverFactory);
            } else {
                return new DnsMultiServerCluster(clusterId, clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);
            }
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
