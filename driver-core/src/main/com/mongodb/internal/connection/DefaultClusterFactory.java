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
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.InetAddressResolver;

import java.util.List;

import static com.mongodb.internal.connection.DefaultClusterFactory.ClusterEnvironment.detectCluster;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_CLUSTER_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_MONITOR_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.clusterListenerMulticaster;
import static com.mongodb.internal.event.EventListenerHelper.serverListenerMulticaster;
import static com.mongodb.internal.event.EventListenerHelper.serverMonitorListenerMulticaster;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

/**
 * The default factory for cluster implementations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@SuppressWarnings("deprecation")
public final class DefaultClusterFactory {
    private static final Logger LOGGER = Loggers.getLogger("client");

    public Cluster createCluster(final ClusterSettings originalClusterSettings, final ServerSettings originalServerSettings,
                                 final ConnectionPoolSettings connectionPoolSettings,
                                 final InternalConnectionPoolSettings internalConnectionPoolSettings,
                                 final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory,
                                 @Nullable final MongoCredential credential,
                                 final LoggerSettings loggerSettings,
                                 @Nullable final CommandListener commandListener,
                                 @Nullable final String applicationName,
                                 @Nullable final MongoDriverInformation mongoDriverInformation,
                                 final List<MongoCompressor> compressorList, @Nullable final ServerApi serverApi,
                                 @Nullable final DnsClient dnsClient, @Nullable final InetAddressResolver inetAddressResolver) {

        detectAndLogClusterEnvironment(originalClusterSettings);

        ClusterId clusterId = new ClusterId(applicationName);
        ClusterSettings clusterSettings;
        ServerSettings serverSettings;

        if (noClusterEventListeners(originalClusterSettings, originalServerSettings)) {
            clusterSettings = ClusterSettings.builder(originalClusterSettings)
                    .clusterListenerList(singletonList(NO_OP_CLUSTER_LISTENER))
                    .build();
            serverSettings = ServerSettings.builder(originalServerSettings)
                    .serverListenerList(singletonList(NO_OP_SERVER_LISTENER))
                    .serverMonitorListenerList(singletonList(NO_OP_SERVER_MONITOR_LISTENER))
                    .build();
        } else {
            AsynchronousClusterEventListener clusterEventListener =
                    AsynchronousClusterEventListener.startNew(clusterId, getClusterListener(originalClusterSettings),
                            getServerListener(originalServerSettings), getServerMonitorListener(originalServerSettings));

            clusterSettings = ClusterSettings.builder(originalClusterSettings)
                    .clusterListenerList(singletonList(clusterEventListener))
                    .build();
            serverSettings = ServerSettings.builder(originalServerSettings)
                    .serverListenerList(singletonList(clusterEventListener))
                    .serverMonitorListenerList(singletonList(clusterEventListener))
                    .build();
        }

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = new DefaultDnsSrvRecordMonitorFactory(clusterId, serverSettings, dnsClient);

        if (clusterSettings.getMode() == ClusterConnectionMode.LOAD_BALANCED) {
            ClusterableServerFactory serverFactory = new LoadBalancedClusterableServerFactory(serverSettings,
                    connectionPoolSettings, internalConnectionPoolSettings, streamFactory, credential, loggerSettings, commandListener,
                    applicationName, mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(),
                    compressorList, serverApi, inetAddressResolver);
            return new LoadBalancedCluster(clusterId, clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);
        } else {
            ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(serverSettings,
                    connectionPoolSettings, internalConnectionPoolSettings,
                    streamFactory, heartbeatStreamFactory, credential, loggerSettings, commandListener, applicationName,
                    mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList,
                    serverApi, inetAddressResolver);

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
    }

    private boolean noClusterEventListeners(final ClusterSettings clusterSettings, final ServerSettings serverSettings) {
        return clusterSettings.getClusterListeners().isEmpty()
                && serverSettings.getServerListeners().isEmpty()
                && serverSettings.getServerMonitorListeners().isEmpty();
    }

    private static ClusterListener getClusterListener(final ClusterSettings clusterSettings) {
        return clusterSettings.getClusterListeners().size() == 0
                ? NO_OP_CLUSTER_LISTENER
                : clusterListenerMulticaster(clusterSettings.getClusterListeners());
    }

    private static ServerListener getServerListener(final ServerSettings serverSettings) {
        return serverSettings.getServerListeners().size() == 0
                ? NO_OP_SERVER_LISTENER
                : serverListenerMulticaster(serverSettings.getServerListeners());
    }

    private static ServerMonitorListener getServerMonitorListener(final ServerSettings serverSettings) {
        return serverSettings.getServerMonitorListeners().size() == 0
                ? NO_OP_SERVER_MONITOR_LISTENER
                : serverMonitorListenerMulticaster(serverSettings.getServerMonitorListeners());
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public void detectAndLogClusterEnvironment(final ClusterSettings clusterSettings) {
        String srvHost = clusterSettings.getSrvHost();
        ClusterEnvironment clusterEnvironment;
        if (srvHost != null) {
            clusterEnvironment = detectCluster(srvHost);
        } else {
            clusterEnvironment = detectCluster(clusterSettings.getHosts()
                    .stream()
                    .map(ServerAddress::getHost)
                    .toArray(String[]::new));
        }

        if (clusterEnvironment != null) {
            LOGGER.info(format("You appear to be connected to a %s cluster. For more information regarding feature compatibility"
                    + " and support please visit %s", clusterEnvironment.clusterProductName, clusterEnvironment.documentationUrl));
        }
    }

    enum ClusterEnvironment {
        AZURE("https://www.mongodb.com/supportability/cosmosdb",
                "CosmosDB",
                ".cosmos.azure.com"),
        AWS("https://www.mongodb.com/supportability/documentdb",
                "DocumentDB",
                ".docdb.amazonaws.com", ".docdb-elastic.amazonaws.com");

        private final String documentationUrl;
        private final String clusterProductName;
        private final String[] hostSuffixes;

        ClusterEnvironment(final String url, final String name, final String... hostSuffixes) {
            this.hostSuffixes = hostSuffixes;
            this.documentationUrl = url;
            this.clusterProductName = name;
        }
        @Nullable
        public static ClusterEnvironment detectCluster(final String... hosts) {
            for (String host : hosts) {
                for (ClusterEnvironment clusterEnvironment : values()) {
                    if (clusterEnvironment.isExternalClusterProvider(host)) {
                        return clusterEnvironment;
                    }
                }
            }
            return null;
        }

        private boolean isExternalClusterProvider(final String host) {
            for (String hostSuffix : hostSuffixes) {
                String lowerCaseHost = host.toLowerCase();
                if (lowerCaseHost.endsWith(hostSuffix)) {
                    return true;
                }
            }
            return false;
        }
    }
}
