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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.ServerListener;
import com.mongodb.internal.inject.SameObjectProvider;

import java.util.HashMap;
import java.util.Map;

public class DefaultTestClusterableServerFactory implements ClusterableServerFactory {
    private final ServerSettings settings = ServerSettings.builder().build();
    private final ClusterId clusterId;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerListenerFactory serverListenerFactory;
    private final Map<ServerAddress, TestServerMonitor> serverAddressToServerMonitorMap = new HashMap<>();

    public DefaultTestClusterableServerFactory(final ClusterId clusterId, final ClusterConnectionMode clusterConnectionMode,
                                               final ServerListenerFactory serverListenerFactory) {
        this.clusterId = clusterId;
        this.clusterConnectionMode = clusterConnectionMode;
        this.serverListenerFactory = serverListenerFactory;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress,
                                    final ServerDescriptionChangedListener serverDescriptionChangedListener,
                                    final ClusterClock clusterClock) {
        ServerId serverId = new ServerId(clusterId, serverAddress);
        if (clusterConnectionMode == ClusterConnectionMode.LOAD_BALANCED) {
            return new LoadBalancedServer(serverId, new TestConnectionPool(),
                    new TestConnectionFactory(), serverListenerFactory.create(serverAddress), clusterClock);
        } else {
            SameObjectProvider<SdamServerDescriptionManager> sdamProvider = SameObjectProvider.uninitialized();
            TestServerMonitor serverMonitor = new TestServerMonitor(sdamProvider);
            serverAddressToServerMonitorMap.put(serverAddress, serverMonitor);
            ConnectionPool connectionPool = new TestConnectionPool();
            ServerListener serverListener = serverListenerFactory.create(serverAddress);
            SdamServerDescriptionManager sdam = new DefaultSdamServerDescriptionManager(serverId, serverDescriptionChangedListener,
                    serverListener, serverMonitor, connectionPool, clusterConnectionMode);
            sdamProvider.initialize(sdam);
            serverMonitor.start();
            return new DefaultServer(serverId, clusterConnectionMode, connectionPool, new TestConnectionFactory(), serverMonitor, sdam,
                    serverListener, null, clusterClock, true);
        }
    }

    @Override
    public ServerSettings getSettings() {
        return settings;
    }


    public void sendNotification(final ServerAddress serverAddress, final ServerDescription serverDescription) {
        serverAddressToServerMonitorMap.get(serverAddress).updateServerDescription(serverDescription);
    }

}
