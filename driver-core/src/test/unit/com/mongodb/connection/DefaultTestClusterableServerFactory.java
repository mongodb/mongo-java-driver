/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.event.ServerListener;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public class DefaultTestClusterableServerFactory implements ClusterableServerFactory {
    private final ServerSettings settings = ServerSettings.builder().build();
    private final ClusterId clusterId;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ServerListenerFactory serverListenerFactory;
    private final Map<ServerAddress, TestServerMonitorFactory> serverAddressToServerMonitorFactoryMap =
            new HashMap<ServerAddress, TestServerMonitorFactory>();

    public DefaultTestClusterableServerFactory(final ClusterId clusterId, final ClusterConnectionMode clusterConnectionMode,
                                               final ServerListenerFactory serverListenerFactory) {
        this.clusterId = clusterId;
        this.clusterConnectionMode = clusterConnectionMode;
        this.serverListenerFactory = serverListenerFactory;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress, final ServerListener serverListener) {
        TestServerMonitorFactory serverMonitorFactory = new TestServerMonitorFactory(new ServerId(clusterId, serverAddress));
        serverAddressToServerMonitorFactoryMap.put(serverAddress, serverMonitorFactory);



        return new DefaultServer(new ServerId(clusterId, serverAddress), clusterConnectionMode, new TestConnectionPool(),
                new TestConnectionFactory(), serverMonitorFactory,
                                        asList(serverListener, serverListenerFactory.create(serverAddress)), null);
    }

    @Override
    public ServerSettings getSettings() {
        return settings;
    }


    public void sendNotification(final ServerAddress serverAddress, final ServerDescription serverDescription) {
        serverAddressToServerMonitorFactoryMap.get(serverAddress).sendNotification(serverDescription);
    }
}
