/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mongodb.connection.ServerConnectionState.CONNECTED;

public class TestClusterableServerFactory implements ClusterableServerFactory {
    private final Map<ServerAddress, TestServer> addressToServerMap = new HashMap<ServerAddress, TestServer>();

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        addressToServerMap.put(serverAddress, new TestServer(serverAddress));
        return addressToServerMap.get(serverAddress);
    }

    @Override
    public void close() {

    }

    public TestServer getServer(final ServerAddress serverAddress) {
        return addressToServerMap.get(serverAddress);
    }

    public void sendNotification(final ServerAddress serverAddress, final ServerType serverType, final List<ServerAddress> hosts) {
        sendNotification(serverAddress, serverType, hosts, null);
    }

    public void sendNotification(final ServerAddress serverAddress, final ServerType serverType, final List<ServerAddress> hosts,
                                 final String setName, final int setVersion) {
        getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, true, setName, setVersion).build());
    }

    public void sendNotification(final ServerAddress serverAddress, final ServerType serverType, final List<ServerAddress> hosts,
                                 final String setName) {
        getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, true, setName, null).build());
    }

    public void sendNotification(final ServerAddress serverAddress, final ServerType serverType, final List<ServerAddress> hosts,
                                 final boolean ok) {
        getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, ok, null, null).build());
    }

    public ServerDescription getDescription(final ServerAddress server) {
        return getServer(server).getDescription();
    }

    public Set<ServerDescription> getDescriptions(final ServerAddress... servers) {
        Set<ServerDescription> serverDescriptions = new HashSet<ServerDescription>();
        for (ServerAddress cur : servers) {
            serverDescriptions.add(getServer(cur).getDescription());
        }
        return serverDescriptions;
    }

    private ServerDescription.Builder getBuilder(final ServerAddress serverAddress, final ServerType serverType,
                                                 final List<ServerAddress> hosts, final boolean ok,
                                                 final String setName, final Integer setVersion) {
        Set<String> hostsSet = new HashSet<String>();
        for (ServerAddress cur : hosts) {
            hostsSet.add(cur.toString());
        }
        return ServerDescription.builder()
                                .address(serverAddress)
                                .type(serverType)
                                .ok(ok)
                                .state(CONNECTED)
                                .hosts(hostsSet)
                                .setName(setName)
                                .setVersion(setVersion);
    }
}
