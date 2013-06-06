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

package org.mongodb.connection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ClusterConnectionMode.Discovering;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 */
public final class DefaultMultiServerCluster extends DefaultCluster {
    private final ConcurrentMap<ServerAddress, ClusterableServer> addressToServerMap =
            new ConcurrentHashMap<ServerAddress, ClusterableServer>();

    public DefaultMultiServerCluster(final List<ServerAddress> seedList, final ClusterableServerFactory serverFactory) {
        super(serverFactory);

        notNull("seedList", seedList);
        // synchronizing this code because addServer registers a callback which is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            for (ServerAddress serverAddress : seedList) {
                addServer(serverAddress);
            }
        }
    }

    @Override
    public void close() {
        if (!isClosed()) {
            synchronized (this) {
                for (ClusterableServer server : addressToServerMap.values()) {
                    server.close();
                }
            }
            super.close();
        }
    }

    @Override
    protected ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        ClusterableServer server = addressToServerMap.get(serverAddress);
        if (server == null) {
            throw new MongoServerNotFoundException("The requested server is not available: " + serverAddress);
        }
        return server;
    }


    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            onChange(event);
        }
    }

    private synchronized void onChange(final ChangeEvent<ServerDescription> event) {
        if (isClosed()) {
            return;
        }

        if (event.getNewValue().isReplicaSetMember()) {
            if (event.getNewValue().isOk()) {
                addNewHosts(event.getNewValue().getHosts());
                addNewHosts(event.getNewValue().getPassives());
                removeExtras(event.getNewValue());
            }
            else if (event.getOldValue().isPrimary()) {
                invalidateAll();
            }
        }
        updateDescription();
    }

    private void addServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        if (!addressToServerMap.containsKey(serverAddress)) {
            ClusterableServer mongoServer = createServer(serverAddress, new DefaultServerStateListener());
            addressToServerMap.put(serverAddress, mongoServer);
            updateDescription();
        }
    }

    private void removeServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        ClusterableServer server = addressToServerMap.remove(serverAddress);
        server.close();
        updateDescription();
    }


    private void invalidateAll() {
        isTrue("open", !isClosed());

        for (ClusterableServer server : addressToServerMap.values()) {
            server.invalidate();
        }
    }

    private void updateDescription() {
        isTrue("open", !isClosed());

        final List<ServerDescription> newServerDescriptionList = getNewServerDescriptionList();
        updateDescription(new ClusterDescription(newServerDescriptionList, Discovering));
    }

    private List<ServerDescription> getNewServerDescriptionList() {
        List<ServerDescription> serverDescriptions = new ArrayList<ServerDescription>();
        for (Server server : addressToServerMap.values()) {
            serverDescriptions.add(server.getDescription());
        }
        return serverDescriptions;
    }

    private void addNewHosts(final Set<String> hosts) {
        for (String cur : hosts) {
            addServer(new ServerAddress(cur));
        }
    }

    private void removeExtras(final ServerDescription serverDescription) {
        Set<ServerAddress> allServerAddresses = getAllServerAddresses(serverDescription);
        for (Server cur : addressToServerMap.values()) {
            if (!allServerAddresses.contains(cur.getDescription().getAddress())) {
                removeServer(cur.getDescription().getAddress());
            }
        }
    }

    private Set<ServerAddress> getAllServerAddresses(final ServerDescription serverDescription) {
        Set<ServerAddress> retVal = new HashSet<ServerAddress>();
        addHostsToSet(serverDescription.getHosts(), retVal);
        addHostsToSet(serverDescription.getPassives(), retVal);
        addHostsToSet(serverDescription.getArbiters(), retVal);
        return retVal;
    }

    private void addHostsToSet(final Set<String> hosts, final Set<ServerAddress> retVal) {
        for (String host : hosts) {
            retVal.add(new ServerAddress(host));
        }
    }
}
