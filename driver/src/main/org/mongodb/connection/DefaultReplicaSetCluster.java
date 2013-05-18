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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;

class DefaultReplicaSetCluster extends MultiServerCluster implements ReplicaSetCluster {

    private final Map<ServerAddress, ServerDescription> mostRecentStateMap = new HashMap<ServerAddress, ServerDescription>();
    private final Map<ServerAddress, Boolean> activeMemberNotifications = new HashMap<ServerAddress, Boolean>();
    private final Random random = new Random();
    private volatile ClusterDescription description = new ClusterDescription(Collections.<ServerDescription>emptyList(),
            random, SLAVE_ACCEPTABLE_LATENCY_MS);
    private ConcurrentMap<ServerSelector, CountDownLatch> serverPreferenceLatches =
            new ConcurrentHashMap<ServerSelector, CountDownLatch>();

    public DefaultReplicaSetCluster(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                    final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                                    final ServerFactory serverFactory) {
        super(seedList, credentialList, options, bufferPool, serverFactory);
        notNull("seedList", seedList);
        notNull("options", options);
        notNull("bufferPool", bufferPool);
    }

    @Override
    public ClusterDescription getDescription() {
        return description;
    }

    @Override
    public Server getServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        final ServerAddress result;

        ServerDescription serverDescription = serverSelector.choose(description);
        if (serverDescription != null) {
            result = serverDescription.getAddress();
        }
        else {
            try {
                CountDownLatch newLatch = new CountDownLatch(1);
                CountDownLatch existingLatch = serverPreferenceLatches.putIfAbsent(serverSelector, newLatch);
                final CountDownLatch latch = existingLatch != null ? existingLatch : newLatch;
                if (latch.await(5, TimeUnit.SECONDS)) {  // TODO: make timeout configurable
                    serverDescription = serverSelector.choose(description);
                }
                if (serverDescription == null) {
                    throw new MongoTimeoutException("Timed out waiting for a server that satisfies the server preference: "
                            + serverSelector);
                }
                result = serverDescription.getAddress();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Thread was interrupted while waiting for a server that satisfied server preference: "
                        + serverSelector, e);
            }
        }
        return getServer(result);
    }

    @Override
    public void close() {
        super.close();
        for (CountDownLatch latch : serverPreferenceLatches.values()) {
            latch.countDown();
        }
    }

    protected ServerStateListener createServerStateListener(final ServerAddress serverAddress) {
        return new ReplicaSetServerStateListener(serverAddress);
    }


    private final class ReplicaSetServerStateListener implements ServerStateListener {
        private ServerAddress serverAddress;

        private ReplicaSetServerStateListener(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Override
        public synchronized void notify(final ServerDescription serverDescription) {
            if (isClosed()) {
                return;
            }

            markAsNotified();

            addNewHosts(serverDescription.getHosts(), true);
            addNewHosts(serverDescription.getPassives(), false);
            if (serverDescription.isPrimary()) {
                removeExtras(serverDescription);
            }

            mostRecentStateMap.put(serverAddress, serverDescription);

            updateDescription();
        }

        @Override
        public synchronized void notify(final MongoException e) {
            if (isClosed()) {
                return;
            }

            markAsNotified();

            ServerDescription serverDescription = mostRecentStateMap.remove(serverAddress);

            updateDescription();

            if (serverDescription.isPrimary()) {
                invalidateAll();
            }

        }

        private void markAsNotified() {
            if (activeMemberNotifications.containsKey(serverAddress)) {
                activeMemberNotifications.put(serverAddress, true);
            }
        }

        private void updateDescription() {
            description = new ClusterDescription(new ArrayList<ServerDescription>(mostRecentStateMap.values()), random,
                    SLAVE_ACCEPTABLE_LATENCY_MS);
            for (Iterator<Map.Entry<ServerSelector, CountDownLatch>> iter = serverPreferenceLatches.entrySet().iterator();
                 iter.hasNext();) {
                Map.Entry<ServerSelector, CountDownLatch> serverPreferenceLatch = iter.next();
                if (serverPreferenceLatch.getKey().choose(description) != null) {
                    serverPreferenceLatch.getValue().countDown();
                    iter.remove();
                }
            }
        }

        private void addNewHosts(final List<String> hosts, final boolean active) {
            for (String cur : hosts) {
                ServerAddress curServerAddress = getServerAddress(cur);
                if (curServerAddress != null) {
                    if (active && !activeMemberNotifications.containsKey(curServerAddress)) {
                        activeMemberNotifications.put(curServerAddress, false);
                    }
                    addNode(curServerAddress);
                }
            }
        }

        private void removeExtras(final ServerDescription serverDescription) {
            Set<ServerAddress> allServerAddresses = getAllServerAddresses(serverDescription);
            for (ServerAddress curServerAddress : DefaultReplicaSetCluster.this.getAllServerAddresses()) {
                if (!allServerAddresses.contains(curServerAddress)) {
                    removeNode(curServerAddress);
                    activeMemberNotifications.remove(curServerAddress);
                    mostRecentStateMap.remove(curServerAddress);
                }
            }
        }

        // TODO: move these next two methods to ServerDescription
        private Set<ServerAddress> getAllServerAddresses(final ServerDescription serverDescription) {
            Set<ServerAddress> retVal = new HashSet<ServerAddress>();
            addHostsToSet(serverDescription.getHosts(), retVal);
            addHostsToSet(serverDescription.getPassives(), retVal);
            return retVal;
        }

        private void addHostsToSet(final List<String> hosts, final Set<ServerAddress> retVal) {
            for (String host : hosts) {
                ServerAddress curServerAddress = getServerAddress(host);
                if (curServerAddress != null) {
                    retVal.add(curServerAddress);
                }
            }
        }

        private ServerAddress getServerAddress(final String serverAddressString) {
            try {
                return new ServerAddress(serverAddressString);
            } catch (UnknownHostException e) {
                return null;
            }
        }
    }
}