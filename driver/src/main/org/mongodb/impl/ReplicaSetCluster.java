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

package org.mongodb.impl;

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoTimeoutException;
import org.mongodb.ReadPreference;
import org.mongodb.Server;
import org.mongodb.ServerAddress;
import org.mongodb.ServerDescription;
import org.mongodb.ServerFactory;
import org.mongodb.io.BufferPool;
import org.mongodb.rs.ReplicaSetDescription;
import org.mongodb.rs.ReplicaSetMemberDescription;

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
import static org.mongodb.impl.MonitorDefaults.LATENCY_SMOOTH_FACTOR;
import static org.mongodb.impl.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;

public class ReplicaSetCluster extends MultiServerCluster {

    private final Map<ServerAddress, ReplicaSetMemberDescription> mostRecentStateMap =
            new HashMap<ServerAddress, ReplicaSetMemberDescription>();
    private final Map<ServerAddress, Boolean> activeMemberNotifications = new HashMap<ServerAddress, Boolean>();
    private final Random random = new Random();
    private volatile ReplicaSetDescription description = new ReplicaSetDescription(Collections.<ReplicaSetMemberDescription>emptyList(),
            random, SLAVE_ACCEPTABLE_LATENCY_MS);
    private ConcurrentMap<ReadPreference, CountDownLatch> readPreferenceLatches = new ConcurrentHashMap<ReadPreference, CountDownLatch>();

    public ReplicaSetCluster(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                             final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                             final ServerFactory serverFactory) {
        super(seedList, credentialList, options, bufferPool, serverFactory);
        notNull("seedList", seedList);
        notNull("options", options);
        notNull("bufferPool", bufferPool);
    }

    @Override
    public Server getServer(final ReadPreference readPreference) {
        isTrue("open", !isClosed());

        return getServer(getAddressForReadPreference(readPreference));
    }

    @Override
    public void close() {
        super.close();
        for (CountDownLatch latch : readPreferenceLatches.values()) {
            latch.countDown();
        }
    }

    protected ServerStateListener createServerStateListener(final ServerAddress serverAddress) {
        return new ReplicaSetServerStateListener(serverAddress);
    }

    // CHECKSTYLE:OFF  Until we fix the underlying bug
    private ServerAddress getAddressForReadPreference(ReadPreference readPreference) {
        // CHECKSTYLE:ON

        // TODO: this is hiding potential bugs.  ReadPreference should not be null
        readPreference = readPreference == null ? ReadPreference.primary() : readPreference;

        ReplicaSetMemberDescription replicaSetMemberDescription = readPreference.chooseReplicaSetMember(description);
        if (replicaSetMemberDescription != null) {
            return replicaSetMemberDescription.getServerAddress();
        }
        else {
            try {
                CountDownLatch newLatch = new CountDownLatch(1);
                CountDownLatch existingLatch = readPreferenceLatches.putIfAbsent(readPreference, newLatch);
                final CountDownLatch latch = existingLatch != null ? existingLatch : newLatch;
                if (latch.await(5, TimeUnit.SECONDS)) {  // TODO: make timeout configurable
                   replicaSetMemberDescription = readPreference.chooseReplicaSetMember(description);
                }
                if (replicaSetMemberDescription == null) {
                    throw new MongoTimeoutException("Timed out waiting for a server that satisfies the read preference: " + readPreference);
                }
                return replicaSetMemberDescription.getServerAddress();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Thread was interrupted while waiting for a server that satisified read preference: "
                        + readPreference, e);
            }
        }
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

            mostRecentStateMap.put(serverAddress, new ReplicaSetMemberDescription(serverAddress, serverDescription,
                    LATENCY_SMOOTH_FACTOR, mostRecentStateMap.get(serverAddress)));

            updateDescription();
        }

        @Override
        public synchronized void notify(final MongoException e) {
            if (isClosed()) {
                return;
            }

            markAsNotified();

            ReplicaSetMemberDescription memberDescription = mostRecentStateMap.remove(serverAddress);

            updateDescription();

            if (memberDescription.getServerDescription().isPrimary()) {
                invalidateAll();
            }

        }

        private void markAsNotified() {
            if (activeMemberNotifications.containsKey(serverAddress)) {
                activeMemberNotifications.put(serverAddress, true);
            }
        }

        private void updateDescription() {
            description = new ReplicaSetDescription(new ArrayList<ReplicaSetMemberDescription>(mostRecentStateMap.values()), random,
                    SLAVE_ACCEPTABLE_LATENCY_MS);
            for (Iterator<Map.Entry<ReadPreference, CountDownLatch>> iter = readPreferenceLatches.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<ReadPreference, CountDownLatch> readPreferenceLatch = iter.next();
                if (readPreferenceLatch.getKey().chooseReplicaSetMember(description) != null) {
                    readPreferenceLatch.getValue().countDown();
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
            for (ServerAddress curServerAddress : ReplicaSetCluster.this.getAllServerAddresses()) {
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