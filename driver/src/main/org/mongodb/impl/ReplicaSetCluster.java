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
import org.mongodb.MongoReadPreferenceException;
import org.mongodb.ReadPreference;
import org.mongodb.Server;
import org.mongodb.ServerAddress;
import org.mongodb.ServerFactory;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.io.BufferPool;
import org.mongodb.rs.ReplicaSetDescription;
import org.mongodb.rs.ReplicaSetMemberDescription;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.impl.MonitorDefaults.CLIENT_OPTIONS_DEFAULTS;
import static org.mongodb.impl.MonitorDefaults.LATENCY_SMOOTH_FACTOR;
import static org.mongodb.impl.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;

public class ReplicaSetCluster extends MultiServerCluster {

    private final Holder<ReplicaSetDescription> holder = new Holder<ReplicaSetDescription>(CLIENT_OPTIONS_DEFAULTS.getConnectTimeout(),
            TimeUnit.MILLISECONDS);
    private final Map<ServerAddress, ReplicaSetMemberDescription> mostRecentStateMap =
            new HashMap<ServerAddress, ReplicaSetMemberDescription>();
    private final Map<ServerAddress, Boolean> activeMemberNotifications = new HashMap<ServerAddress, Boolean>();
    private final Random random = new Random();

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
        holder.close();
        super.close();
    }

    protected MongoServerStateListener createServerStateListener(final ServerAddress serverAddress) {
        return new ReplicaSetServerStateListener(serverAddress);
    }

    private ServerAddress getAddressForReadPreference(final ReadPreference readPreference) {
        // TODO: this is hiding potential bugs.  ReadPreference should not be null
        ReadPreference appliedReadPreference = readPreference == null ? ReadPreference.primary() : readPreference;
        final ReplicaSetDescription replicaSetDescription = holder.get();
        final ReplicaSetMemberDescription replicaSetMemberDescription = appliedReadPreference.chooseReplicaSetMember(replicaSetDescription);
        if (replicaSetMemberDescription == null) {
            throw new MongoReadPreferenceException(readPreference, replicaSetDescription);
        }
        return replicaSetMemberDescription.getServerAddress();
    }

    private final class ReplicaSetServerStateListener implements MongoServerStateListener {
        private ServerAddress serverAddress;

        private ReplicaSetServerStateListener(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Override
        public synchronized void notify(final IsMasterCommandResult isMasterCommandResult) {
            if (isClosed()) {
                return;
            }

            if (getServer(serverAddress) == null) {
                return;
            }

            markAsNotified();

            addNewHosts(isMasterCommandResult.getHosts(), true);
            addNewHosts(isMasterCommandResult.getPassives(), false);
            if (isMasterCommandResult.isPrimary()) {
                removeExtras(isMasterCommandResult);
            }

            mostRecentStateMap.put(serverAddress, new ReplicaSetMemberDescription(serverAddress, isMasterCommandResult,
                    LATENCY_SMOOTH_FACTOR, mostRecentStateMap.get(serverAddress)));

            setHolder();
        }

        @Override
        public synchronized void notify(final MongoException e) {
            if (isClosed()) {
                return;
            }

            if (getServer(serverAddress) == null) {
                return;
            }

            markAsNotified();

            ReplicaSetMemberDescription description = mostRecentStateMap.remove(serverAddress);

            setHolder();

            if (description.primary()) {
                invalidateAll();
            }

        }

        private void markAsNotified() {
            if (activeMemberNotifications.containsKey(serverAddress)) {
                activeMemberNotifications.put(serverAddress, true);
            }
        }

        private void setHolder() {
            holder.set(new ReplicaSetDescription(new ArrayList<ReplicaSetMemberDescription>(mostRecentStateMap.values()), random,
                    SLAVE_ACCEPTABLE_LATENCY_MS));
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

        private void removeExtras(final IsMasterCommandResult isMasterCommandResult) {
            Set<ServerAddress> allServerAddresses = getAllServerAddresses(isMasterCommandResult);
            for (ServerAddress curServerAddress : ReplicaSetCluster.this.getAllServerAddresses()) {
                if (!allServerAddresses.contains(curServerAddress)) {
                    removeNode(curServerAddress);
                    activeMemberNotifications.remove(curServerAddress);
                    mostRecentStateMap.remove(curServerAddress);
                }
            }
        }

        // TODO: move these next two methods
        private Set<ServerAddress> getAllServerAddresses(final IsMasterCommandResult masterCommandResult) {
            Set<ServerAddress> retVal = new HashSet<ServerAddress>();
            addHostsToSet(masterCommandResult.getHosts(), retVal);
            addHostsToSet(masterCommandResult.getPassives(), retVal);
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