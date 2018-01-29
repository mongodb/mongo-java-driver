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

import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListenerAdapter;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.REPLICA_SET_GHOST;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static java.lang.String.format;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 */
final class MultiServerCluster extends BaseCluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private ClusterType clusterType;
    private String replicaSetName;
    private ObjectId maxElectionId;
    private Integer maxSetVersion;

    private final ConcurrentMap<ServerAddress, ServerTuple> addressToServerTupleMap =
    new ConcurrentHashMap<ServerAddress, ServerTuple>();

    private static final class ServerTuple {
        private final ClusterableServer server;
        private ServerDescription description;

        private ServerTuple(final ClusterableServer server, final ServerDescription description) {
            this.server = server;
            this.description = description;
        }
    }

    MultiServerCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        super(clusterId, settings, serverFactory);
        isTrue("connection mode is multiple", settings.getMode() == ClusterConnectionMode.MULTIPLE);
        clusterType = settings.getRequiredClusterType();
        replicaSetName = settings.getRequiredReplicaSetName();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));
        }

        ClusterDescription newDescription;

        // synchronizing this code because addServer registers a callback which is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            for (final ServerAddress serverAddress : settings.getHosts()) {
                addServer(serverAddress);
            }
            newDescription = updateDescription();
        }
        fireChangeEvent(new ClusterDescriptionChangedEvent(clusterId, newDescription,
                new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, Collections.<ServerDescription>emptyList(),
                                              settings, serverFactory.getSettings())));
    }

    @Override
    protected void connect() {
        for (ServerTuple cur : addressToServerTupleMap.values()) {
            cur.server.connect();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (!isClosed()) {
                for (final ServerTuple serverTuple : addressToServerTupleMap.values()) {
                    serverTuple.server.close();
                }
            }
            super.close();
        }
    }

    @Override
    protected ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("is open", !isClosed());

        ServerTuple serverTuple = addressToServerTupleMap.get(serverAddress);
        if (serverTuple == null) {
            return null;
        }
        return serverTuple.server;
    }


    private final class DefaultServerStateListener extends ServerListenerAdapter {
        @Override
        public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
            onChange(event);
        }
    }

    private void onChange(final ServerDescriptionChangedEvent event) {
        ClusterDescription oldClusterDescription = null;
        ClusterDescription newClusterDescription = null;
        boolean shouldUpdateDescription = true;
        synchronized (this) {
            if (isClosed()) {
                return;
            }

            ServerDescription newDescription = event.getNewDescription();

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Handling description changed event for server %s with description %s",
                                    newDescription.getAddress(), newDescription));
            }

            ServerTuple serverTuple = addressToServerTupleMap.get(newDescription.getAddress());
            if (serverTuple == null) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Ignoring description changed event for removed server %s",
                                        newDescription.getAddress()));
                }
                return;
            }

            if (event.getNewDescription().isOk()) {
                if (clusterType == UNKNOWN && newDescription.getType() != REPLICA_SET_GHOST) {
                    clusterType = newDescription.getClusterType();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(format("Discovered cluster type of %s", clusterType));
                    }
                }

                switch (clusterType) {
                    case REPLICA_SET:
                        shouldUpdateDescription = handleReplicaSetMemberChanged(newDescription);
                        break;
                    case SHARDED:
                        shouldUpdateDescription = handleShardRouterChanged(newDescription);
                        break;
                    case STANDALONE:
                        shouldUpdateDescription = handleStandAloneChanged(newDescription);
                        break;
                    default:
                        break;
                }
            }

            if (shouldUpdateDescription) {
                serverTuple.description = newDescription;
                oldClusterDescription = getCurrentDescription();
                newClusterDescription = updateDescription();
            }
        }
        if (shouldUpdateDescription) {
            fireChangeEvent(new ClusterDescriptionChangedEvent(getClusterId(), newClusterDescription, oldClusterDescription));
        }
    }

    private boolean handleReplicaSetMemberChanged(final ServerDescription newDescription) {
        if (!newDescription.isReplicaSetMember()) {
            LOGGER.error(format("Expecting replica set member, but found a %s.  Removing %s from client view of cluster.",
                                newDescription.getType(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        if (newDescription.getType() == REPLICA_SET_GHOST) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Server %s does not appear to be a member of an initiated replica set.", newDescription.getAddress()));
            }
            return true;
        }

        if (replicaSetName == null) {
            replicaSetName = newDescription.getSetName();
        }

        if (!replicaSetName.equals(newDescription.getSetName())) {
            LOGGER.error(format("Expecting replica set member from set '%s', but found one from set '%s'.  "
                                 + "Removing %s from client view of cluster.",
                                 replicaSetName, newDescription.getSetName(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        ensureServers(newDescription);

        if (newDescription.getCanonicalAddress() != null
                && !newDescription.getAddress().equals(new ServerAddress(newDescription.getCanonicalAddress()))) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Canonical address %s does not match server address.  Removing %s from client view of cluster",
                                   newDescription.getCanonicalAddress(), newDescription.getAddress()));
            }
            removeServer(newDescription.getAddress());
            return true;
        }

        if (newDescription.isPrimary()) {
            if (newDescription.getSetVersion() != null && newDescription.getElectionId() != null) {
                if (isStalePrimary(newDescription)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(format("Invalidating potential primary %s whose (set version, election id) tuple of (%d, %s) "
                                + "is less than one already seen of (%d, %s)",
                                newDescription.getAddress(),
                                newDescription.getSetVersion(), newDescription.getElectionId(),
                                maxSetVersion, maxElectionId));
                    }
                    addressToServerTupleMap.get(newDescription.getAddress()).server.invalidate();
                    return false;
                }

                if (!newDescription.getElectionId().equals(maxElectionId)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(format("Setting max election id to %s from replica set primary %s", newDescription.getElectionId(),
                                newDescription.getAddress()));
                    }
                    maxElectionId = newDescription.getElectionId();
                }
            }

            if (newDescription.getSetVersion() != null
                    && (maxSetVersion == null || newDescription.getSetVersion().compareTo(maxSetVersion) > 0)) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(format("Setting max set version to %d from replica set primary %s", newDescription.getSetVersion(),
                            newDescription.getAddress()));
                }
                maxSetVersion = newDescription.getSetVersion();
            }

            if (isNotAlreadyPrimary(newDescription.getAddress())) {
                LOGGER.info(format("Discovered replica set primary %s", newDescription.getAddress()));
            }
            invalidateOldPrimaries(newDescription.getAddress());
        }
        return true;
    }

    private boolean isStalePrimary(final ServerDescription newDescription) {
        if (maxSetVersion == null || maxElectionId == null) {
            return false;
        }

        return (maxSetVersion.compareTo(newDescription.getSetVersion()) > 0
                || (maxSetVersion.equals(newDescription.getSetVersion()) && maxElectionId.compareTo(newDescription.getElectionId()) > 0));
    }

    private boolean isNotAlreadyPrimary(final ServerAddress address) {
        ServerTuple serverTuple = addressToServerTupleMap.get(address);
        return serverTuple == null || !serverTuple.description.isPrimary();
    }

    private boolean handleShardRouterChanged(final ServerDescription newDescription) {
        if (!newDescription.isShardRouter()) {
            LOGGER.error(format("Expecting a %s, but found a %s.  Removing %s from client view of cluster.",
                                 SHARD_ROUTER, newDescription.getType(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
        }
        return true;
    }

    private boolean handleStandAloneChanged(final ServerDescription newDescription) {
        if (getSettings().getHosts().size() > 1) {
            LOGGER.error(format("Expecting a single %s, but found more than one.  Removing %s from client view of cluster.",
                                 STANDALONE, newDescription.getAddress()));
            clusterType = UNKNOWN;
            removeServer(newDescription.getAddress());
        }
        return true;
    }

    private void addServer(final ServerAddress serverAddress) {
        if (!addressToServerTupleMap.containsKey(serverAddress)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Adding discovered server %s to client view of cluster", serverAddress));
            }
            ClusterableServer server = createServer(serverAddress, new DefaultServerStateListener());
            addressToServerTupleMap.put(serverAddress, new ServerTuple(server, getConnectingServerDescription(serverAddress)));
        }
    }

    private void removeServer(final ServerAddress serverAddress) {
        ServerTuple removed = addressToServerTupleMap.remove(serverAddress);
        if (removed != null) {
            removed.server.close();
        }
    }

    private void invalidateOldPrimaries(final ServerAddress newPrimary) {
        for (final ServerTuple serverTuple : addressToServerTupleMap.values()) {
            if (!serverTuple.description.getAddress().equals(newPrimary) && serverTuple.description.isPrimary()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(format("Rediscovering type of existing primary %s", serverTuple.description.getAddress()));
                }
                serverTuple.server.invalidate();
            }
        }
    }

    private ServerDescription getConnectingServerDescription(final ServerAddress serverAddress) {
        return ServerDescription.builder().state(CONNECTING).address(serverAddress).build();
    }

    private ClusterDescription updateDescription() {
        ClusterDescription newDescription = new ClusterDescription(MULTIPLE, clusterType, getNewServerDescriptionList(),
                                                                          getSettings(), getServerFactory().getSettings());
        updateDescription(newDescription);
        return newDescription;
    }

    private List<ServerDescription> getNewServerDescriptionList() {
        List<ServerDescription> serverDescriptions = new ArrayList<ServerDescription>();
        for (final ServerTuple cur : addressToServerTupleMap.values()) {
            serverDescriptions.add(cur.description);
        }
        return serverDescriptions;
    }

    private void ensureServers(final ServerDescription description) {
        if (description.isPrimary() || !hasPrimary()) {
            addNewHosts(description.getHosts());
            addNewHosts(description.getPassives());
            addNewHosts(description.getArbiters());
        }

        if (description.isPrimary()) {
            removeExtraHosts(description);
        }
    }

    private boolean hasPrimary() {
        for (ServerTuple serverTuple : addressToServerTupleMap.values()) {
            if (serverTuple.description.isPrimary()) {
                return true;
            }
        }
        return false;
    }

    private void addNewHosts(final Set<String> hosts) {
        for (final String cur : hosts) {
            addServer(new ServerAddress(cur));
        }
    }

    private void removeExtraHosts(final ServerDescription serverDescription) {
        Set<ServerAddress> allServerAddresses = getAllServerAddresses(serverDescription);
        for (Iterator<ServerTuple> iterator = addressToServerTupleMap.values().iterator(); iterator.hasNext();) {
            ServerTuple cur = iterator.next();
            if (!allServerAddresses.contains(cur.description.getAddress())) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(format("Server %s is no longer a member of the replica set.  Removing from client view of cluster.",
                                       cur.description.getAddress()));
                }
                iterator.remove();
                cur.server.close();
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
        for (final String host : hosts) {
            retVal.add(new ServerAddress(host));
        }
    }
}
