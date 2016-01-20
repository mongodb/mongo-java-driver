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

package com.mongodb;

import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.Sharded;
import static com.mongodb.ClusterType.Unknown;
import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerType.ReplicaSetGhost;
import static com.mongodb.ServerType.ShardRouter;
import static java.lang.String.format;
import static org.bson.util.Assertions.isTrue;

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

    public MultiServerCluster(final String clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                              final ClusterListener clusterListener) {
        super(clusterId, settings, serverFactory, clusterListener);
        isTrue("connection mode is multiple", settings.getMode() == Multiple);
        clusterType = settings.getRequiredClusterType();
        replicaSetName = settings.getRequiredReplicaSetName();

        LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));

        // synchronizing this code because addServer registers a callback which is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            for (ServerAddress serverAddress : settings.getHosts()) {
                addServer(serverAddress);
            }
            updateDescription();
        }
        fireChangeEvent();
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
                for (ServerTuple serverTuple : addressToServerTupleMap.values()) {
                    serverTuple.server.close();
                }
                super.close();
            }
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


    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            onChange(event);
        }
    }

    private void onChange(final ChangeEvent<ServerDescription> event) {
        boolean shouldUpdateDescription = true;
        synchronized (this) {
            if (isClosed()) {
                return;
            }

            ServerDescription newDescription = event.getNewValue();

            LOGGER.fine(format("Handling description changed event for server %s with description %s",
                    newDescription.getAddress(), newDescription));

            ServerTuple serverTuple = addressToServerTupleMap.get(newDescription.getAddress());
            if (serverTuple == null) {
                LOGGER.fine(format("Ignoring description changed event for removed server %s",
                        newDescription.getAddress()));
                return;
            }

            if (event.getNewValue().isOk()) {
                if (clusterType == Unknown && newDescription.getType() != ReplicaSetGhost) {
                    clusterType = newDescription.getClusterType();
                    LOGGER.info(format("Discovered cluster type of %s", clusterType));
                }

                switch (clusterType) {
                    case ReplicaSet:
                        shouldUpdateDescription = handleReplicaSetMemberChanged(newDescription);
                        break;
                    case Sharded:
                        shouldUpdateDescription = handleShardRouterChanged(newDescription);
                        break;
                    case StandAlone:
                        shouldUpdateDescription = handleStandAloneChanged(newDescription);
                        break;
                    default:
                        break;
                }
            }

            if (shouldUpdateDescription) {
                serverTuple.description = newDescription;
                updateDescription();
            }
        }
        if (shouldUpdateDescription) {
            fireChangeEvent();
        }
    }

    private boolean handleReplicaSetMemberChanged(final ServerDescription newDescription) {
        if (!newDescription.isReplicaSetMember()) {
            LOGGER.severe(format("Expecting replica set member, but found a %s.  Removing %s from client view of cluster.",
                                 newDescription.getType(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        if (newDescription.getType() == ReplicaSetGhost) {
            LOGGER.info(format("Server %s does not appear to be a member of an initiated replica set.", newDescription.getAddress()));
            return true;
        }

        if (replicaSetName == null) {
            replicaSetName = newDescription.getSetName();
        }

        if (!replicaSetName.equals(newDescription.getSetName())) {
            LOGGER.severe(format("Expecting replica set member from set '%s', but found one from set '%s'.  "
                                 + "Removing %s from client view of cluster.",
                                 replicaSetName, newDescription.getSetName(), newDescription.getAddress()
                                ));
            removeServer(newDescription.getAddress());
            return true;
        }

        ensureServers(newDescription);

        if (newDescription.getCanonicalAddress() != null && !newDescription.getAddress().sameHost(newDescription.getCanonicalAddress())) {
            LOGGER.info(format("Canonical address %s does not match server address.  Removing %s from client view of cluster",
                               newDescription.getCanonicalAddress(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        if (newDescription.isPrimary()) {
            if (newDescription.getSetVersion() != null && newDescription.getElectionId() != null) {
                if (isStalePrimary(newDescription)) {
                    LOGGER.info(format("Invalidating potential primary %s whose (set version, election id) tuple of (%d, %s) "
                                    + "is less than one already seen of (%d, %s)",
                            newDescription.getAddress(),
                            newDescription.getSetVersion(), newDescription.getElectionId(),
                            maxSetVersion, maxElectionId));
                    addressToServerTupleMap.get(newDescription.getAddress()).server.invalidate();
                    return false;
                }

                if (!newDescription.getElectionId().equals(maxElectionId)) {
                    LOGGER.info(format("Setting max election id to %s from replica set primary %s", newDescription.getElectionId(),
                            newDescription.getAddress()));
                    maxElectionId = newDescription.getElectionId();
                }
            }

            if (newDescription.getSetVersion() != null
                    && (maxSetVersion == null || newDescription.getSetVersion().compareTo(maxSetVersion) > 0)) {
                LOGGER.info(format("Setting max set version to %d from replica set primary %s", newDescription.getSetVersion(),
                        newDescription.getAddress()));
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
        if (newDescription.getClusterType() != Sharded) {
            LOGGER.severe(format("Expecting a %s, but found a %s.  Removing %s from client view of cluster.",
                                 ShardRouter, newDescription.getType(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
        }
        return true;
    }

    private boolean handleStandAloneChanged(final ServerDescription newDescription) {
        if (getSettings().getHosts().size() > 1) {
            LOGGER.severe(format("Expecting a single %s, but found more than one.  Removing %s from client view of cluster.",
                                 ServerType.StandAlone, newDescription.getAddress()));
            clusterType = Unknown;
            removeServer(newDescription.getAddress());
        }
        return true;
    }

    private void addServer(final ServerAddress serverAddress) {
        if (!addressToServerTupleMap.containsKey(serverAddress)) {
            LOGGER.info(format("Adding discovered server %s to client view of cluster", serverAddress));
            ClusterableServer server = createServer(serverAddress, new DefaultServerStateListener());
            addressToServerTupleMap.put(serverAddress, new ServerTuple(server,
                                                                       getConnectingServerDescription(serverAddress)));
        }
    }

    private void removeServer(final ServerAddress serverAddress) {
        ServerTuple removed = addressToServerTupleMap.remove(serverAddress);
        if (removed != null) {
            removed.server.close();
        }
    }

    private void invalidateOldPrimaries(final ServerAddress newPrimary) {
        for (ServerTuple serverTuple : addressToServerTupleMap.values()) {
            if (!serverTuple.description.getAddress().equals(newPrimary) && serverTuple.description.isPrimary()) {
                LOGGER.info(format("Rediscovering type of existing primary %s", serverTuple.description.getAddress()));
                serverTuple.server.invalidate();
            }
        }
    }

    private ServerDescription getConnectingServerDescription(final ServerAddress serverAddress) {
        return ServerDescription.builder().state(Connecting).address(serverAddress).build();
    }

    private void updateDescription() {
        final List<ServerDescription> newServerDescriptionList = getNewServerDescriptionList();
        updateDescription(new ClusterDescription(Multiple, clusterType, newServerDescriptionList));
    }

    private List<ServerDescription> getNewServerDescriptionList() {
        List<ServerDescription> serverDescriptions = new ArrayList<ServerDescription>();
        for (ServerTuple cur : addressToServerTupleMap.values()) {
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
        for (String cur : hosts) {
            try {
                addServer(new ServerAddress(cur));
            } catch (UnknownHostException e) {
                // ignore, can't happen anymore
            }
        }
    }

    private void removeExtraHosts(final ServerDescription serverDescription) {
        Set<ServerAddress> allServerAddresses = getAllServerAddresses(serverDescription);
        for (ServerTuple cur : addressToServerTupleMap.values()) {
            if (!allServerAddresses.contains(cur.description.getAddress())) {
                LOGGER.info(format("Server %s is no longer a member of the replica set.  Removing from client view of cluster.",
                                   cur.description.getAddress()));
                removeServer(cur.description.getAddress());
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
            try {
                retVal.add(new ServerAddress(host));
            } catch (UnknownHostException e) {
                // ignore, can't happen anymore
            }
        }
    }
}
