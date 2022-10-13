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

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ServerDescriptionChangedEvent;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collection;
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
import static com.mongodb.internal.operation.ServerVersionHelper.SIX_DOT_ZERO_WIRE_VERSION;
import static java.lang.String.format;

public abstract class AbstractMultiServerCluster extends BaseCluster {
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

    AbstractMultiServerCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        super(clusterId, settings, serverFactory);
        isTrue("connection mode is multiple", settings.getMode() == ClusterConnectionMode.MULTIPLE);
        clusterType = settings.getRequiredClusterType();
        replicaSetName = settings.getRequiredReplicaSetName();
    }

    ClusterType getClusterType() {
        return clusterType;
    }

    MongoException getSrvResolutionException() {
        return null;
    }

    protected void initialize(final Collection<ServerAddress> serverAddresses) {
        ClusterDescription currentDescription = getCurrentDescription();

        // synchronizing this code because addServer registers a callback which is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        withLock(() -> {
            for (final ServerAddress serverAddress : serverAddresses) {
                addServer(serverAddress);
            }
            ClusterDescription newDescription = updateDescription();
            fireChangeEvent(newDescription, currentDescription);
        });
    }

    @Override
    protected void connect() {
        for (ServerTuple cur : addressToServerTupleMap.values()) {
            cur.server.connect();
        }
    }

    @Override
    public void close() {
        withLock(() -> {
            if (!isClosed()) {
                for (final ServerTuple serverTuple : addressToServerTupleMap.values()) {
                    serverTuple.server.close();
                }
            }
            super.close();
        });
    }

    @Override
    public ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("is open", !isClosed());

        ServerTuple serverTuple = addressToServerTupleMap.get(serverAddress);
        if (serverTuple == null) {
            return null;
        }
        return serverTuple.server;
    }

    void onChange(final Collection<ServerAddress> newHosts) {
        withLock(() -> {
            if (isClosed()) {
                return;
            }

            for (ServerAddress cur : newHosts) {
                addServer(cur);
            }

            for (Iterator<ServerTuple> iterator = addressToServerTupleMap.values().iterator(); iterator.hasNext();) {
                ServerTuple cur = iterator.next();
                if (!newHosts.contains(cur.description.getAddress())) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(format("Removing %s from client view of cluster.", cur.description.getAddress()));
                    }
                    iterator.remove();
                    cur.server.close();
                }
            }

            ClusterDescription oldClusterDescription = getCurrentDescription();
            ClusterDescription newClusterDescription = updateDescription();

            fireChangeEvent(newClusterDescription, oldClusterDescription);
        });
    }

    @Override
    public void onChange(final ServerDescriptionChangedEvent event) {
        withLock(() -> {
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

            boolean shouldUpdateDescription = true;
            if (newDescription.isOk()) {
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

            ClusterDescription oldClusterDescription = null;
            ClusterDescription newClusterDescription = null;
            if (shouldUpdateDescription) {
                serverTuple.description = newDescription;
                oldClusterDescription = getCurrentDescription();
                newClusterDescription = updateDescription();
            }
            if (shouldUpdateDescription) {
                fireChangeEvent(newClusterDescription, oldClusterDescription);
            }
        });
    }

    private boolean handleReplicaSetMemberChanged(final ServerDescription newDescription) {
        if (!newDescription.isReplicaSetMember()) {
            LOGGER.error(format("Expecting replica set member, but found a %s.  Removing %s from client view of cluster.",
                                newDescription.getType(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        if (newDescription.getType() == REPLICA_SET_GHOST) {
            LOGGER.info(format("Server %s does not appear to be a member of an initiated replica set.", newDescription.getAddress()));
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
                && !newDescription.getAddress().equals(new ServerAddress(newDescription.getCanonicalAddress()))
                && !newDescription.isPrimary()) {
            LOGGER.info(format("Canonical address %s does not match server address.  Removing %s from client view of cluster",
                    newDescription.getCanonicalAddress(), newDescription.getAddress()));
            removeServer(newDescription.getAddress());
            return true;
        }

        if (!newDescription.isPrimary()) {
            return true;
        }

        if (isStalePrimary(newDescription)) {
            invalidatePotentialPrimary(newDescription);
            return false;
        }

        maxElectionId = nullSafeMax(newDescription.getElectionId(), maxElectionId);
        maxSetVersion = nullSafeMax(newDescription.getSetVersion(), maxSetVersion);

        invalidateOldPrimaries(newDescription.getAddress());

        if (isNotAlreadyPrimary(newDescription.getAddress())) {
            LOGGER.info(format("Discovered replica set primary %s with max election id %s and max set version %d",
                    newDescription.getAddress(), newDescription.getElectionId(), newDescription.getSetVersion()));
        }

        return true;
    }

    private boolean isStalePrimary(final ServerDescription description) {
        ObjectId electionId = description.getElectionId();
        Integer setVersion = description.getSetVersion();
        if (description.getMaxWireVersion() >= SIX_DOT_ZERO_WIRE_VERSION) {
            return nullSafeCompareTo(electionId, maxElectionId) <= 0
                    && (nullSafeCompareTo(electionId, maxElectionId) != 0 || nullSafeCompareTo(setVersion, maxSetVersion) < 0);
        } else {
            return setVersion != null && electionId != null
                    && (nullSafeCompareTo(setVersion, maxSetVersion) < 0
                    || (nullSafeCompareTo(setVersion, maxSetVersion) == 0
                    && nullSafeCompareTo(electionId, maxElectionId) < 0));
        }
     }

    private void invalidatePotentialPrimary(final ServerDescription newDescription) {
        LOGGER.info(format("Invalidating potential primary %s whose (set version, election id) tuple of (%d, %s) "
                        + "is less than one already seen of (%d, %s)",
                newDescription.getAddress(), newDescription.getSetVersion(), newDescription.getElectionId(),
                maxSetVersion, maxElectionId));
        addressToServerTupleMap.get(newDescription.getAddress()).server.resetToConnecting();
    }

    /**
     * Implements the same contract as {@link Comparable#compareTo(Object)}, except that a null value is always considers less-than any
     * other value (except null, which it considers as equal-to).
     */
    private static <T extends Comparable<T>> int nullSafeCompareTo(final T first, final T second) {
        if (first == null) {
            return second == null ? 0 : -1;
        }
        if (second == null) {
            return 1;
        }
        return first.compareTo(second);
    }

    private static <T extends Comparable<T>> T nullSafeMax(final T first, final T second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first.compareTo(second) >= 0 ? first : second;
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
            ClusterableServer server = createServer(serverAddress);
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
        ClusterDescription newDescription = new ClusterDescription(MULTIPLE, clusterType, getSrvResolutionException(),
                getNewServerDescriptionList(), getSettings(), getServerFactory().getSettings());
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
