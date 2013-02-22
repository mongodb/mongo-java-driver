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

import org.mongodb.Document;
import org.mongodb.MongoClosedException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoConnection;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.rs.Tag;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps replica set status.  Maintains a background thread to ping all members of the set to keep the status current.
 */
@ThreadSafe
class ReplicaSetMonitor extends AbstractConnectionSetMonitor {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.ReplicaSetMonitor");
    private static final int SLAVE_ACCEPTABLE_LATENCY_MS;
    private static final int INET_ADDR_CACHE_MS;

    private final ReplicaSetHolder replicaSetHolder;

    // will get changed to use replica set name once it's found
    private volatile Logger logger = LOGGER;

    private final ReplicaSetStateGenerator replicaSetStateGenerator;

    private volatile long nextResolveTime;  // TODO: use this

    static {
        SLAVE_ACCEPTABLE_LATENCY_MS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        INET_ADDR_CACHE_MS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
    }

    // TODO: do we need the connection?  Maybe just for some client options
    ReplicaSetMonitor(final List<ServerAddress> seedList, final MongoConnection connection) {
        super("ReplicaSetMonitor");
        replicaSetStateGenerator = new ReplicaSetStateGenerator(seedList,
                new MongoClientIsMasterExecutorFactory(getClientOptions()), getLatencySmoothFactor());
        nextResolveTime = System.currentTimeMillis() + INET_ADDR_CACHE_MS;
        replicaSetHolder = new ReplicaSetHolder(getClientOptions().getConnectTimeout());
    }

    ReplicaSet getCurrentState() {
        checkClosed();
        return replicaSetHolder.get();
    }


    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                int curUpdateIntervalMS = getUpdaterIntervalNoPrimaryMS();

                try {
                    updateInetAddresses();

                    ReplicaSet replicaSet = replicaSetStateGenerator.getReplicaSetState();

                    if (replicaSet.getErrorStatus().isOk() && replicaSet.hasPrimary()) {
                        curUpdateIntervalMS = getUpdaterIntervalMS();
                    }
                    replicaSetHolder.set(replicaSet);
                } catch (Exception e) {
                    // TODO: can any exceptions get through to here?
                    logger.log(Level.WARNING, "Exception in replica set monitor update pass", e);
                }

                Thread.sleep(curUpdateIntervalMS);
            }
        } catch (Exception e) {
            // Allow thread to exit
        }

        replicaSetHolder.close();
        replicaSetStateGenerator.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(", members: ").append(replicaSetHolder);
        sb.append(", updaterIntervalMS: ").append(getUpdaterIntervalMS());
        sb.append(", updaterIntervalNoMasterMS: ").append(getUpdaterIntervalNoPrimaryMS());
        sb.append(", slaveAcceptableLatencyMS: ").append(SLAVE_ACCEPTABLE_LATENCY_MS);
        sb.append(", inetAddrCacheMS: ").append(INET_ADDR_CACHE_MS);
        sb.append(", latencySmoothFactor: ").append(getLatencySmoothFactor());
        sb.append("}");

        return sb.toString();
    }

    static class ReplicaSetStateGenerator {

        static class ChannelState {
            private final IsMasterExecutor executor;
            private ReplicaSetMember mostRecentState;

            ChannelState(final IsMasterExecutor executor) {
                this.executor = executor;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }

                if (!getClass().equals(o.getClass())) {
                    return false;
                }

                final ChannelState that = (ChannelState) o;

                if (!executor.getServerAddress().equals(that.executor.getServerAddress())) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                return executor.getServerAddress().hashCode();
            }
        }

        private Logger logger = Logger.getLogger("org.mongodb.ReplicaSetMonitor");
        private final Random random = new Random();
        private final float latencySmoothFactor;
        private final IsMasterExecutorFactory isMasterExecutorFactory;
        private Set<ChannelState> channelStates = new HashSet<ChannelState>();

        ReplicaSetStateGenerator(final List<ServerAddress> seedList, final IsMasterExecutorFactory isMasterExecutorFactory,
                                 final float latencySmoothFactor) {
            this.isMasterExecutorFactory = isMasterExecutorFactory;
            this.latencySmoothFactor = latencySmoothFactor;
            for (ServerAddress cur : seedList) {
                channelStates.add(new ChannelState(isMasterExecutorFactory.create(cur)));
            }
        }

        void close() {
            for (ChannelState channelState : channelStates) {
                try {
                    channelState.executor.close();
                } catch (final Throwable t) {
                    // ignore
                }
            }
        }

        Set<ChannelState> getChannelStates() {
            return channelStates;
        }

        ReplicaSet getReplicaSetState() {
            Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();

            updateAll(seenAddresses);
            removeUnused(seenAddresses);
            addMissingMemberClients(seenAddresses);

            return new ReplicaSet(createReplicaSetMemberList(), random, SLAVE_ACCEPTABLE_LATENCY_MS);
        }

        private void updateAll(final Set<ServerAddress> seenAddresses) {
            for (ChannelState channelState : channelStates) {
                seenAddresses.addAll(updateChannelState(channelState));
            }
        }

        private void removeUnused(final Set<ServerAddress> seenAddresses) {
            if (!seenAddresses.isEmpty()) {
                for (Iterator<ChannelState> iter = channelStates.iterator(); iter.hasNext();) {
                    ChannelState channelState = iter.next();
                    if (!seenAddresses.contains(channelState.executor.getServerAddress())) {
                        iter.remove();
                        channelState.executor.close();
                    }
                }
            }
        }

        private List<ReplicaSetMember> createReplicaSetMemberList() {
            List<ReplicaSetMember> replicaSetMembers = new ArrayList<ReplicaSetMember>();
            for (ChannelState channelState : channelStates) {
                replicaSetMembers.add(channelState.mostRecentState);
            }
            return replicaSetMembers;
        }

        Set<ServerAddress> updateChannelState(final ChannelState channelState) {
            final Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();
            try {
                IsMasterCommandResult res = channelState.executor.execute();
                if (!res.isOk()) {
                    throw new MongoCommandFailureException(res);  // TODO: should the command throw this if not ok?
                }

                addHosts(seenAddresses, res.getHosts());
                addHosts(seenAddresses, res.getPassives());

                Set<Tag> tags = getTagsFromMap(res.getTags());

                if (res.getSetName() != null) {
                    logger = Logger.getLogger(LOGGER.getName() + "." + res.getSetName());
                }

                float elapsedMilliseconds = res.getElapsedNanoseconds() / 1000000F;
                float normalizedPingTimeMilliseconds =
                        channelState.mostRecentState == null || !channelState.mostRecentState.isOk()
                                ? elapsedMilliseconds
                                : channelState.mostRecentState.getPingTime()
                                + ((elapsedMilliseconds - channelState.mostRecentState.getPingTime()) / latencySmoothFactor);


                channelState.mostRecentState = new ReplicaSetMember(
                        channelState.executor.getServerAddress(), res.getSetName(),
                        normalizedPingTimeMilliseconds, res.isOk(), res.isMaster(),
                        res.isSecondary(), tags, res.getMaxBsonObjectSize());
            } catch (Exception e) {
                channelState.mostRecentState = new ReplicaSetMember(channelState.executor.getServerAddress());
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Exception reaching replica set member at: " + channelState.executor.getServerAddress(), e);
                }
            }
            return seenAddresses;
        }

        private Set<Tag> getTagsFromMap(final Document tagsDocuments) {
            if (tagsDocuments == null) {
                return Collections.emptySet();
            }
            final Set<Tag> tagSet = new HashSet<Tag>();
            for (final Map.Entry<String, Object> curEntry : tagsDocuments.entrySet()) {
                tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue().toString()));
            }
            return tagSet;
        }

        // TODO: need to cache this lookup
        private void addHosts(final Set<ServerAddress> serverAddresses, final List<String> hosts) {
            if (hosts == null) {
                return;
            }

            for (Object host : hosts) {
                try {
                    serverAddresses.add(new ServerAddress(host.toString()));
                } catch (UnknownHostException e) {
                    logger.log(Level.WARNING, "couldn't resolve host [" + host + "]");  // TODO: this will get annoying
                }
            }
        }


        // TODO: handle this
        private void updateAddr() {
//            try {
//                if (serverAddress.updateInetAddress()) {
//                    // address changed, need to use new ports
//                    port = new DBPort(addr, null, mongoOptions);
//                    mongo.getConnector().updatePortPool(addr);
//                    logger.get().log(Level.INFO, "Address of host " + serverAddress.toString() + " changed to " +
//                               serverAddress.getSocketAddress().toString());
//                }
//            } catch (UnknownHostException ex) {
//                logger.get().log(Level.WARNING, null, ex);
//            }
        }

        private void addMissingMemberClients(final Set<ServerAddress> seenAddresses) {
            for (ServerAddress seenAddress : seenAddresses) {
                boolean alreadySeen = false;
                for (ChannelState channelState : channelStates) {
                    if (channelState.executor.getServerAddress().equals(seenAddress)) {
                        alreadySeen = true;
                        break;
                    }
                }
                if (!alreadySeen) {
                    final ChannelState newChannelState = new ChannelState(isMasterExecutorFactory.create(seenAddress));
                    channelStates.add(newChannelState);
                    updateChannelState(newChannelState);
                }
            }
        }
    }

    // Simple abstraction over a volatile ReplicaSet reference that starts as null.  The get method blocks until members
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    class ReplicaSetHolder {
        private volatile ReplicaSet members;
        private final long connectTimeout;

        ReplicaSetHolder(final long connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        // blocks until replica set is set, or a timeout occurs
        synchronized ReplicaSet get() {
            while (members == null) {
                try {
                    wait(connectTimeout);
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
                }
                if (isClosed()) {
                    throw new MongoClosedException("Closed while waiting for next update to replica set status");
                }
            }
            return members;
        }

        // set the replica set to a non-null value and notifies all threads waiting.
        synchronized void set(final ReplicaSet newMembers) {
            if (newMembers == null) {
                throw new IllegalArgumentException("members can not be null");
            }

            this.members = newMembers;
            notifyAll();
        }

        // blocks until the replica set is set again
        synchronized void waitForNextUpdate() {  // TODO: currently unused
            try {
                wait(connectTimeout);
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
            }
        }

        synchronized void close() {
            this.members = null;
            notifyAll();
        }

        @Override
        public String toString() {
            ReplicaSet cur = this.members;
            if (cur != null) {
                return cur.toString();
            }
            return "none";
        }
    }

    // TODO: handle this
    private void updateInetAddresses() {
//        long now = System.currentTimeMillis();
//        if (INET_ADDR_CACHE_MS > 0 && nextResolveTime < now) {
//            nextResolveTime = now + INET_ADDR_CACHE_MS;
//            for (ReplicaSetMemberMonitor node : all) {
//                node.updateAddr();
//            }
//        }
    }
}

