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
 *
 */

package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.MongoClosedException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.IsMasterCommandResult;
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

    private final ReplicaSetHolder replicaSetHolder = new ReplicaSetHolder();

    // will get changed to use replica set name once it's found
    private volatile Logger logger = LOGGER;

    private final List<SingleChannelMongoClient> memberClients;

    private volatile long nextResolveTime;
    private final Random random = new Random();

    static {
        SLAVE_ACCEPTABLE_LATENCY_MS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        INET_ADDR_CACHE_MS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
    }

    ReplicaSetMonitor(final List<ServerAddress> seedList, final AbstractMongoClient client) {
        super(client, "ReplicaSetMonitor");
        memberClients = new ArrayList<SingleChannelMongoClient>();
        for (ServerAddress addr : seedList) {
            memberClients.add(new SingleChannelMongoClient(addr, getMongoClient().getBufferPool(), getClientOptions()));

        }
        nextResolveTime = System.currentTimeMillis() + INET_ADDR_CACHE_MS;
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

                    ReplicaSet replicaSet = updateAll();
                    replicaSetHolder.set(replicaSet);

                    if (replicaSet.getErrorStatus().isOk() && replicaSet.hasPrimary()) {
                        curUpdateIntervalMS = getUpdaterIntervalMS();
                    }
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
        closeAllNodes();
    }

    private ReplicaSet updateAll() {
        Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();

        List<ReplicaSetMember> replicaSetMembers = new ArrayList<ReplicaSetMember>();
        for (int i = 0; i < memberClients.size(); i++) {
            replicaSetMembers.add(new ReplicaSetMemberMonitor(memberClients.get(i)).getCurrentState(seenAddresses));
            addMissingMemberClients(seenAddresses);
        }

        if (seenAddresses.size() > 0) {
            // not empty, means that at least one server gave node list, so remove unused hosts
            Iterator<SingleChannelMongoClient> it = memberClients.iterator();
            while (it.hasNext()) {
                final SingleChannelMongoClient next = it.next();
                if (!seenAddresses.contains(next.getServerAddress())) {
                    it.remove();
                    next.close();
                }
            }
        }

        return new ReplicaSet(replicaSetMembers, random, SLAVE_ACCEPTABLE_LATENCY_MS);
    }

    private void addMissingMemberClients(final Set<ServerAddress> seenAddresses) {
        for (ServerAddress seenAddress : seenAddresses) {
            boolean alreadySeen = false;
            for (SingleChannelMongoClient client : memberClients) {
                if (client.getServerAddress().equals(seenAddress)) {
                    alreadySeen = true;
                    break;
                }
            }
            if (!alreadySeen) {
                memberClients.add(new SingleChannelMongoClient(seenAddress, getMongoClient().getBufferPool(), getClientOptions()));
            }
        }
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

    // Represents the state of a member in the replica set.  Instances of this class are mutable.
    class ReplicaSetMemberMonitor extends ConnectionSetMemberMonitor {

        ReplicaSetMemberMonitor(final SingleChannelMongoClient client) {
            super(client);
        }

        ReplicaSetMember getCurrentState(final Set<ServerAddress> seenAddresses) {
            try {
                IsMasterCommandResult res = update();
                if (res.isOk()) {
                    addHosts(seenAddresses, res.getHosts());
                    addHosts(seenAddresses, res.getPassives());

                    // Tags were added in 2.0 but may not be present
                    Set<Tag> tags = getTagsFromMap(res.getTags());

                    // old versions of mongod don't report setName
                    if (res.getSetName() != null) {
                        logger = Logger.getLogger(LOGGER.getName() + "." + res.getSetName());
                    }
                    return new ReplicaSetMember(getClient().getServerAddress(), res.getSetName(), getPingTimeMS(), res.isOk(),
                            res.isMaster(), res.isSecondary(), tags, res.getMaxBsonObjectSize());
                }
            } catch (Exception e) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Server seen down: " + getClient().getServerAddress(), e);
                }
            }
            return new ReplicaSetMember(getClient().getServerAddress(), getPingTimeMS());
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
    }

    // Simple abstraction over a volatile ReplicaSet reference that starts as null.  The get method blocks until members
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    class ReplicaSetHolder {
        private volatile ReplicaSet members;

        // blocks until replica set is set, or a timeout occurs
        synchronized ReplicaSet get() {
            while (members == null) {
                try {
                    wait(getMongoClient().getOptions().getConnectTimeout());
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
                wait(getMongoClient().getOptions().getConnectTimeout());
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

    private void closeAllNodes() {
        for (SingleChannelMongoClient client : memberClients) {
            try {
                client.close();
            } catch (final Throwable t) {
                // ignore
            }
        }
    }
}

