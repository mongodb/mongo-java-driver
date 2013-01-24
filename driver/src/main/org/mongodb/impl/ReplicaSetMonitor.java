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
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoClosedException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.result.CommandResult;
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

    private final List<ReplicaSetMemberMonitor> all;
    private volatile long nextResolveTime;
    private final Random random = new Random();

    static {
        SLAVE_ACCEPTABLE_LATENCY_MS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        INET_ADDR_CACHE_MS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
    }

    ReplicaSetMonitor(final List<ServerAddress> seedList, final AbstractMongoClient client) {
        super(client, "ReplicaSetMonitor");
        all = new ArrayList<ReplicaSetMemberMonitor>(seedList.size());
        for (ServerAddress addr : seedList) {
            all.add(new ReplicaSetMemberMonitor(addr, all, getMongoClient(), getClientOptions()));
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
                    updateAll();

                    updateInetAddresses();

                    ReplicaSet replicaSet = new ReplicaSet(createNodeList(), random, SLAVE_ACCEPTABLE_LATENCY_MS);
                    replicaSetHolder.set(replicaSet);

                    if (replicaSet.getErrorStatus().isOk() && replicaSet.hasPrimary()) {
                        curUpdateIntervalMS = getUpdaterIntervalMS();
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "couldn't do update pass", e);
                }

                Thread.sleep(curUpdateIntervalMS);
            }
        } catch (Exception e) {
            // Allow thread to exit
        }

        replicaSetHolder.close();
        closeAllNodes();
    }

    private void updateAll() {
        HashSet<ReplicaSetMemberMonitor> seenNodes = new HashSet<ReplicaSetMemberMonitor>();

        for (int i = 0; i < all.size(); i++) {
            all.get(i).update(seenNodes);
        }

        if (seenNodes.size() > 0) {
            // not empty, means that at least one server gave node list, so remove unused hosts
            Iterator<ReplicaSetMemberMonitor> it = all.iterator();
            while (it.hasNext()) {
                if (!seenNodes.contains(it.next())) {
                    it.remove();
                }
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


    // Represents the state of a member in the replica set.  Instances of this class are mutable.
    class ReplicaSetMemberMonitor extends ConnectionSetMemberMonitor {

        ReplicaSetMemberMonitor(final ServerAddress serverAddress,
                                final List<ReplicaSetMemberMonitor> all,
                                final AbstractMongoClient client,
                                final MongoClientOptions clientOptions) {
            super(serverAddress, client, clientOptions);
            this.all = all;
            names.add(this.getServerAddress().toString());
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

        void update(final Set<ReplicaSetMemberMonitor> seenNodes) {
            CommandResult commandResult = update();
            if (commandResult == null || !isOk()) {
                return;
            }

            Document res = commandResult.getResponse();
            isMaster = getBoolean(res, "ismaster", false);
            isSecondary = getBoolean(res, "secondary", false);

            if (res.containsKey("hosts")) {
                for (Object x : (List) res.get("hosts")) {
                    String host = x.toString();
                    ReplicaSetMemberMonitor node = addIfNotHere(host);
                    if (node != null && seenNodes != null) {
                        seenNodes.add(node);
                    }
                }
            }

            if (res.containsKey("passives")) {
                for (Object x : (List) res.get("passives")) {
                    String host = x.toString();
                    ReplicaSetMemberMonitor node = addIfNotHere(host);
                    if (node != null && seenNodes != null) {
                        seenNodes.add(node);
                    }
                }
            }

            // Tags were added in 2.0 but may not be present
            if (res.containsKey("tags")) {
                tags = getTagsFromMap((Document) res.get("tags"));
            }

            //old versions of mongod don't report setName
            if (res.containsKey("setName")) {
                setName = getString(res, "setName", "");
                logger = Logger.getLogger(LOGGER.getName() + "." + setName);
            }
        }

        private Set<Tag> getTagsFromMap(final Document tagsDocuments) {
            final Set<Tag> tagSet = new HashSet<Tag>();
            for (final Map.Entry<String, Object> curEntry : tagsDocuments.entrySet()) {
                tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue().toString()));
            }
            return tagSet;
        }

        private boolean getBoolean(final Document doc, final String key, final boolean defaultValue) {
            if (doc.containsKey(key)) {
                return (Boolean) doc.get(key);
            } else {
                return defaultValue;
            }
        }

        private String getString(final Document doc, final String key, final String defaultValue) {
            if (doc.containsKey(key)) {
                return (String) doc.get(key);
            } else {
                return defaultValue;
            }
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        ReplicaSetMemberMonitor addIfNotHere(final String host) {
            ReplicaSetMemberMonitor n = findNode(host, all);
            if (n == null) {
                try {
                    n = new ReplicaSetMemberMonitor(new ServerAddress(host), all, getClient(), getOptions());
                    all.add(n);
                } catch (UnknownHostException un) {
                    getLogger().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                }
            }
            return n;
        }

        private ReplicaSetMemberMonitor findNode(final String host, final List<ReplicaSetMemberMonitor> members) {
            for (ReplicaSetMemberMonitor node : members) {
                if (node.names.contains(host)) {
                    return node;
                }
            }

            ServerAddress addr;
            try {
                addr = new ServerAddress(host);
            } catch (UnknownHostException un) {
                getLogger().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                return null;
            }

            for (ReplicaSetMemberMonitor node : members) {
                if (node.getServerAddress().equals(addr)) {
                    node.names.add(host);
                    return node;
                }
            }

            return null;
        }

        void close() {
            getSingleChannelMongoClient().close();
            setSingleChannelMongoClient(null);
        }

        private final Set<String> names = Collections.synchronizedSet(new HashSet<String>());

        private Set<Tag> tags = new HashSet<Tag>();
        private boolean isMaster = false;
        private boolean isSecondary = false;
        private String setName;

        private final List<ReplicaSetMemberMonitor> all;
    }

    private List<ReplicaSetMember> createNodeList() {
        List<ReplicaSetMember> nodeList = new ArrayList<ReplicaSetMember>(all.size());
        for (ReplicaSetMemberMonitor cur : all) {
            nodeList.add(new ReplicaSetMember(cur.getServerAddress(), cur.names, cur.setName, cur.getPingTimeMS(), cur.isOk(),
                    cur.isMaster, cur.isSecondary, cur.tags, cur.getMaxBsonObjectSize()));
        }
        return nodeList;
    }

    private void updateInetAddresses() {
        long now = System.currentTimeMillis();
        if (INET_ADDR_CACHE_MS > 0 && nextResolveTime < now) {
            nextResolveTime = now + INET_ADDR_CACHE_MS;
            for (ReplicaSetMemberMonitor node : all) {
                node.updateAddr();
            }
        }
    }

    private void closeAllNodes() {
        for (ReplicaSetMemberMonitor node : all) {
            try {
                node.close();
            } catch (final Throwable t) {
                // ignore
            }
        }
    }
}

