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
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.rs.Tag;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mongodb.impl.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;

class ReplicaSetStateGenerator {

    private Logger logger = Logger.getLogger("org.mongodb.ReplicaSetMonitor");
    private final Random random = new Random();
    private final float latencySmoothFactor;
    private final IsMasterExecutorFactory isMasterExecutorFactory;
    private final Map<ServerAddress, IsMasterExecutor> isMasterExecutorMap = new HashMap<ServerAddress, IsMasterExecutor>();
    private final Map<ServerAddress, ReplicaSetMember> mostRecentStateMap = new HashMap<ServerAddress, ReplicaSetMember>();

    ReplicaSetStateGenerator(final List<ServerAddress> seedList, final IsMasterExecutorFactory isMasterExecutorFactory,
                             final float latencySmoothFactor) {
        this.isMasterExecutorFactory = isMasterExecutorFactory;
        this.latencySmoothFactor = latencySmoothFactor;
        for (ServerAddress cur : seedList) {
            isMasterExecutorMap.put(cur, isMasterExecutorFactory.create(cur));
        }
    }

    void close() {
        for (IsMasterExecutor executor : isMasterExecutorMap.values()) {
            try {
                executor.close();
            } catch (final Throwable t) { // NOPMD
                // ignore
            }
        }
    }

    Map<ServerAddress, IsMasterExecutor> getIsMasterExecutorMap() {
        return isMasterExecutorMap;
    }

    ReplicaSet getReplicaSetState() {
        Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();

        updateAll(seenAddresses);
        removeUnused(seenAddresses);
        addMissingMemberClients(seenAddresses);

        return new ReplicaSet(new ArrayList<ReplicaSetMember>(mostRecentStateMap.values()), random, SLAVE_ACCEPTABLE_LATENCY_MS);
    }

    private void updateAll(final Set<ServerAddress> seenAddresses) {
        for (IsMasterExecutor executor : isMasterExecutorMap.values()) {
            seenAddresses.addAll(updateMemberState(executor));
        }
    }

    private void removeUnused(final Set<ServerAddress> seenAddresses) {
        if (!seenAddresses.isEmpty()) {
            for (Iterator<Map.Entry<ServerAddress, IsMasterExecutor>> iter =
                         isMasterExecutorMap.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<ServerAddress, IsMasterExecutor> entry = iter.next();
                if (!seenAddresses.contains(entry.getKey())) {
                    iter.remove();
                    entry.getValue().close();
                }
            }
        }
    }

    Set<ServerAddress> updateMemberState(final IsMasterExecutor executor) {
        final Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();
        try {
            IsMasterCommandResult res = executor.execute();

            addHosts(seenAddresses, res.getHosts());
            addHosts(seenAddresses, res.getPassives());

            Set<Tag> tags = getTagsFromMap(res.getTags());

            if (res.getSetName() != null) {
                logger = Logger.getLogger(logger.getName() + "." + res.getSetName());
            }

            mostRecentStateMap.put(executor.getServerAddress(), new ReplicaSetMember(executor.getServerAddress(), res.getSetName(),
                    res.getElapsedNanoseconds() / 1000000F, res.isOk(), res.isMaster(), res.isSecondary(), tags,
                    res.getMaxBSONObjectSize(), latencySmoothFactor, mostRecentStateMap.get(executor.getServerAddress())));
        } catch (Exception e) {
            mostRecentStateMap.put(executor.getServerAddress(), new ReplicaSetMember(executor.getServerAddress()));
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Exception reaching replica set member at: " + executor.getServerAddress(), e);
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
    private void updateInetAddresses() {
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
            if (!isMasterExecutorMap.containsKey(seenAddress)) {
                final IsMasterExecutor executor = isMasterExecutorFactory.create(seenAddress);
                isMasterExecutorMap.put(seenAddress, executor);
                updateMemberState(executor);
            }
        }
    }
}
