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

import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.rs.ReplicaSet;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mongodb.impl.MonitorDefaults.CLIENT_OPTIONS_DEFAULTS;
import static org.mongodb.impl.MonitorDefaults.INET_ADDRESS_CACHE_MS;
import static org.mongodb.impl.MonitorDefaults.LATENCY_SMOOTH_FACTOR;
import static org.mongodb.impl.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;
import static org.mongodb.impl.MonitorDefaults.UPDATER_INTERVAL_MS;
import static org.mongodb.impl.MonitorDefaults.UPDATER_INTERVAL_NO_PRIMARY_MS;

/**
 * Keeps replica set status.  Maintains a background thread to ping all held of the set to keep the status current.
 */
@ThreadSafe
class ReplicaSetMonitor extends Thread {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.ReplicaSetMonitor");

    // will get changed to use replica set name once it's found
    private volatile Logger logger = LOGGER;

    private final ReplicaSetStateGenerator replicaSetStateGenerator;

    // private volatile long nextResolveTime;  // TODO: use this

    private final Holder holder = new Holder(CLIENT_OPTIONS_DEFAULTS.getConnectTimeout());

    ReplicaSetMonitor(final List<ServerAddress> seedList) {
        super("ReplicaSetMonitor: " + seedList);
        setDaemon(true);
        replicaSetStateGenerator = new ReplicaSetStateGenerator(seedList,
                new MongoConnectionIsMasterExecutorFactory(CLIENT_OPTIONS_DEFAULTS), LATENCY_SMOOTH_FACTOR);
//        nextResolveTime = System.currentTimeMillis() + INET_ADDRESS_CACHE_MS;
    }

    ReplicaSet getCurrentState() {
        checkClosed();
        return (ReplicaSet) holder.get();
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                int curUpdateIntervalMS = UPDATER_INTERVAL_NO_PRIMARY_MS;

                try {
                    updateInetAddresses();

                    ReplicaSet replicaSet = replicaSetStateGenerator.getReplicaSetState();

                    if (replicaSet.getErrorStatus().isOk() && replicaSet.hasPrimary()) {
                        curUpdateIntervalMS = UPDATER_INTERVAL_MS;
                    }
                    holder.set(replicaSet);
                } catch (Exception e) {
                    // TODO: can any exceptions get through to here?
                    logger.log(Level.WARNING, "Exception in replica set monitor update pass", e);
                }

                Thread.sleep(curUpdateIntervalMS);
            }
        } catch (Exception e) { // NOPMD
            // Allow thread to exit
        }

        holder.close();
        replicaSetStateGenerator.close();
    }

    /**
     * Stop the updater if there is one
     */
    void close() {
        holder.close();
        interrupt();
    }

    /**
     * Whether this connection has been closed.
     */
    void checkClosed() {
        if (holder.isClosed()) {
            throw new IllegalStateException("ReplicaSetStatus closed");  // TODO: different exception
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(", members: ").append(holder);
        sb.append(", updaterIntervalMS: ").append(UPDATER_INTERVAL_MS);
        sb.append(", updaterIntervalNoMasterMS: ").append(UPDATER_INTERVAL_NO_PRIMARY_MS);
        sb.append(", slaveAcceptableLatencyMS: ").append(SLAVE_ACCEPTABLE_LATENCY_MS);
        sb.append(", inetAddressCacheMS: ").append(INET_ADDRESS_CACHE_MS);
        sb.append(", latencySmoothFactor: ").append(LATENCY_SMOOTH_FACTOR);
        sb.append("}");

        return sb.toString();
    }

    // TODO: handle this
    private void updateInetAddresses() {
//        long now = System.currentTimeMillis();
//        if (INET_ADDRESS_CACHE_MS > 0 && nextResolveTime < now) {
//            nextResolveTime = now + INET_ADDRESS_CACHE_MS;
//            for (ReplicaSetMemberMonitor node : all) {
//                node.updateAddr();
//            }
//        }
    }
}

