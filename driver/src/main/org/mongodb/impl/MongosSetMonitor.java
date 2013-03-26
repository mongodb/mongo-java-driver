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

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mongodb.impl.MonitorDefaults.CLIENT_OPTIONS_DEFAULTS;
import static org.mongodb.impl.MonitorDefaults.LATENCY_SMOOTH_FACTOR;
import static org.mongodb.impl.MonitorDefaults.UPDATER_INTERVAL_MS;
import static org.mongodb.impl.MonitorDefaults.UPDATER_INTERVAL_NO_PRIMARY_MS;

public class MongosSetMonitor extends Thread {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.MongosSetMonitor");

    private final MongosSetStateGenerator mongosSetStateGenerator;
    private final Holder holder = new Holder(CLIENT_OPTIONS_DEFAULTS.getConnectTimeout());

    MongosSetMonitor(final List<ServerAddress> serverAddressList) {
        super("MongosSetMonitor: " + serverAddressList);
        setDaemon(true);
        mongosSetStateGenerator = new MongosSetStateGenerator(serverAddressList,
                new MongoConnectionIsMasterExecutorFactory(CLIENT_OPTIONS_DEFAULTS), LATENCY_SMOOTH_FACTOR);
    }

    MongosSet getCurrentState() {
        checkClosed();
        return (MongosSet) holder.get();
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                int curUpdateIntervalMS = UPDATER_INTERVAL_NO_PRIMARY_MS;

                try {
//                    updateInetAddresses();

                    MongosSet mongosSet = mongosSetStateGenerator.getMongosSetState();

                    if (mongosSet.getPreferred() != null) {
                        curUpdateIntervalMS = UPDATER_INTERVAL_MS;
                    }

                    holder.set(mongosSet);
                } catch (Exception e) {
                    // TODO: can any exceptions get through to here?
                    LOGGER.log(Level.WARNING, "Exception in mongos set monitor update pass", e);
                }

                Thread.sleep(curUpdateIntervalMS);
            }
        } catch (Exception e) { // NOPMD
            // Allow thread to exit
        } finally {
            holder.close();
            mongosSetStateGenerator.close();
        }
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
}
