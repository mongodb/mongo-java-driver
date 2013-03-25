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
import org.mongodb.command.IsMasterCommandResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongosSetMonitor extends AbstractConnectionSetMonitor {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.MongosSetMonitor");

    private final MongosSetStateGenerator mongosSetStateGenerator;

    MongosSetMonitor(final List<ServerAddress> serverAddressList) {
        super("MongosSetMonitor: " + serverAddressList);
        mongosSetStateGenerator = new MongosSetStateGenerator(serverAddressList,
                new MongoConnectionIsMasterExecutorFactory(getClientOptions()), getLatencySmoothFactor());
    }

    MongosSet getCurrentState() {
        checkClosed();
        return (MongosSet) getHolder().get();
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                int curUpdateIntervalMS = getUpdaterIntervalNoPrimaryMS();

                try {
//                    updateInetAddresses();

                    MongosSet mongosSet = mongosSetStateGenerator.getMongosSetState();

                    if (mongosSet.getPreferred() != null) {
                        curUpdateIntervalMS = getUpdaterIntervalMS();
                    }

                    getHolder().set(mongosSet);
                } catch (Exception e) {
                    // TODO: can any exceptions get through to here?
                    LOGGER.log(Level.WARNING, "Exception in mongos set monitor update pass", e);
                }

                Thread.sleep(curUpdateIntervalMS);
            }
        } catch (Exception e) { // NOPMD
            // Allow thread to exit
        }

//        replicaSetHolder.close();
//        replicaSetStateGenerator.close();
    }


    static class MongosSetStateGenerator {
        private final float latencySmoothFactor;
        private final Map<ServerAddress, IsMasterExecutor> isMasterExecutorMap = new HashMap<ServerAddress, IsMasterExecutor>();
        private final Map<ServerAddress, MongosSetMember> mostRecentStateMap = new HashMap<ServerAddress, MongosSetMember>();
        private MongosSet currentMongosSet;

        MongosSetStateGenerator(final List<ServerAddress> serverAddressList, final IsMasterExecutorFactory isMasterExecutorFactory,
                                final float latencySmoothFactor) {
            this.latencySmoothFactor = latencySmoothFactor;
            for (ServerAddress serverAddress : serverAddressList) {
                isMasterExecutorMap.put(serverAddress, isMasterExecutorFactory.create(serverAddress));
            }
        }

        public MongosSet getMongosSetState() {
            for (IsMasterExecutor executor : isMasterExecutorMap.values()) {
                updateMemberState(executor);
            }
            currentMongosSet = new MongosSet(new ArrayList<MongosSetMember>(mostRecentStateMap.values()), currentMongosSet);
            return currentMongosSet;
        }

        Set<ServerAddress> updateMemberState(final IsMasterExecutor executor) {
            final Set<ServerAddress> seenAddresses = new HashSet<ServerAddress>();
            try {
                // TODO: this is duplicated in ReplicaSetStateGenerator
                IsMasterCommandResult res = executor.execute();

                float elapsedMilliseconds = res.getElapsedNanoseconds() / 1000000F;
                final MongosSetMember mostRecentState = mostRecentStateMap.get(executor.getServerAddress());
                float normalizedPingTimeMilliseconds = mostRecentState == null || !mostRecentState.isOk()
                        ? elapsedMilliseconds
                        : mostRecentState.getPingTime() + ((elapsedMilliseconds - mostRecentState.getPingTime()) / latencySmoothFactor);

                mostRecentStateMap.put(executor.getServerAddress(), new MongosSetMember(executor.getServerAddress(),
                        normalizedPingTimeMilliseconds, res.isOk(), res.getMaxBSONObjectSize()));
            } catch (Exception e) {
                mostRecentStateMap.put(executor.getServerAddress(), new MongosSetMember(executor.getServerAddress()));
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.log(Level.FINE, "Exception reaching mongos at: " + executor.getServerAddress(), e);
                }
            }
            return seenAddresses;
        }
    }
}
