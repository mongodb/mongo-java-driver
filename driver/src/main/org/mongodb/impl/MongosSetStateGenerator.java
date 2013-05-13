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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class MongosSetStateGenerator {
    private static final Logger LOGGER = Logger.getLogger("org.mongodb.MongosSetMonitor");
    private final float latencySmoothFactor;
    private final Map<ServerAddress, IsMasterExecutor> isMasterExecutorMap = new LinkedHashMap<ServerAddress, IsMasterExecutor>();
    private final Map<ServerAddress, MongosSetMemberDescription> mostRecentStateMap =
            new HashMap<ServerAddress, MongosSetMemberDescription>();
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
        currentMongosSet = new MongosSet(new ArrayList<MongosSetMemberDescription>(mostRecentStateMap.values()), currentMongosSet);
        return currentMongosSet;
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

    void updateMemberState(final IsMasterExecutor executor) {
        try {
            IsMasterCommandResult res = executor.execute();

            mostRecentStateMap.put(executor.getServerAddress(), new MongosSetMemberDescription(executor.getServerAddress(),
                    res.getElapsedNanoseconds() / 1000000F, res.isOk(), res.getMaxBSONObjectSize(), latencySmoothFactor,
                    mostRecentStateMap.get(executor.getServerAddress())));
        } catch (Exception e) {
            mostRecentStateMap.put(executor.getServerAddress(), new MongosSetMemberDescription(executor.getServerAddress()));
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "Exception reaching mongos at: " + executor.getServerAddress(), e);
            }
        }
    }
}
