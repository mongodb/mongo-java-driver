/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A connection to a set of mongos servers.
 */
class MongosStatus extends ConnectionStatus {

    private static final Logger logger = Logger.getLogger("com.mongodb.MongosStatus");

    MongosStatus(Mongo mongo, List<ServerAddress> mongosAddresses) {
        super(mongosAddresses, mongo);
        _updater = new MongosUpdater();
    }

    @Override
    boolean hasServerUp() {
        return preferred != null;
    }

    @Override
    Node ensureMaster() {
        checkClosed();
        return getPreferred();
    }


    @Override
    List<ServerAddress> getServerAddressList() {
        return new ArrayList<ServerAddress>(_mongosAddresses);
    }

    class MongosUpdater extends BackgroundUpdater {
        MongosUpdater() {
            super("MongosStatus:MongosUpdater");
        }

        @Override
        public void run() {
            List<MongosNode> mongosNodes = getMongosNodes();
            try {
                while (!Thread.interrupted()) {
                    try {
                        MongosNode bestThisPass = null;
                        for (MongosNode cur : mongosNodes) {
                            cur.update();
                            if (cur.isOk()) {
                                if (bestThisPass == null || (cur._pingTimeMS < bestThisPass._pingTimeMS)) {
                                    bestThisPass = cur;
                                }
                            }
                        }
                        setPreferred(bestThisPass);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, "couldn't do update pass", e);
                    }

                    int sleepTime = preferred == null ? updaterIntervalNoMasterMS : updaterIntervalMS;
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                logger.log(Level.INFO, "Exiting background thread");
                // Allow thread to exit
            }
        }

        private List<MongosNode> getMongosNodes() {
            List<MongosNode> mongosNodes = new ArrayList<MongosNode>(_mongosAddresses.size());
            for (ServerAddress serverAddress : _mongosAddresses) {
                mongosNodes.add(new MongosNode(serverAddress, _mongo, _mongoOptions));
            }
            return mongosNodes;
        }
    }

    static class MongosNode extends UpdatableNode {
        MongosNode(final ServerAddress addr, Mongo mongo, MongoOptions mongoOptions) {
            super(addr, mongo, mongoOptions);
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    // Sends a notification every time preferred is set.
    private synchronized void setPreferred(final MongosNode bestThisPass) {
        if (bestThisPass == null) {
            preferred = null;
        } else {
            preferred = new Node(bestThisPass._pingTimeMS, bestThisPass._addr, bestThisPass._maxBsonObjectSize, bestThisPass.isOk());
        }
        notifyAll();
    }

    // Gets the current preferred node.  If there is no preferred node, wait to get a notification before returning null.
    private synchronized Node getPreferred() {
        if (preferred == null) {
            try {
                synchronized (this) {
                    wait(_mongo.getMongoOptions().getConnectTimeout());
                }
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to mongos status", e);
            }
        }
        return preferred;
    }

    // The current preferred mongos Node to use as the master.  This is not necessarily the node that is currently in use.
    // Rather, it's the node that is preferred if there is a problem with the currently in use node.
    private volatile Node preferred;
}