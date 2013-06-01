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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Responsible for dynamically determining whether the list of server address represents a set of mongos server or
 * a replica set.  It starts threads that call the ismaster command on every server in the seed list, and as soon as it
 * reaches one determines what type of server it is.  It then creates the appropriate ConnectionStatus implementation
 * and forwards all calls to it.
 */
class DynamicConnectionStatus extends ConnectionStatus {

    private static final Logger logger = Logger.getLogger("com.mongodb.DynamicConnectionStatus");

    DynamicConnectionStatus(Mongo mongo, List<ServerAddress> mongosAddresses) {
        super(mongosAddresses, mongo);
    }

    @Override
    void start() {
        super.start();
        executorService = Executors.newFixedThreadPool(_mongosAddresses.size());
        initExecutorService();
    }

    @Override
    void close() {
        if (connectionStatus != null) {
            connectionStatus.close();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
        super.close();
    }

    ReplicaSetStatus asReplicaSetStatus() {
        ConnectionStatus connectionStatus = getConnectionStatus();
        if (connectionStatus instanceof ReplicaSetStatus) {
            return (ReplicaSetStatus) connectionStatus;
        }
        return null;
    }

    MongosStatus asMongosStatus() {
        ConnectionStatus connectionStatus = getConnectionStatus();
        if (connectionStatus instanceof MongosStatus) {
            return (MongosStatus) connectionStatus;
        }
        return null;
    }

    @Override
    List<ServerAddress> getServerAddressList() {
        if (connectionStatus != null) {
            return connectionStatus.getServerAddressList();
        } else {
            return new ArrayList<ServerAddress>(_mongosAddresses);
        }
    }

    @Override
    boolean hasServerUp() {
        ConnectionStatus connectionStatus = getConnectionStatus();
        if (connectionStatus != null) {
            return connectionStatus.hasServerUp();
        } else {
            return false;
        }
    }

    @Override
    Node ensureMaster() {
        ConnectionStatus connectionStatus = getConnectionStatus();
        if (connectionStatus != null) {
            return connectionStatus.ensureMaster();
        } else {
            return null;
        }
    }

    void initExecutorService() {
        try {
            for (final ServerAddress cur : _mongosAddresses) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        DynamicNode node = new DynamicNode(cur, _mongo, _mongoOptions);
                        try {
                            while (!Thread.interrupted()) {
                                try {
                                    node.update();
                                    if (node.isOk()) {
                                        notifyOfOkNode(node);
                                        return;
                                    }
                                } catch (Exception e) {
                                    logger.log(Level.WARNING, "couldn't reach " + node._addr, e);
                                }

                                int sleepTime = updaterIntervalNoMasterMS;
                                Thread.sleep(sleepTime);
                            }
                        } catch (InterruptedException e) {
                            // fall through
                        }
                    }
                });
            }
        } catch (RejectedExecutionException e) {
            // Ignore, as this can happen if a good node is found before all jobs are submitted and the service has
            // been shutdown.
        }
    }

    private void notifyOfOkNode(DynamicNode node) {
        synchronized (this) {
            if (connectionStatus != null) {
                return;
            }

            if (node.isMongos) {
                connectionStatus = new MongosStatus(_mongo, _mongosAddresses);
            } else {
                connectionStatus = new ReplicaSetStatus(_mongo, _mongosAddresses);
            }
            notifyAll();
        }
        connectionStatus.start();
        executorService.shutdownNow();
    }

    static class DynamicNode extends UpdatableNode {
        DynamicNode(final ServerAddress addr, Mongo mongo, MongoOptions mongoOptions) {
            super(addr, mongo, mongoOptions);
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public CommandResult update() {
            CommandResult res = super.update();

            if (res != null) {
                String msg = res.getString("msg");
                if (msg != null && msg.equals("isdbgrid")) {
                    isMongos = true;
                }
            }
            return res;
        }

        private boolean isMongos;
    }

    private synchronized ConnectionStatus getConnectionStatus() {
        if (connectionStatus == null) {
            try {
                wait(_mongo.getMongoOptions().getConnectTimeout());
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to dynamic status", e);
            }
        }
        return connectionStatus;
    }


    private volatile ConnectionStatus connectionStatus;
    private ExecutorService executorService;
}
