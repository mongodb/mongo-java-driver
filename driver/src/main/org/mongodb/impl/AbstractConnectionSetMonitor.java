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

import org.mongodb.CommandDocument;
import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;
import org.mongodb.operation.MongoCommand;
import org.mongodb.result.CommandResult;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for classes that manage connections to mongo instances as background tasks.
 */
abstract class AbstractConnectionSetMonitor extends Thread {

    AbstractConnectionSetMonitor(final AbstractMongoClient mongoClient, final String name) {
        super(name);
        setDaemon(true);
        clientOptions = CLIENT_OPTIONS_DEFAULTS;
        this.mongoClient = mongoClient;
    }

    private final AbstractMongoClient mongoClient;
    private volatile boolean closed;
    private final MongoClientOptions clientOptions;

    private static final int UPDATER_INTERVAL_MS;
    private static final int UPDATER_INTERVAL_NO_PRIMARY_MS;
    private static final float LATENCY_SMOOTH_FACTOR;
    private static final MongoClientOptions CLIENT_OPTIONS_DEFAULTS = MongoClientOptions.builder().build();

    static int getUpdaterIntervalMS() {
        return UPDATER_INTERVAL_MS;
    }

    static int getUpdaterIntervalNoPrimaryMS() {
        return UPDATER_INTERVAL_NO_PRIMARY_MS;
    }

    static float getLatencySmoothFactor() {
        return LATENCY_SMOOTH_FACTOR;
    }

    /**
     * Stop the updater if there is one
     */
    void close() {
        closed = true;
        interrupt();
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Whether this connection has been closed.
     */
    void checkClosed() {
        if (closed) {
            throw new IllegalStateException("ReplicaSetStatus closed");  // TODO: different exception
        }
    }

    static {
        UPDATER_INTERVAL_MS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        UPDATER_INTERVAL_NO_PRIMARY_MS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10"));
// TODO: clean this up
//        CLIENT_OPTIONS_DEFAULTS.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
//        CLIENT_OPTIONS_DEFAULTS.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
        LATENCY_SMOOTH_FACTOR = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
    }

    protected AbstractMongoClient getMongoClient() {
        return mongoClient;
    }

    protected MongoClientOptions getClientOptions() {
        return clientOptions;
    }

    abstract static class ConnectionSetMemberMonitor {
        private final ServerAddress serverAddress;
        private final MongoClientOptions options;
        private final AbstractMongoClient client;

        private SingleChannelMongoClient singleChannelMongoClient;

        private boolean successfullyContacted = false;
        private boolean ok = false;
        private float pingTimeMS = 0;
        private int maxBsonObjectSize;


        ConnectionSetMemberMonitor(final ServerAddress addr, final AbstractMongoClient mongoClient,
                                   final MongoClientOptions clientOptions) {
            this.serverAddress = addr;
            this.client = mongoClient;
            this.options = clientOptions;
            this.singleChannelMongoClient = new SingleChannelMongoClient(addr, mongoClient.getBufferPool(), clientOptions);
        }

        public CommandResult update() {
            CommandResult res = null;
            try {
                long start = System.nanoTime();
                res = singleChannelMongoClient.getDatabase("admin").executeCommand(new MongoCommand(new CommandDocument("ismaster", 1)));
                long end = System.nanoTime();
                float newPingMS = (end - start) / 1000000F;
                if (!successfullyContacted) {
                    pingTimeMS = newPingMS;
                }
                else {
                    pingTimeMS = pingTimeMS + ((newPingMS - pingTimeMS) / LATENCY_SMOOTH_FACTOR);
                }
                getLogger().log(Level.FINE, "Latency to " + serverAddress + " actual=" + newPingMS + " smoothed=" + pingTimeMS);

                successfullyContacted = true;

                if (!ok) {
                    getLogger().log(Level.INFO, "Server seen up: " + serverAddress);
                }
                ok = true;

                // max size was added in 1.8
                if (res.getResponse().containsKey("maxBsonObjectSize")) {
                    maxBsonObjectSize = (Integer) res.getResponse().get("maxBsonObjectSize");
                } else {
                    maxBsonObjectSize = 16 * 1024 * 1024;  // TODO: magic number
                }
            } catch (Exception e) {
                if (!(ok || (Math.random() > 0.1))) {  // TODO: check this
                    return res;
                }

                final StringBuilder logError = (new StringBuilder("Server seen down: ")).append(serverAddress);

                getLogger().log(Level.WARNING, logError.toString(), e);
                ok = false;
            }

            return res;
        }

        protected abstract Logger getLogger();

        protected ServerAddress getServerAddress() {
            return serverAddress;
        }

        protected MongoClientOptions getOptions() {
            return options;
        }

        protected AbstractMongoClient getClient() {
            return client;
        }

        protected SingleChannelMongoClient getSingleChannelMongoClient() {
            return singleChannelMongoClient;
        }

        protected void setSingleChannelMongoClient(final SingleChannelMongoClient singleChannelMongoClient) {
            this.singleChannelMongoClient = singleChannelMongoClient;
        }

        protected boolean isOk() {
            return ok;
        }

        protected float getPingTimeMS() {
            return pingTimeMS;
        }

        protected int getMaxBsonObjectSize() {
            return maxBsonObjectSize;
        }
    }

}
