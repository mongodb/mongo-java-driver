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
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnection;
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.operation.MongoCommand;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

/**
 * Base class for classes that manage connections to mongo instances as background tasks.
 */
abstract class AbstractConnectionSetMonitor extends Thread {

    private volatile boolean closed;
    private final MongoClientOptions clientOptions;
    private static final int UPDATER_INTERVAL_MS;

    private static final int UPDATER_INTERVAL_NO_PRIMARY_MS;
    private static final float LATENCY_SMOOTH_FACTOR;
    private static final MongoClientOptions CLIENT_OPTIONS_DEFAULTS = MongoClientOptions.builder().build();

    AbstractConnectionSetMonitor(final String name) {
        super(name);
        setDaemon(true);
        clientOptions = CLIENT_OPTIONS_DEFAULTS;
    }

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
        UPDATER_INTERVAL_NO_PRIMARY_MS = Integer.parseInt(System
                                                          .getProperty("com.mongodb.updaterIntervalNoMasterMS", "10"));
// TODO: clean this up
//        CLIENT_OPTIONS_DEFAULTS.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
//        CLIENT_OPTIONS_DEFAULTS.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
        LATENCY_SMOOTH_FACTOR = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
    }

    protected MongoClientOptions getClientOptions() {
        return clientOptions;
    }

    interface IsMasterExecutor {
        IsMasterCommandResult execute();

        ServerAddress getServerAddress();

        void close();
    }

    static class MongoClientIsMasterExecutor implements IsMasterExecutor {
        private final MongoConnection connection;
        private final ServerAddress serverAddress;

        MongoClientIsMasterExecutor(final MongoConnection connection, final ServerAddress serverAddress) {
            this.connection = connection;
            this.serverAddress = serverAddress;
        }

        @Override
        public IsMasterCommandResult execute() {
            return new IsMasterCommandResult(
                    connection.command("admin", new MongoCommand(new Document("ismaster", 1)),
                            new DocumentSerializer(PrimitiveSerializers.createDefault())));
        }

        @Override
        public ServerAddress getServerAddress() {
            return serverAddress;
        }

        @Override
        public void close() {
            connection.close();
        }
    }

    interface IsMasterExecutorFactory {
        IsMasterExecutor create(ServerAddress serverAddress);
    }

    static class MongoClientIsMasterExecutorFactory implements IsMasterExecutorFactory {

        private final MongoClientOptions options;

        MongoClientIsMasterExecutorFactory(final MongoClientOptions options) {
            this.options = options;
        }

        @Override
        public IsMasterExecutor create(final ServerAddress serverAddress) {
            return new MongoClientIsMasterExecutor(MongoConnectionsImpl.create(serverAddress, options), serverAddress);
        }
    }

}
