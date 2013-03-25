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
import org.mongodb.MongoClosedException;
import org.mongodb.MongoConnection;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.operation.MongoCommand;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;


// TODO: this is a lousy base class (at least it's not public)
/**
 * Base class for classes that manage connections to mongo instances as background tasks.
 */
abstract class AbstractConnectionSetMonitor extends Thread {

    static final int SLAVE_ACCEPTABLE_LATENCY_MS;
    static final int INET_ADDRESS_CACHE_MS;

    private final Holder holder;

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
        holder = new Holder(getClientOptions().getConnectTimeout());
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
        SLAVE_ACCEPTABLE_LATENCY_MS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        INET_ADDRESS_CACHE_MS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));

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

    protected Holder getHolder() {
        return  holder;
    }

    interface IsMasterExecutor {
        IsMasterCommandResult execute();

        ServerAddress getServerAddress();

        void close();
    }

    static class MongoConnectionIsMasterExecutor implements IsMasterExecutor {
        private final MongoConnection connection;
        private final ServerAddress serverAddress;

        MongoConnectionIsMasterExecutor(final MongoConnection connection, final ServerAddress serverAddress) {
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

    static class MongoConnectionIsMasterExecutorFactory implements IsMasterExecutorFactory {

        private final MongoClientOptions options;

        MongoConnectionIsMasterExecutorFactory(final MongoClientOptions options) {
            this.options = options;
        }

        @Override
        public IsMasterExecutor create(final ServerAddress serverAddress) {
            return new MongoConnectionIsMasterExecutor(MongoConnectionsImpl.create(serverAddress, options), serverAddress);
        }
    }

    // Simple abstraction over a volatile Object reference that starts as null.  The get method blocks until held
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    class Holder {
        private volatile Object held;
        private final long connectTimeout;

        Holder(final long connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        // blocks until replica set is set, or a timeout occurs
        synchronized Object get() {
            while (held == null) {
                try {
                    wait(connectTimeout);
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
                }
                if (isClosed()) {
                    throw new MongoClosedException("Closed while waiting for next update to replica set status");
                }
            }
            return held;
        }

        // set the replica set to a non-null value and notifies all threads waiting.
        synchronized void set(final Object newHeld) {
            if (newHeld == null) {
                throw new IllegalArgumentException("held can not be null");
            }

            this.held = newHeld;
            notifyAll();
        }

        // blocks until the replica set is set again
        synchronized void waitForNextUpdate() {  // TODO: currently unused
            try {
                wait(connectTimeout);
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
            }
        }

        synchronized void close() {
            this.held = null;
            notifyAll();
        }

        @Override
        public String toString() {
            Object cur = this.held;
            if (cur != null) {
                return cur.toString();
            }
            return "none";
        }
    }
}
