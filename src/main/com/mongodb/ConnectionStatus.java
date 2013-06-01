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

import static com.mongodb.ConnectionStatus.UpdatableNode.ConnectionState.Connected;
import static com.mongodb.ConnectionStatus.UpdatableNode.ConnectionState.Connecting;
import static com.mongodb.ConnectionStatus.UpdatableNode.ConnectionState.Unconnected;

/**
 * Base class for classes that manage connections to mongo instances as background tasks.
 */
abstract class ConnectionStatus {

    ConnectionStatus(List<ServerAddress> mongosAddresses, Mongo mongo) {
        _mongoOptions = mongoOptionsDefaults.copy();
        _mongoOptions.socketFactory = mongo._options.socketFactory;
        this._mongosAddresses = new ArrayList<ServerAddress>(mongosAddresses);
        this._mongo = mongo;
    }

    protected BackgroundUpdater _updater;
    protected final Mongo _mongo;
    protected final List<ServerAddress> _mongosAddresses;
    protected volatile boolean _closed;
    protected final MongoOptions _mongoOptions;

    protected static int updaterIntervalMS;
    protected static int updaterIntervalNoMasterMS;
    @SuppressWarnings("deprecation")
    protected static final MongoOptions mongoOptionsDefaults = new MongoOptions();
    protected static final float latencySmoothFactor;
    protected static final DBObject isMasterCmd = new BasicDBObject("ismaster", 1);

    /**
     * Start the updater if there is one
     */
    void start() {
        if (_updater != null) {
            _updater.start();
        }
    }

    /**
     * Stop the updater if there is one
     */
    void close() {
        _closed = true;
        if (_updater != null) {
            _updater.interrupt();
        }
    }

    /**
     * Gets the list of addresses for this connection.
     */
    abstract List<ServerAddress> getServerAddressList();


    /**
     * Whether there is least one server up.
     */
    abstract boolean hasServerUp();

    /**
     * Ensures that we have the current master, if there is one. If the current snapshot of the replica set
     * has no master, this method waits one cycle to find a new master, and returns it if found, or null if not.
     *
     * @return address of the current master, or null if there is none
     */
    abstract Node ensureMaster();

    /**
     * Whether this connection has been closed.
     */
    void checkClosed() {
        if (_closed)
            throw new IllegalStateException("ReplicaSetStatus closed");
    }

    static {
        updaterIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        updaterIntervalNoMasterMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10"));
        mongoOptionsDefaults.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        mongoOptionsDefaults.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
        latencySmoothFactor = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
    }

    static class Node {

        Node(float pingTime, ServerAddress addr, int maxBsonObjectSize, boolean ok) {
            this._pingTime = pingTime;
            this._addr = addr;
            this._maxBsonObjectSize = maxBsonObjectSize;
            this._ok = ok;
        }

        public boolean isOk() {
            return _ok;
        }

        public int getMaxBsonObjectSize() {
            return _maxBsonObjectSize;
        }

        public ServerAddress getServerAddress() {
            return _addr;
        }

        protected final ServerAddress _addr;
        protected final float _pingTime;
        protected final boolean _ok;
        protected final int _maxBsonObjectSize;

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Node node = (Node) o;

            if (_maxBsonObjectSize != node._maxBsonObjectSize) return false;
            if (_ok != node._ok) return false;
            if (Float.compare(node._pingTime, _pingTime) != 0) return false;
            if (!_addr.equals(node._addr)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = _addr.hashCode();
            result = 31 * result + (_pingTime != +0.0f ? Float.floatToIntBits(_pingTime) : 0);
            result = 31 * result + (_ok ? 1 : 0);
            result = 31 * result + _maxBsonObjectSize;
            return result;
        }

        public String toJSON() {
            StringBuilder buf = new StringBuilder();
            buf.append("{");
            buf.append("address:'").append(_addr).append("', ");
            buf.append("ok:").append(_ok).append(", ");
            buf.append("ping:").append(_pingTime).append(", ");
            buf.append("maxBsonObjectSize:").append(_maxBsonObjectSize).append(", ");
            buf.append("}");

            return buf.toString();
        }
    }

    static class BackgroundUpdater extends Thread {
        public BackgroundUpdater(final String name) {
            super(name);
            setDaemon(true);
        }
    }

    static abstract class UpdatableNode {

        enum ConnectionState {
            Connecting, Connected, Unconnected
        }

        UpdatableNode(final ServerAddress addr, Mongo mongo, MongoOptions mongoOptions) {
            this._addr = addr;
            this._mongo = mongo;
            this._mongoOptions = mongoOptions;
            this._port = new DBPort(addr, null, mongoOptions);
        }

        public boolean isOk() {
            return _connectionState == Connected;
        }

        public CommandResult update() {
            try {
                long start = System.nanoTime();
                CommandResult res = _port.runCommand(_mongo.getDB("admin"), isMasterCmd);

                long end = System.nanoTime();
                float newPingMS = (end - start) / 1000000F;
                if (_connectionState != Connected) {
                    _pingTimeMS = newPingMS;
                }
                else {
                    _pingTimeMS = _pingTimeMS + ((newPingMS - _pingTimeMS) / latencySmoothFactor);
                }

                _maxBsonObjectSize = res.getInt("maxBsonObjectSize", Bytes.MAX_OBJECT_SIZE);

                if (_connectionState != Connected) {
                    _connectionState = Connected;
                    getLogger().log(Level.INFO, "Server seen up: " + _addr);
                }

                getLogger().log(Level.FINE, "Latency to " + _addr + " actual=" + newPingMS + " smoothed=" + _pingTimeMS);

                return res;
            } catch (Exception e) {
                if (_connectionState != Unconnected) {
                    _connectionState = Unconnected;
                    getLogger().log(Level.WARNING, String.format("Server seen down: %s", _addr), e);
                }
                return null;
            }
        }

        protected abstract Logger getLogger();

        final ServerAddress _addr;
        final MongoOptions _mongoOptions;
        final Mongo _mongo;

        DBPort _port; // we have our own port so we can set different socket options and don't have to worry about the pool

        float _pingTimeMS = 0;
        int _maxBsonObjectSize;
        ConnectionState _connectionState = Connecting;
    }

}
