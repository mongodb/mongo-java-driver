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

// MongoOptions.java

package com.mongodb;

/**
 * Various settings for a Mongo instance. Not thread safe, and superseded by MongoClientOptions.  This class may be
 * deprecated in a future release.
 *
 * @see MongoClientOptions
 * @see MongoClient
 */
public class MongoOptions {

    public MongoOptions() {
        reset();
    }

    public MongoOptions(final MongoClientOptions options) {
        description = options.getDescription();
        connectionsPerHost = options.getConnectionsPerHost();
        threadsAllowedToBlockForConnectionMultiplier = options.getThreadsAllowedToBlockForConnectionMultiplier();
        maxWaitTime = options.getMaxWaitTime();
        connectTimeout = options.getConnectTimeout();
        socketTimeout = options.getSocketTimeout();
        socketKeepAlive = options.isSocketKeepAlive();
        autoConnectRetry = options.isAutoConnectRetry();
        maxAutoConnectRetryTime = options.getMaxAutoConnectRetryTime();
        readPreference = options.getReadPreference();
        writeConcern = options.getWriteConcern();
    }

    public void reset() {
        description = null;
        connectionsPerHost = Bytes.CONNECTIONS_PER_HOST;
        threadsAllowedToBlockForConnectionMultiplier = 5;
        maxWaitTime = 1000 * 60 * 2;
        connectTimeout = 1000 * 10;
        socketTimeout = 0;
        socketKeepAlive = false;
        autoConnectRetry = false;
        maxAutoConnectRetryTime = 0;
        readPreference = ReadPreference.primary();
        writeConcern = WriteConcern.UNACKNOWLEDGED;
    }

    public MongoOptions copy() {
        final MongoOptions m = new MongoOptions();
        m.description = description;
        m.connectionsPerHost = connectionsPerHost;
        m.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        m.maxWaitTime = maxWaitTime;
        m.connectTimeout = connectTimeout;
        m.socketTimeout = socketTimeout;
        m.socketKeepAlive = socketKeepAlive;
        m.autoConnectRetry = autoConnectRetry;
        m.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
        m.readPreference = readPreference;
        m.writeConcern = writeConcern;
        return m;
    }

    /**
     * Helper method to return the appropriate WriteConcern instance based on the current related options settings.
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MongoOptions options = (MongoOptions) o;

        if (autoConnectRetry != options.autoConnectRetry) {
            return false;
        }
        if (connectTimeout != options.connectTimeout) {
            return false;
        }
        if (connectionsPerHost != options.connectionsPerHost) {
            return false;
        }
        if (maxAutoConnectRetryTime != options.maxAutoConnectRetryTime) {
            return false;
        }
        if (maxWaitTime != options.maxWaitTime) {
            return false;
        }
        if (socketKeepAlive != options.socketKeepAlive) {
            return false;
        }
        if (socketTimeout != options.socketTimeout) {
            return false;
        }
        if (threadsAllowedToBlockForConnectionMultiplier != options.threadsAllowedToBlockForConnectionMultiplier) {
            return false;
        }
        if (description != null ? !description.equals(options.description) : options.description != null) {
            return false;
        }
        if (readPreference != null ? !readPreference.equals(options.readPreference) : options.readPreference != null) {
            return false;
        }
        if (writeConcern != null ? !writeConcern.equals(options.writeConcern) : options.writeConcern != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = description != null ? description.hashCode() : 0;
        result = 31 * result + connectionsPerHost;
        result = 31 * result + threadsAllowedToBlockForConnectionMultiplier;
        result = 31 * result + maxWaitTime;
        result = 31 * result + connectTimeout;
        result = 31 * result + socketTimeout;
        result = 31 * result + (socketKeepAlive ? 1 : 0);
        result = 31 * result + (autoConnectRetry ? 1 : 0);
        result = 31 * result + (int) (maxAutoConnectRetryTime ^ (maxAutoConnectRetryTime >>> 32));
        result = 31 * result + (readPreference != null ? readPreference.hashCode() : 0);
        result = 31 * result + (writeConcern != null ? writeConcern.hashCode() : 0);
        return result;
    }

    /**
     * <p>The description for <code>Mongo</code> instances created with these options. This is used in various places
     * like logging.</p>
     */
    public String description;

    /**
     * The maximum number of connections allowed per host for this Mongo instance. Those connections will be kept in a
     * pool when idle. Once the pool is exhausted, any operation requiring a connection will block waiting for an
     * available connection. Default is 10.
     *
     * @see {@linkplain MongoOptions#threadsAllowedToBlockForConnectionMultiplier}</p>
     */
    public int connectionsPerHost;

    /**
     * this multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be
     * waiting for a connection to become available from the pool. All further threads will get an exception right away.
     * For example if connectionsPerHost is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50
     * threads can wait for a connection. Default is 5.
     */
    public int threadsAllowedToBlockForConnectionMultiplier;

    /**
     * The maximum wait time in milliseconds that a thread may wait for a connection to become available. Default is
     * 120,000. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.
     */
    public int maxWaitTime;

    /**
     * The connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new
     * connection {@link java.net.Socket#connect(java.net.SocketAddress, int) } Default is 10,000.
     */
    public int connectTimeout;

    /**
     * The socket timeout in milliseconds It is used for I/O socket read and write operations {@link
     * java.net.Socket#setSoTimeout(int)} Default is 0 and means no timeout.
     */
    public int socketTimeout;

    /**
     * This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)} Default is false.
     */
    public boolean socketKeepAlive;

    /**
     * If true, the driver will keep trying to connect to the same server in case that the socket cannot be established.
     * There is maximum amount of time to keep retrying, which is 15s by default. This can be useful to avoid some
     * exceptions being thrown when a server is down temporarily by blocking the operations. It also can be useful to
     * smooth the transition to a new master (so that a new master is elected within the retry time). Note that when
     * using this flag: - for a replica set, the driver will trying to connect to the old master for that time, instead
     * of failing over to the new one right away - this does not prevent exception from being thrown in read/write
     * operations on the socket, which must be handled by application
     * <p/>
     * Even if this flag is false, the driver already has mechanisms to automatically recreate broken connections and
     * retry the read operations. Default is false.
     */
    public boolean autoConnectRetry;

    /**
     * The maximum amount of time in MS to spend retrying to open connection to the same server. Default is 0, which
     * means to use the default 15s if autoConnectRetry is on.
     */
    public long maxAutoConnectRetryTime;

    /**
     * Specifies the read preference.
     */
    public ReadPreference readPreference;

    /**
     * Sets the write concern.  If this is not set, the write concern defaults to {@code WriteConcern.UNACKNOWLEDGED}
     *
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public WriteConcern writeConcern;

    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("description=").append(description).append(", ");
        buf.append("connectionsPerHost=").append(connectionsPerHost).append(", ");
        buf.append("threadsAllowedToBlockForConnectionMultiplier=").append(threadsAllowedToBlockForConnectionMultiplier)
           .append(", ");
        buf.append("maxWaitTime=").append(maxWaitTime).append(", ");
        buf.append("connectTimeout=").append(connectTimeout).append(", ");
        buf.append("socketTimeout=").append(socketTimeout).append(", ");
        buf.append("socketKeepAlive=").append(socketKeepAlive).append(", ");
        buf.append("autoConnectRetry=").append(autoConnectRetry).append(", ");
        buf.append("maxAutoConnectRetryTime=").append(maxAutoConnectRetryTime).append(", ");
        if (readPreference != null) {
            buf.append("readPreference").append(readPreference);
        }
        if (writeConcern != null) {
            buf.append("writeConcern").append(writeConcern);
        }

        return buf.toString();
    }

    /**
     * @return The description for <code>MongoClient</code> instances created with these options
     */
    public synchronized String getDescription() {
        return description;
    }

    /**
     * @param desc The description for <code>Mongo</code> instances created with these options
     */
    public synchronized void setDescription(final String desc) {
        description = desc;
    }

    /**
     * @return the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    /**
     * @param connections sets the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized void setConnectionsPerHost(final int connections) {
        connectionsPerHost = connections;
    }

    /**
     * @return the maximum number of threads that may be waiting for a connection
     */
    public synchronized int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * @param threads multiplied with connectionsPerHost, sets the maximum number of threads that may be waiting for a
     *                connection
     */
    public synchronized void setThreadsAllowedToBlockForConnectionMultiplier(final int threads) {
        threadsAllowedToBlockForConnectionMultiplier = threads;
    }

    /**
     * @return The maximum time in milliseconds that threads wait for a connection
     */
    public synchronized int getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * @param timeMS set the maximum time in milliseconds that threads wait for a connection
     */
    public synchronized void setMaxWaitTime(final int timeMS) {
        maxWaitTime = timeMS;
    }

    /**
     * @return the connection timeout in milliseconds.
     */
    public synchronized int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @param timeoutMS set the connection timeout in milliseconds.
     */
    public synchronized void setConnectTimeout(final int timeoutMS) {
        connectTimeout = timeoutMS;
    }

    /**
     * @return The socket timeout in milliseconds
     */
    public synchronized int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * @param timeoutMS set the socket timeout in milliseconds
     */
    public synchronized void setSocketTimeout(final int timeoutMS) {
        socketTimeout = timeoutMS;
    }

    /**
     * @return connection keep-alive flag
     */
    public synchronized boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * @param keepAlive set connection keep-alive flag
     */
    public synchronized void setSocketKeepAlive(final boolean keepAlive) {
        socketKeepAlive = keepAlive;
    }

    /**
     * @return keep trying connection flag
     */
    public synchronized boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }

    /**
     * @param retry sets keep trying connection flag
     */
    public synchronized void setAutoConnectRetry(final boolean retry) {
        autoConnectRetry = retry;
    }

    /**
     * @return max time in MS to retrying open connection
     */
    public synchronized long getMaxAutoConnectRetryTime() {
        return maxAutoConnectRetryTime;
    }

    /**
     * @param retryTimeMS set max time in MS to retrying open connection
     */
    public synchronized void setMaxAutoConnectRetryTime(final long retryTimeMS) {
        maxAutoConnectRetryTime = retryTimeMS;
    }

    /**
     * @param writeConcern sets the write concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * @param readPreference the read preference
     */
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }
}
