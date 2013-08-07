// MongoOptions.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import javax.net.SocketFactory;

/**
 * Various settings for a Mongo instance. Not thread safe, and superseded by MongoClientOptions.  This class may
 * be deprecated in a future release.
 *
 * @see MongoClientOptions
 * @see MongoClient
 *
 * @deprecated Replaced by {@link MongoClientOptions}.
 */
@Deprecated
public class MongoOptions {

    @Deprecated
    public MongoOptions(){
        reset();
    }

    /**
     * @deprecated Replaced by {@link MongoClientOptions}
     */
    @Deprecated
    public MongoOptions(final MongoClientOptions options) {
        connectionsPerHost = options.getConnectionsPerHost();
        threadsAllowedToBlockForConnectionMultiplier = options.getThreadsAllowedToBlockForConnectionMultiplier();
        maxWaitTime = options.getMaxWaitTime();
        connectTimeout = options.getConnectTimeout();
        socketTimeout = options.getSocketTimeout();
        socketKeepAlive = options.isSocketKeepAlive();
        autoConnectRetry = options.isAutoConnectRetry();
        maxAutoConnectRetryTime = options.getMaxAutoConnectRetryTime();
        readPreference = options.getReadPreference();
        dbDecoderFactory = options.getDbDecoderFactory();
        dbEncoderFactory = options.getDbEncoderFactory();
        socketFactory = options.getSocketFactory();
        description = options.getDescription();
        cursorFinalizerEnabled = options.isCursorFinalizerEnabled();
        writeConcern = options.getWriteConcern();
        slaveOk = false; // default to false, as readPreference field will be responsible
        alwaysUseMBeans = options.isAlwaysUseMBeans();
    }

    public void reset(){
        connectionsPerHost = Bytes.CONNECTIONS_PER_HOST;
        threadsAllowedToBlockForConnectionMultiplier = 5;
        maxWaitTime = 1000 * 60 * 2;
        connectTimeout = 1000 * 10;
        socketTimeout = 0;
        socketKeepAlive = false;
        autoConnectRetry = false;
        maxAutoConnectRetryTime = 0;
        slaveOk = false;
        readPreference = null;
        writeConcern = null;
        safe = false;
        w = 0;
        wtimeout = 0;
        fsync = false;
        j = false;
        dbDecoderFactory = DefaultDBDecoder.FACTORY;
        dbEncoderFactory = DefaultDBEncoder.FACTORY;
        socketFactory = SocketFactory.getDefault();
        description = null;
        cursorFinalizerEnabled = true;
        alwaysUseMBeans = false;
    }

    public MongoOptions copy() {
        MongoOptions m = new MongoOptions();
        m.connectionsPerHost = connectionsPerHost;
        m.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        m.maxWaitTime = maxWaitTime;
        m.connectTimeout = connectTimeout;
        m.socketTimeout = socketTimeout;
        m.socketKeepAlive = socketKeepAlive;
        m.autoConnectRetry = autoConnectRetry;
        m.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
        m.slaveOk = slaveOk;
        m.readPreference = readPreference;
        m.writeConcern = writeConcern;
        m.safe = safe;
        m.w = w;
        m.wtimeout = wtimeout;
        m.fsync = fsync;
        m.j = j;
        m.dbDecoderFactory = dbDecoderFactory;
        m.dbEncoderFactory = dbEncoderFactory;
        m.socketFactory = socketFactory;
        m.description = description;
        m.cursorFinalizerEnabled = cursorFinalizerEnabled;
        m.alwaysUseMBeans = alwaysUseMBeans;
        return m;
    }

    /**
     * Helper method to return the appropriate WriteConcern instance based on the current related options settings.
     **/
    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        } else if ( w != 0 || wtimeout != 0 || fsync | j) {
            return new WriteConcern( w , wtimeout , fsync, j );
        } else if (safe) {
            return WriteConcern.SAFE;
        } else {
            return WriteConcern.NORMAL;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoOptions options = (MongoOptions) o;

        if (autoConnectRetry != options.autoConnectRetry) return false;
        if (connectTimeout != options.connectTimeout) return false;
        if (connectionsPerHost != options.connectionsPerHost) return false;
        if (cursorFinalizerEnabled != options.cursorFinalizerEnabled) return false;
        if (fsync != options.fsync) return false;
        if (j != options.j) return false;
        if (maxAutoConnectRetryTime != options.maxAutoConnectRetryTime) return false;
        if (maxWaitTime != options.maxWaitTime) return false;
        if (safe != options.safe) return false;
        if (slaveOk != options.slaveOk) return false;
        if (socketKeepAlive != options.socketKeepAlive) return false;
        if (socketTimeout != options.socketTimeout) return false;
        if (threadsAllowedToBlockForConnectionMultiplier != options.threadsAllowedToBlockForConnectionMultiplier)
            return false;
        if (w != options.w) return false;
        if (wtimeout != options.wtimeout) return false;
        if (dbDecoderFactory != null ? !dbDecoderFactory.equals(options.dbDecoderFactory) : options.dbDecoderFactory != null)
            return false;
        if (dbEncoderFactory != null ? !dbEncoderFactory.equals(options.dbEncoderFactory) : options.dbEncoderFactory != null)
            return false;
        if (description != null ? !description.equals(options.description) : options.description != null) return false;
        if (readPreference != null ? !readPreference.equals(options.readPreference) : options.readPreference != null)
            return false;
        if (socketFactory != null ? !socketFactory.equals(options.socketFactory) : options.socketFactory != null)
            return false;
        if (writeConcern != null ? !writeConcern.equals(options.writeConcern) : options.writeConcern != null)
            return false;

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
        result = 31 * result + (slaveOk ? 1 : 0);
        result = 31 * result + (readPreference != null ? readPreference.hashCode() : 0);
        result = 31 * result + (dbDecoderFactory != null ? dbDecoderFactory.hashCode() : 0);
        result = 31 * result + (dbEncoderFactory != null ? dbEncoderFactory.hashCode() : 0);
        result = 31 * result + (safe ? 1 : 0);
        result = 31 * result + w;
        result = 31 * result + wtimeout;
        result = 31 * result + (fsync ? 1 : 0);
        result = 31 * result + (j ? 1 : 0);
        result = 31 * result + (socketFactory != null ? socketFactory.hashCode() : 0);
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        result = 31 * result + (writeConcern != null ? writeConcern.hashCode() : 0);
        return result;
    }

    /**
     * <p>The description for <code>Mongo</code> instances created with these options. This is used in various places like logging.</p>
     */
    public String description;

    /**
     * The maximum number of connections allowed per host for this Mongo instance.
     * Those connections will be kept in a pool when idle.
     * Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.
     * Default is 10.
     * @see {@linkplain MongoOptions#threadsAllowedToBlockForConnectionMultiplier}</p>
     */
    public int connectionsPerHost;

    /**
     * this multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that
     * may be waiting for a connection to become available from the pool.
     * All further threads will get an exception right away.
     * For example if connectionsPerHost is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.
     * Default is 5.
     */
    public int threadsAllowedToBlockForConnectionMultiplier;

    /**
     * The maximum wait time in milliseconds that a thread may wait for a connection to become available.
     * Default is 120,000. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.
     */
    public int maxWaitTime;

    /**
     * The connection timeout in milliseconds.  A value of 0 means no timeout.
     * It is used solely when establishing a new connection {@link java.net.Socket#connect(java.net.SocketAddress, int) }
     * Default is 10,000.
     */
    public int connectTimeout;

    /**
     * The socket timeout in milliseconds
     * It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout(int)}
     * Default is 0 and means no timeout.
     */
    public int socketTimeout;

    /**
     * This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link java.net.Socket#setKeepAlive(boolean)}
     * Default is false.
     */
    public boolean socketKeepAlive;

    /**
     * If true, the driver will keep trying to connect to the same server in case that the socket cannot be established.
     * There is maximum amount of time to keep retrying, which is 15s by default.
     * This can be useful to avoid some exceptions being thrown when a server is down temporarily by blocking the operations.
     * It also can be useful to smooth the transition to a new master (so that a new master is elected within the retry time).
     * Note that when using this flag:
     * - for a replica set, the driver will trying to connect to the old master for that time, instead of failing over to the new one right away
     * - this does not prevent exception from being thrown in read/write operations on the socket, which must be handled by application
     *
     * Even if this flag is false, the driver already has mechanisms to automatically recreate broken connections and retry the read operations.
     * Default is false.
     */
    public boolean autoConnectRetry;

    /**
     * The maximum amount of time in MS to spend retrying to open connection to the same server.
     * Default is 0, which means to use the default 15s if autoConnectRetry is on.
     */
    public long maxAutoConnectRetryTime;

    /**
     * This flag specifies if the driver is allowed to read from secondary (slave) servers.
     * Specifically in the current implementation, the driver will avoid reading from the primary server and round robin requests to secondaries.
     * Driver also factors in the latency to secondaries when choosing a server.
     * Note that reading from secondaries can increase performance and reliability, but it may result in temporary inconsistent results.
     * Default is false.
     *
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     * @see ReadPreference#secondaryPreferred()
     */
    @Deprecated
    public boolean slaveOk;

    /**
     * Specifies the read preference.
     */
    public ReadPreference readPreference;

    /**
     * Override the DBCallback factory. Default is for the standard Mongo Java driver configuration.
     */
    public DBDecoderFactory dbDecoderFactory;

    /**
     * Override the encoding factory. Default is for the standard Mongo Java driver configuration.
     */
    public DBEncoderFactory dbEncoderFactory;

    /**
     * If <b>true</b> the driver will use a WriteConcern of WriteConcern.SAFE for all operations.
     * If w, wtimeout, fsync or j are specified, this setting is ignored.
     * Default is false.
     */
    public boolean safe;

    /**
     * The "w" value, (number of writes), of the global WriteConcern.
     * Default is 0.
     */
    public int w;

    /**
     * The "wtimeout" value of the global WriteConcern.
     * Default is 0.
     */
    public int wtimeout;

    /**
     * The "fsync" value of the global WriteConcern.
     * true indicates writes should wait for data to be written to server data file
     * Default is false.
     */
    public boolean fsync;

    /**
     * The "j" value of the global WriteConcern.
     * true indicates writes should wait for a journaling group commit
     * Default is false.
     */
    public boolean j;

    /**
     * sets the socket factory for creating sockets to mongod
     * Default is SocketFactory.getDefault()
     */
    public SocketFactory socketFactory;

    /**
     * Sets whether there is a a finalize method created that cleans up instances of DBCursor that the client
     * does not close.  If you are careful to always call the close method of DBCursor, then this can safely be set to false.
     * @see com.mongodb.DBCursor#close().
     * Default is true.
     */
    public boolean cursorFinalizerEnabled;

    /**
     * Sets the write concern.  If this is not set, the write concern defaults to the combination of settings of
     * the other write concern-related fields.  If set, this will override all of the other write concern-related
     * fields.
     *
     * @see #w
     * @see #safe
     * @see #wtimeout
     * @see #fsync
     * @see #j
     */
    public WriteConcern writeConcern;

    /**
     * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is
     * Java 6 or greater. If false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if
     * the VM is Java 5.
     * <p>
     *     Default is false.
     * </p>
     */
    public boolean alwaysUseMBeans;

    /**
     * @return The description for <code>MongoClient</code> instances created with these options
     */
    public synchronized String getDescription() {
        return description;
    }

    /**
     *
     * @param desc The description for <code>Mongo</code> instances created with these options
     */
    public synchronized void setDescription(String desc) {
        description = desc;
    }

    /**
     *
     * @return the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    /**
     *
     * @param connections sets the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized void setConnectionsPerHost(int connections) {
        connectionsPerHost = connections;
    }

    /**
     *
     * @return the maximum number of threads that
     * may be waiting for a connection
     */
    public synchronized int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     *
     * @param threads multiplied with connectionsPerHost, sets the maximum number of threads that
     * may be waiting for a connection
     */
    public synchronized void setThreadsAllowedToBlockForConnectionMultiplier(int threads) {
        threadsAllowedToBlockForConnectionMultiplier = threads;
    }

    /**
     *
     * @return The maximum time in milliseconds that threads wait for a connection
     */
    public synchronized int getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     *
     * @param timeMS set the maximum time in milliseconds that threads wait for a connection
     */
    public synchronized void setMaxWaitTime(int timeMS) {
        maxWaitTime = timeMS;
    }

    /**
     *
     * @return the connection timeout in milliseconds.
     */
    public synchronized int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     *
     * @param timeoutMS set the connection timeout in milliseconds.
     */
    public synchronized void setConnectTimeout(int timeoutMS) {
        connectTimeout = timeoutMS;
    }

    /**
     *
     * @return The socket timeout in milliseconds
     */
    public synchronized int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     *
     * @param timeoutMS set the socket timeout in milliseconds
     */
    public synchronized void setSocketTimeout(int timeoutMS) {
        socketTimeout = timeoutMS;
    }

    /**
     *
     * @return connection keep-alive flag
     */
    public synchronized boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     *
     * @param keepAlive set connection keep-alive flag
     */
    public synchronized void setSocketKeepAlive(boolean keepAlive) {
        socketKeepAlive = keepAlive;
    }

    /**
     *
     * @return keep trying connection flag
     */
    public synchronized boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }

    /**
     *
     * @param retry sets keep trying connection flag
     */
    public synchronized void setAutoConnectRetry(boolean retry) {
        autoConnectRetry = retry;
    }

    /**
     *
     * @return max time in MS to retrying open connection
     */
    public synchronized long getMaxAutoConnectRetryTime() {
        return maxAutoConnectRetryTime;
    }

    /**
     *
     * @param retryTimeMS set max time in MS to retrying open connection
     */
    public synchronized void setMaxAutoConnectRetryTime(long retryTimeMS) {
        maxAutoConnectRetryTime = retryTimeMS;
    }

    /**
     *
     * @return the DBCallback decoding factory
     */
    public synchronized DBDecoderFactory getDbDecoderFactory() {
        return dbDecoderFactory;
    }

    /**
     *
     * @param factory sets the DBCallback decoding factory
     */
    public synchronized void setDbDecoderFactory(DBDecoderFactory factory) {
        dbDecoderFactory = factory;
    }

    /**
     *
     * @return the encoding factory
     */
    public synchronized DBEncoderFactory getDbEncoderFactory() {
        return dbEncoderFactory;
    }

    /**
     *
     * @param factory sets the encoding factory
     */
    public synchronized void setDbEncoderFactory(DBEncoderFactory factory) {
        dbEncoderFactory = factory;
    }

    /**
     *
     * @return true if driver uses WriteConcern.SAFE for all operations.
     */
    public synchronized boolean isSafe() {
        return safe;
    }

    /**
     *
     * @param isSafe true if driver uses WriteConcern.SAFE for all operations.
     */
    public synchronized void setSafe(boolean isSafe) {
        safe = isSafe;
    }

    /**
     *
     * @return value returns the number of writes of the global WriteConcern.
     */
    public synchronized int getW() {
        return w;
    }

    /**
     *
     * @param val set the number of writes of the global WriteConcern.
     */
    public synchronized void setW(int val) {
        w = val;
    }

    /**
     *
     * @return timeout for write operation
     */
    public synchronized int getWtimeout() {
        return wtimeout;
    }

    /**
     *
     * @param timeoutMS sets timeout for write operation
     */
    public synchronized void setWtimeout(int timeoutMS) {
        wtimeout = timeoutMS;
    }

    /**
     *
     * @return true if global write concern is set to fsync
     */
    public synchronized boolean isFsync() {
        return fsync;
    }

    /**
     *
     * @param sync sets global write concern's fsync safe value
     */
    public synchronized void setFsync(boolean sync) {
        fsync = sync;
    }

    /**
     *
     * @return true if global write concern is set to journal safe
     */
    public synchronized boolean isJ() {
        return j;
    }

    /**
     *
     * @param safe sets global write concern's journal safe value
     */
    public synchronized void setJ(boolean safe) {
        j = safe;
    }

    /**
     *
     * @param writeConcern sets the write concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     *
     * @return the socket factory for creating sockets to mongod
     */
    public synchronized SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     *
     * @param factory sets the socket factory for creating sockets to mongod
     */
    public synchronized void setSocketFactory(SocketFactory factory) {
        socketFactory = factory;
    }

    /**
     *
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     *
     * @param readPreference the read preference
     */
    public void setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
    }


    /**
     *
     * @return whether DBCursor finalizer is enabled
     */
    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    /**
     *
     * @param cursorFinalizerEnabled whether cursor finalizer is enabled
     */
    public void setCursorFinalizerEnabled(final boolean cursorFinalizerEnabled) {
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;

    }

    /**
     *
     * @return true if the driver should always use MBeans, regardless of VM
     */
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    /**
     *
     * @param alwaysUseMBeans sets whether the driver should always use MBeans, regardless of VM
     */
    public void setAlwaysUseMBeans(final boolean alwaysUseMBeans) {
        this.alwaysUseMBeans = alwaysUseMBeans;
    }

    @Override
    public String toString() {
        return "MongoOptions{" +
                "description='" + description + '\'' +
                ", connectionsPerHost=" + connectionsPerHost +
                ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier +
                ", maxWaitTime=" + maxWaitTime +
                ", connectTimeout=" + connectTimeout +
                ", socketTimeout=" + socketTimeout +
                ", socketKeepAlive=" + socketKeepAlive +
                ", autoConnectRetry=" + autoConnectRetry +
                ", maxAutoConnectRetryTime=" + maxAutoConnectRetryTime +
                ", slaveOk=" + slaveOk +
                ", readPreference=" + readPreference +
                ", dbDecoderFactory=" + dbDecoderFactory +
                ", dbEncoderFactory=" + dbEncoderFactory +
                ", safe=" + safe +
                ", w=" + w +
                ", wtimeout=" + wtimeout +
                ", fsync=" + fsync +
                ", j=" + j +
                ", socketFactory=" + socketFactory +
                ", cursorFinalizerEnabled=" + cursorFinalizerEnabled +
                ", writeConcern=" + writeConcern +
                ", alwaysUseMBeans=" + alwaysUseMBeans +
                '}';
    }
}
