/*
 * Copyright 2008-present MongoDB, Inc.
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

import javax.net.SocketFactory;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoClientOptions.Builder;

/**
 * Various settings for a Mongo instance. Not thread safe, and superseded by MongoClientOptions.  This class may be deprecated in a future
 * release.
 *
 * @see MongoClientOptions
 * @see MongoClient
 * @deprecated Please use {@link MongoClientOptions} instead.
 */
@Deprecated
public class MongoOptions {

    /**
     * The description for {@code Mongo} instances created with these options. This is used in various places like logging.
     */
    public String description;

    /**
     * The maximum number of connections allowed per host for this Mongo instance. Those connections will be kept in a pool when idle. Once
     * the pool is exhausted, any operation requiring a connection will block waiting for an available connection. Default is 10.
     *
     * @see MongoOptions#threadsAllowedToBlockForConnectionMultiplier
     */
    public int connectionsPerHost;

    /**
     * This multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be waiting for a
     * connection to become available from the pool. All further threads will get an exception right away. For example if connectionsPerHost
     * is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection. Default is 5.
     */
    public int threadsAllowedToBlockForConnectionMultiplier;

    /**
     * The maximum wait time in milliseconds that a thread may wait for a connection to become available. Default is 120,000. A value of 0
     * means that it will not wait.  A negative value means to wait indefinitely.
     */
    public int maxWaitTime;

    /**
     * The connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new connection {@link
     * java.net.Socket#connect(java.net.SocketAddress, int) } Default is 10,000.
     */
    public int connectTimeout;

    /**
     * The socket timeout in milliseconds It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout(int)}
     * Default is 0 and means no timeout.
     */
    public int socketTimeout;

    /**
     * This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)} Default is false.
     */
    public boolean socketKeepAlive;

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
     * If {@code true} the driver will use a WriteConcern of WriteConcern.SAFE for all operations. If w, wtimeout, fsync or j are specified,
     * this setting is ignored. Default is false.
     */
    public boolean safe;

    /**
     * The "w" value, (number of writes), of the global WriteConcern. Default is 0.
     */
    public int w;

    /**
     * The "wtimeout" value of the global WriteConcern. Default is 0.
     */
    public int wtimeout;

    /**
     * The "fsync" value of the global WriteConcern. true indicates writes should wait for data to be written to server data file Default is
     * false.
     */
    public boolean fsync;

    /**
     * The "j" value of the global WriteConcern. true indicates writes should wait for a journaling group commit Default is false.
     */
    public boolean j;

    /**
     * Sets the socket factory for creating sockets to mongod Default is SocketFactory.getDefault()
     */
    public SocketFactory socketFactory;

    /**
     * <p>Sets whether there is a a finalize method created that cleans up instances of DBCursor that the client does not close.  If you are
     * careful to always call the close method of DBCursor, then this can safely be set to false.</p>
     *
     * <p>Default is true.</p>
     *
     * @see DBCursor#close()
     */
    public boolean cursorFinalizerEnabled;

    /**
     * Sets the write concern.  If this is not set, the write concern defaults to the combination of settings of the other write
     * concern-related fields.  If set, this will override all of the other write concern-related fields.
     *
     * @see #w
     * @see #safe
     * @see #wtimeout
     * @see #fsync
     * @see #j
     */
    public WriteConcern writeConcern;

    /**
     * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If false,
     * the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5. <p> Default is false. </p>
     */
    public boolean alwaysUseMBeans;

    String requiredReplicaSetName;

    /**
     * Creates a new default {@code MongoOptions}. This class is deprecated, use {@link com.mongodb.MongoClientOptions}.
     *
     * @deprecated use {@link com.mongodb.MongoClientOptions}
     */
    @Deprecated
    public MongoOptions() {
        reset();
    }

    /**
     * Creates a new {@code MongoOptions} with the given options. This class is deprecated, use {@link com.mongodb.MongoClientOptions}.
     *
     * @param options the MongoClientOptions to copy values from into the new MongoOptions.
     * @deprecated use {@link com.mongodb.MongoClientOptions}
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    public MongoOptions(final MongoClientOptions options) {
        connectionsPerHost = options.getConnectionsPerHost();
        threadsAllowedToBlockForConnectionMultiplier = options.getThreadsAllowedToBlockForConnectionMultiplier();
        maxWaitTime = options.getMaxWaitTime();
        connectTimeout = options.getConnectTimeout();
        socketFactory = options.getSocketFactory();
        socketTimeout = options.getSocketTimeout();
        socketKeepAlive = options.isSocketKeepAlive();
        readPreference = options.getReadPreference();
        dbDecoderFactory = options.getDbDecoderFactory();
        dbEncoderFactory = options.getDbEncoderFactory();
        description = options.getDescription();
        writeConcern = options.getWriteConcern();
        alwaysUseMBeans = options.isAlwaysUseMBeans();
        requiredReplicaSetName = options.getRequiredReplicaSetName();
    }

    /**
     * Reset all settings to the default.
     */
    public void reset() {
        connectionsPerHost = 10;
        threadsAllowedToBlockForConnectionMultiplier = 5;
        maxWaitTime = 1000 * 60 * 2;
        connectTimeout = 1000 * 10;
        socketFactory = SocketFactory.getDefault();
        socketTimeout = 0;
        socketKeepAlive = false;
        readPreference = null;
        writeConcern = null;
        safe = false;
        w = 0;
        wtimeout = 0;
        fsync = false;
        j = false;
        dbDecoderFactory = DefaultDBDecoder.FACTORY;
        dbEncoderFactory = DefaultDBEncoder.FACTORY;
        description = null;
        cursorFinalizerEnabled = true;
        alwaysUseMBeans = false;
        requiredReplicaSetName = null;
    }

    /**
     * Copy this MongoOptions instance into a new instance.
     *
     * @return the new MongoOptions with the same settings as this instance.
     */
    public MongoOptions copy() {
        MongoOptions m = new MongoOptions();
        m.connectionsPerHost = connectionsPerHost;
        m.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        m.maxWaitTime = maxWaitTime;
        m.connectTimeout = connectTimeout;
        m.socketFactory = socketFactory;
        m.socketTimeout = socketTimeout;
        m.socketKeepAlive = socketKeepAlive;
        m.readPreference = readPreference;
        m.writeConcern = writeConcern;
        m.safe = safe;
        m.w = w;
        m.wtimeout = wtimeout;
        m.fsync = fsync;
        m.j = j;
        m.dbDecoderFactory = dbDecoderFactory;
        m.dbEncoderFactory = dbEncoderFactory;
        m.description = description;
        m.cursorFinalizerEnabled = cursorFinalizerEnabled;
        m.alwaysUseMBeans = alwaysUseMBeans;
        m.requiredReplicaSetName = requiredReplicaSetName;
        return m;
    }

    @SuppressWarnings("deprecation")
    MongoClientOptions toClientOptions() {
        Builder builder = MongoClientOptions.builder()
                                            .requiredReplicaSetName(requiredReplicaSetName)
                                            .connectionsPerHost(connectionsPerHost)
                                            .connectTimeout(connectTimeout)
                                            .dbDecoderFactory(dbDecoderFactory)
                                            .dbEncoderFactory(dbEncoderFactory)
                                            .description(description)
                                            .maxWaitTime(maxWaitTime)
                                            .socketFactory(socketFactory)
                                            .socketKeepAlive(socketKeepAlive)
                                            .socketTimeout(socketTimeout)
                                            .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
                                            .cursorFinalizerEnabled(cursorFinalizerEnabled)
                                            .alwaysUseMBeans(alwaysUseMBeans);

        builder.writeConcern(getWriteConcern());

        if (readPreference != null) {
            builder.readPreference(getReadPreference());
        }
        return builder.build();
    }

    /**
     * Helper method to return the appropriate WriteConcern instance based on the current related options settings.
     *
     * @return a WriteConcern for the current MongoOptions.
     */
    @SuppressWarnings("deprecation")
    public WriteConcern getWriteConcern() {
        WriteConcern retVal;

        if (writeConcern != null) {
            retVal = writeConcern;
        } else if (w != 0 || wtimeout != 0 || fsync | j) {
            retVal = WriteConcern.ACKNOWLEDGED;
            if (w != 0) {
                retVal = retVal.withW(w);
            }
            if (wtimeout != 0) {
                retVal = retVal.withWTimeout(wtimeout, TimeUnit.MILLISECONDS);
            }
            if (fsync) {
                retVal = retVal.withFsync(fsync);
            }
            if (j) {
                retVal = retVal.withJ(j);
            }
        } else if (safe) {
            retVal = WriteConcern.ACKNOWLEDGED;
        } else {
            retVal = WriteConcern.UNACKNOWLEDGED;
        }
        return retVal;
    }

    /**
     * Sets the write concern.  If this is not set, the write concern defaults to the combination of settings of the other write
     * concern-related fields.  If set, this will override all of the other write concern-related fields.
     *
     * @param writeConcern sets the write concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the socket factory for creating sockets to mongod. Default is SocketFactory.getDefault()
     *
     * @return the socket factory for creating sockets to mongod
     */
    public synchronized SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     * Sets the socket factory for creating sockets to mongod.
     *
     * @param factory sets the socket factory for creating sockets to mongod
     */
    public synchronized void setSocketFactory(final SocketFactory factory) {
        socketFactory = factory;
    }

    /**
     * Gets the description for {@code Mongo} instances created with these options.
     *
     * @return The description for {@code MongoClient} instances created with these options
     */
    public synchronized String getDescription() {
        return description;
    }

    /**
     * Sets the description for {@code Mongo} instances created with these options. This is used in various places like logging.
     *
     * @param desc The description for {@code Mongo} instances created with these options
     */
    public synchronized void setDescription(final String desc) {
        description = desc;
    }

    /**
     * Gets the maximum number of connections allowed per host for this Mongo instance.
     *
     * @return the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    /**
     * Sets the maximum number of connections allowed per host for this Mongo instance. Those connections will be kept in a pool when idle.
     * Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection. Default is 10.
     *
     * @param connections sets the maximum number of connections allowed per host for this Mongo instance
     */
    public synchronized void setConnectionsPerHost(final int connections) {
        connectionsPerHost = connections;
    }

    /**
     * Gets the multiplier which, when multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be
     * waiting for a connection to become available from the pool.
     *
     * @return the maximum number of threads that may be waiting for a connection
     */
    public synchronized int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * Sets the multiplier which, when multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be
     * waiting for a connection to become available from the pool. All further threads will get an exception right away. For example if
     * connectionsPerHost is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.
     * Default is 5.
     *
     * @param threads multiplied with connectionsPerHost, sets the maximum number of threads that may be waiting for a connection
     */
    public synchronized void setThreadsAllowedToBlockForConnectionMultiplier(final int threads) {
        threadsAllowedToBlockForConnectionMultiplier = threads;
    }

    /**
     * Gets the maximum wait time in milliseconds that a thread may wait for a connection to become available.
     *
     * @return The maximum time in milliseconds that threads wait for a connection
     */
    public synchronized int getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * Sets the maximum wait time in milliseconds that a thread may wait for a connection to become available. Default is 120, 000. A value
     * of 0 means that it will not wait.  A negative value means to wait indefinitely.
     *
     * @param timeMS set the maximum time in milliseconds that threads wait for a connection
     */
    public synchronized void setMaxWaitTime(final int timeMS) {
        maxWaitTime = timeMS;
    }

    /**
     * Gets the connection timeout in milliseconds.  A value of 0 means no timeout.
     *
     * @return the connection timeout in milliseconds.
     */
    public synchronized int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new connection
     * {@link java.net.Socket#connect(java.net.SocketAddress, int) } Default is 10,000.
     *
     * @param timeoutMS set the connection timeout in milliseconds.
     */
    public synchronized void setConnectTimeout(final int timeoutMS) {
        connectTimeout = timeoutMS;
    }

    /**
     * Gets the socket timeout in milliseconds. 0 means no timeout.
     *
     * @return The socket timeout in milliseconds
     */
    public synchronized int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Sets the socket timeout in milliseconds It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout
     * (int)} Default is 0 and means no timeout.
     *
     * @param timeoutMS set the socket timeout in milliseconds
     */
    public synchronized void setSocketTimeout(final int timeoutMS) {
        socketTimeout = timeoutMS;
    }

    /**
     * Gets the flag that controls the socket keep alive feature that keeps a connection alive through firewalls.
     *
     * @return connection keep-alive flag
     */
    public synchronized boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * Sets the flag that controls the socket keep alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)} Default is false.
     *
     * @param keepAlive set connection keep-alive flag
     */
    public synchronized void setSocketKeepAlive(final boolean keepAlive) {
        socketKeepAlive = keepAlive;
    }

    /**
     * Gets the DBCallback factory.
     *
     * @return the DBCallback decoding factory
     */
    public synchronized DBDecoderFactory getDbDecoderFactory() {
        return dbDecoderFactory;
    }

    /**
     * Override the DBCallback factory. Default is for the standard Mongo Java driver configuration.
     *
     * @param factory sets the DBCallback decoding factory
     */
    public synchronized void setDbDecoderFactory(final DBDecoderFactory factory) {
        dbDecoderFactory = factory;
    }

    /**
     * Gets the encoding factory.
     *
     * @return the encoding factory
     */
    public synchronized DBEncoderFactory getDbEncoderFactory() {
        return dbEncoderFactory;
    }

    /**
     * Override the encoding factory. Default is for the standard Mongo Java driver configuration.
     *
     * @param factory sets the encoding factory
     */
    public synchronized void setDbEncoderFactory(final DBEncoderFactory factory) {
        dbEncoderFactory = factory;
    }

    /**
     * Returns whether the driver will use a WriteConcern of WriteConcern.ACKNOWLEDGED for all operations.
     *
     * @return true if driver uses WriteConcern.SAFE for all operations.
     */
    public synchronized boolean isSafe() {
        return safe;
    }

    /**
     * If {@code true} the driver will use a WriteConcern of WriteConcern.SAFE for all operations. If w, wtimeout, fsync or j are specified,
     * this setting is ignored. Default is false.
     *
     * @param isSafe true if driver uses WriteConcern.SAFE for all operations.
     */
    public synchronized void setSafe(final boolean isSafe) {
        safe = isSafe;
    }

    /**
     * Gets the "w" value, (number of writes), of the global WriteConcern.
     *
     * @return value returns the number of writes of the global WriteConcern.
     */
    public synchronized int getW() {
        return w;
    }

    /**
     * Sets the "w" value, (number of writes), of the global WriteConcern. Default is 0.
     *
     * @param val set the number of writes of the global WriteConcern.
     */
    public synchronized void setW(final int val) {
        w = val;
    }

    /**
     * Gets the "wtimeout" value of the global WriteConcern.
     *
     * @return timeout in millis for write operation
     */
    public synchronized int getWtimeout() {
        return wtimeout;
    }

    /**
     * Sets the "wtimeout" value of the global WriteConcern. Default is 0.
     *
     * @param timeoutMS sets timeout in millis for write operation
     */
    public synchronized void setWtimeout(final int timeoutMS) {
        wtimeout = timeoutMS;
    }

    /**
     * Gets the "fsync" value of the global WriteConcern. True indicates writes should wait for data to be written to server data file
     *
     * @return true if global write concern is set to fsync
     */
    public synchronized boolean isFsync() {
        return fsync;
    }

    /**
     * Sets the "fsync" value of the global WriteConcern. True indicates writes should wait for data to be written to server data file
     * Default is false.
     *
     * @param sync sets global write concern's fsync safe value
     */
    public synchronized void setFsync(final boolean sync) {
        fsync = sync;
    }

    /**
     * Gets the "j" value of the global WriteConcern. True indicates writes should wait for a journaling group commit
     *
     * @return true if global write concern is set to journal safe
     */
    public synchronized boolean isJ() {
        return j;
    }

    /**
     * Sets the "j" value of the global WriteConcern. True indicates writes should wait for a journaling group commit. Default is false.
     *
     * @param safe sets global write concern's journal safe value
     */
    public synchronized void setJ(final boolean safe) {
        j = safe;
    }

    /**
     * Gets the read preference.
     *
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Specifies the read preference.
     *
     * @param readPreference the read preference
     */
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    /**
     * Gets whether there is a a finalize method created that cleans up instances of DBCursor that the client does not close.
     *
     * @return whether DBCursor finalizer is enabled
     */
    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    /**
     * Sets whether there is a a finalize method created that cleans up instances of DBCursor that the client does not close.  If you are
     * careful to always call the close method of DBCursor, then this can safely be set to false.  Default is true.
     *
     * @param cursorFinalizerEnabled whether cursor finalizer is enabled
     */
    public void setCursorFinalizerEnabled(final boolean cursorFinalizerEnabled) {
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;

    }

    /**
     * Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If false,
     * the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5.
     *
     * @return true if the driver should always use MBeans, regardless of VM
     */
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    /**
     * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If false,
     * the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5. Default is false.
     *
     * @param alwaysUseMBeans sets whether the driver should always use MBeans, regardless of VM
     */
    public void setAlwaysUseMBeans(final boolean alwaysUseMBeans) {
        this.alwaysUseMBeans = alwaysUseMBeans;
    }

    /**
     * Gets the required replica set name that this client should be connecting to.
     *
     * @return the required replica set name, or null if none is required
     * @since 2.12
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoOptions options = (MongoOptions) o;

        if (alwaysUseMBeans != options.alwaysUseMBeans) {
            return false;
        }
        if (connectTimeout != options.connectTimeout) {
            return false;
        }
        if (connectionsPerHost != options.connectionsPerHost) {
            return false;
        }
        if (cursorFinalizerEnabled != options.cursorFinalizerEnabled) {
            return false;
        }
        if (fsync != options.fsync) {
            return false;
        }
        if (j != options.j) {
            return false;
        }
        if (maxWaitTime != options.maxWaitTime) {
            return false;
        }
        if (safe != options.safe) {
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
        if (w != options.w) {
            return false;
        }
        if (wtimeout != options.wtimeout) {
            return false;
        }
        if (dbDecoderFactory != null ? !dbDecoderFactory.equals(options.dbDecoderFactory) : options.dbDecoderFactory != null) {
            return false;
        }
        if (dbEncoderFactory != null ? !dbEncoderFactory.equals(options.dbEncoderFactory) : options.dbEncoderFactory != null) {
            return false;
        }
        if (description != null ? !description.equals(options.description) : options.description != null) {
            return false;
        }
        if (readPreference != null ? !readPreference.equals(options.readPreference) : options.readPreference != null) {
            return false;
        }
        if (socketFactory != null ? !socketFactory.equals(options.socketFactory) : options.socketFactory != null) {
            return false;
        }
        if (writeConcern != null ? !writeConcern.equals(options.writeConcern) : options.writeConcern != null) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(options.requiredReplicaSetName)
                                           : options.requiredReplicaSetName != null) {
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
        result = 31 * result + (alwaysUseMBeans ? 1 : 0);
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoOptions{"
               + "description='" + description + '\''
               + ", connectionsPerHost=" + connectionsPerHost
               + ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier
               + ", maxWaitTime=" + maxWaitTime
               + ", connectTimeout=" + connectTimeout
               + ", socketTimeout=" + socketTimeout
               + ", socketKeepAlive=" + socketKeepAlive
               + ", readPreference=" + readPreference
               + ", dbDecoderFactory=" + dbDecoderFactory
               + ", dbEncoderFactory=" + dbEncoderFactory
               + ", safe=" + safe
               + ", w=" + w
               + ", wtimeout=" + wtimeout
               + ", fsync=" + fsync
               + ", j=" + j
               + ", socketFactory=" + socketFactory
               + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
               + ", writeConcern=" + writeConcern
               + ", alwaysUseMBeans=" + alwaysUseMBeans
               + ", requiredReplicaSetName=" + requiredReplicaSetName
               + '}';
    }
}
