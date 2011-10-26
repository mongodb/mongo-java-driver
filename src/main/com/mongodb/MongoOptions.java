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
 * Various settings for the driver
 */
public class MongoOptions {

    public MongoOptions(){
        reset();
    }

    public void reset(){
        connectionsPerHost = Bytes.CONNECTIONS_PER_HOST;
        threadsAllowedToBlockForConnectionMultiplier = 5;
        maxWaitTime = 1000 * 60 * 2;
        connectTimeout = 0;
        socketTimeout = 0;
        socketKeepAlive = false;
        autoConnectRetry = false;
        maxAutoConnectRetryTime = 0;
        slaveOk = false;
        safe = false;
        w = 0;
        wtimeout = 0;
        fsync = false;
        j = false;
        dbDecoderFactory = DefaultDBDecoder.FACTORY;
        dbEncoderFactory = DefaultDBEncoder.FACTORY;
        socketFactory = SocketFactory.getDefault();
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
        m.safe = safe;
        m.w = w;
        m.wtimeout = wtimeout;
        m.fsync = fsync;
        m.j = j;
        m.dbDecoderFactory = dbDecoderFactory;
        m.dbEncoderFactory = dbEncoderFactory;
        m.socketFactory = socketFactory;
        return m;
    }

    /**
     * Helper method to return the appropriate WriteConcern instance based
     * on the current related options settings.
     **/
    public WriteConcern getWriteConcern(){
        // Ensure we only set writeconcern once; if non-default w, etc skip safe (implied)
        if ( w != 0 || wtimeout != 0 || fsync )
            return new WriteConcern( w , wtimeout , fsync );
        else if (safe)
            return WriteConcern.SAFE;
        else
            return WriteConcern.NORMAL;
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
     * The maximum wait time in ms that a thread may wait for a connection to become available.
     * Default is 120,000.
     */
    public int maxWaitTime;

    /**
     * The connection timeout in milliseconds.
     * It is used solely when establishing a new connection {@link java.net.Socket#connect(java.net.SocketAddress, int) }
     * Default is 0 and means no timeout.
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
     * @deprecated Replaced in MongoDB 2.0/Java Driver 2.7 with ReadPreference.SECONDARY
     * @see com.mongodb.ReadPreference.SECONDARY
     */
    @Deprecated
    public boolean slaveOk;

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
     * The "w" value of the global WriteConcern.
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
     * Default is false.
     */
    public boolean fsync;

    /**
     * The "j" value of the global WriteConcern.
     * Default is false.
     */
    public boolean j;

    /**
     * sets the socket factory for creating sockets to mongod
     * Default is SocketFactory.getDefault()
     */
    public SocketFactory socketFactory;

    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append( "description=" ).append( description ).append( ", " );
        buf.append( "connectionsPerHost=" ).append( connectionsPerHost ).append( ", " );
        buf.append( "threadsAllowedToBlockForConnectionMultiplier=" ).append( threadsAllowedToBlockForConnectionMultiplier ).append( ", " );
        buf.append( "maxWaitTime=" ).append( maxWaitTime ).append( ", " );
        buf.append( "connectTimeout=" ).append( connectTimeout ).append( ", " );
        buf.append( "socketTimeout=" ).append( socketTimeout ).append( ", " );
        buf.append( "socketKeepAlive=" ).append( socketKeepAlive ).append( ", " );
        buf.append( "autoConnectRetry=" ).append( autoConnectRetry ).append( ", " );
        buf.append( "maxAutoConnectRetryTime=" ).append( maxAutoConnectRetryTime ).append( ", " );
        buf.append( "slaveOk=" ).append( slaveOk ).append( ", " );
        buf.append( "safe=" ).append( safe ).append( ", " );
        buf.append( "w=" ).append( w ).append( ", " );
        buf.append( "wtimeout=" ).append( wtimeout ).append( ", " );
        buf.append( "fsync=" ).append( fsync ).append( ", " );
        buf.append( "j=" ).append( j );

        return buf.toString();
    }

}
