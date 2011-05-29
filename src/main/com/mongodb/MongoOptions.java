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
        slaveOk = false;
        safe = false;
        w = 0;
        wtimeout = 0;
        fsync = false;
        dbCallbackFactory = DefaultDBCallback.FACTORY;
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
     * <p>The number of connections allowed per host
     * (the pool size, per host)</p>
     * <p>Once the pool is exhausted, this will block.
     * @see {@linkplain MongoOptions#threadsAllowedToBlockForConnectionMultiplier}</p>
     */
    public int connectionsPerHost;

    /**
     *  multiplier for connectionsPerHost for # of threads that
     *  can block if connectionsPerHost is 10, and
     *  threadsAllowedToBlockForConnectionMultiplier is 5,
     *  then 50 threads can block
     *  more than that and an exception will be throw
     */
    public int threadsAllowedToBlockForConnectionMultiplier;

    /**
     * The max wait time for a blocking thread for a connection from the pool in ms.
     */
    public int maxWaitTime;

    /**
     *  The connection timeout in milliseconds; this is for
     *  establishing the socket connections (open).
     *  0 is default and infinite
     */
    public int connectTimeout;

    /**
     * The socket timeout; this value is passed to
     * {@link java.net.Socket#setSoTimeout(int)}.
     * 0 is default and infinite
     */
    public int socketTimeout;

    /**
     * This controls whether or not to have socket keep alive
     * turned on (SO_KEEPALIVE).
     *
     * defaults to false
     */
    public boolean socketKeepAlive;

    /**
     * This controls whether the system retries automatically
     * on connection errors.
     * defaults to false
     */
    public boolean autoConnectRetry;

    /**
     * Specifies if the driver is allowed to read from secondaries
     * or slaves.
     *
     * defaults to false
     */
    public boolean slaveOk;

    /**
     * Override the DBCallback factory. Default is for the standard Mongo Java
     * driver configuration.
     */
    public DBCallbackFactory dbCallbackFactory;

    /**
     * If <b>true</b> the driver sends a getLastError command after
     * every update to ensure it succeeded (see also w and wtimeout)
     * If <b>false</b>, the driver does not send a getlasterror command
     * after every update.
     *
     * defaults to false
     */
    public boolean safe;

    /**
     * If set, the w value of WriteConcern for the connection is set
     * to this.
     *
     * Defaults to 0; implies safe = true
     */
    public int w;

    /**
     * If set, the wtimeout value of WriteConcern for the
     * connection is set to this.
     *
     * Defaults to 0; implies safe = true
     */
    public int wtimeout;

    /**
     * Sets the fsync value of WriteConcern for the connection.
     *
     * Defaults to false; implies safe = true
     */
    public boolean fsync;

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
        buf.append( "slaveOk=" ).append( slaveOk ).append( ", " );
        buf.append( "safe=" ).append( safe ).append( ", " );
        buf.append( "w=" ).append( w ).append( ", " );
        buf.append( "wtimeout=" ).append( wtimeout ).append( ", " );
        buf.append( "fsync=" ).append( fsync );

        return buf.toString();
    }

}
