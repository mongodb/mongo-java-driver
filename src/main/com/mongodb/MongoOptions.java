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

import java.net.*;

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
        autoConnectRetry = false;
    }

    /**
       <p>The number of connections allowed per host (the pool size, per host)</p>
       <p>Once the pool is exhausted, this will block. See {@linkplain MongoOptions#threadsAllowedToBlockForConnectionMultiplier}</p>
     */
    public int connectionsPerHost;

    /**
       multiplier for connectionsPerHost for # of threads that can block
       if connectionsPerHost is 10, and threadsAllowedToBlockForConnectionMultiplier is 5, 
       then 50 threads can block
       more than that and an exception will be throw
     */
    public int threadsAllowedToBlockForConnectionMultiplier;
    
    /**
     * The max wait time for a blocking thread for a connection from the pool
     */
    public int maxWaitTime;

    /**
       The connection timeout in milliseconds; this is for establishing the socket connections (open). 0 is default and infinite
     */
    public int connectTimeout;

    /**
      The socket timeout; this value is passed to {@link java.net.Socket#setSoTimeout(int)}.  0 is default and infinite
     */
    public int socketTimeout;
    
    /**
       This controls whether the system retries automatically on connection errors.  defaults to false
    */
    public boolean autoConnectRetry;
    
    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append( "connectionsPerHost: " ).append( connectionsPerHost ).append( " " );
        buf.append( "threadsAllowedToBlockForConnectionMultiplier: " ).append( threadsAllowedToBlockForConnectionMultiplier ).append( " " );
        buf.append( "maxWaitTime: " ).append( maxWaitTime ).append( " " );
        buf.append( "connectTimeout: " ).append( connectTimeout ).append( " " );
        buf.append( "socketTimeout: " ).append( socketTimeout ).append( " " );
        buf.append( "autoConnectRetry: " ).append( autoConnectRetry ).append( " " );
        return buf.toString();
    }
    
}
