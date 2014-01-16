/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

// DBPortPool.java

package com.mongodb;

import com.mongodb.util.SimplePool;

import java.util.concurrent.Semaphore;

/**
 * This class is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.
 *
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class DBPortPool extends SimplePool<DBPort> {

    public String getHost() {
        return _addr.getHost();
    }

    public int getPort() {
        return _addr.getPort();
    }

    // ----

    /**
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoClientException} instead.
     */
    @Deprecated
    public static class NoMoreConnection extends MongoClientException {

        private static final long serialVersionUID = -4415279469780082174L;

        /**
         * Constructs a new instance with the given message.
         *
         * @param msg the message
         */
        public NoMoreConnection(String msg) {
            super(msg);
        }
    }

    /**
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoWaitQueueFullException} instead.
     */
    @Deprecated
    public static class SemaphoresOut extends MongoWaitQueueFullException {

        private static final long serialVersionUID = -4415279469780082174L;

        private static final String message = "Concurrent requests for database connection have exceeded limit";
        SemaphoresOut(){
            super( message );
        }

        SemaphoresOut(int numPermits){
            super( message + " of " + numPermits);
        }
    }

    /**
     * @deprecated This class will be dropped in 3.x versions.
     *             Please catch {@link MongoTimeoutException} instead.
     */
    @Deprecated
    public static class ConnectionWaitTimeOut extends MongoTimeoutException {

        private static final long serialVersionUID = -4415279469780082174L;

        ConnectionWaitTimeOut(int timeout) {
            super("Connection wait timeout after " + timeout + " ms");
        }
    }

    // ----

    DBPortPool( ServerAddress addr , MongoOptions options ){
        super( "DBPortPool-" + addr.toString() + ", options = " +  options.toString() , options.connectionsPerHost );
        _options = options;
        _addr = addr;
        _waitingSem = new Semaphore( _options.connectionsPerHost * _options.threadsAllowedToBlockForConnectionMultiplier );
    }

    /**
     * @return
     * @throws MongoException
     */
    @Override
    public DBPort get() {
        DBPort port = null;
        if ( ! _waitingSem.tryAcquire() )
            throw new SemaphoresOut(_options.connectionsPerHost * _options.threadsAllowedToBlockForConnectionMultiplier);

        try {
            port = get( _options.maxWaitTime );
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(e);
        } finally {
            _waitingSem.release();
        }

        if ( port == null )
            throw new ConnectionWaitTimeOut( _options.maxWaitTime );

        return port;
    }

    @Override
    public void cleanup( DBPort p ){
        p.close();
    }

    @Override
    protected DBPort createNew(){
        return new DBPort( _addr );
    }

    public ServerAddress getServerAddress() {
        return _addr;
    }

    final MongoOptions _options;
    final private Semaphore _waitingSem;
    final ServerAddress _addr;
}
