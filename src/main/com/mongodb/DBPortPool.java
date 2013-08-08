// DBPortPool.java

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

import com.mongodb.util.ConnectionPoolStatisticsBean;
import com.mongodb.util.SimplePool;
import com.mongodb.util.management.JMException;
import com.mongodb.util.management.MBeanServerFactory;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

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

    public synchronized ConnectionPoolStatisticsBean getStatistics() {
        return new ConnectionPoolStatisticsBean(getTotal(), getInUse(), getInUseConnections());
    }

    private InUseConnectionBean[] getInUseConnections() {
        List<InUseConnectionBean> inUseConnectionInfoList = new ArrayList<InUseConnectionBean>();
        long currentNanoTime = System.nanoTime();
        for (DBPort port : _out) {
            inUseConnectionInfoList.add(new InUseConnectionBean(port, currentNanoTime));
        }
        return inUseConnectionInfoList.toArray(new InUseConnectionBean[inUseConnectionInfoList.size()]);
    }


    static class Holder {

        Holder( MongoOptions options ){
            _options = options;
        }

        DBPortPool get( ServerAddress addr ){

            DBPortPool p = _pools.get( addr );

            if (p != null)
                return p;

            synchronized (_pools) {
                p = _pools.get( addr );
                if (p != null) {
                    return p;
                }

                p = createPool(addr);
                _pools.put( addr , p);

                try {
                    String on = createObjectName(addr);
                    if (MBeanServerFactory.getMBeanServer().isRegistered(on)) {
                        MBeanServerFactory.getMBeanServer().unregisterMBean(on);
                        Bytes.LOGGER.log(Level.INFO, "multiple Mongo instances for same host, jmx numbers might be off");
                    }
                    MBeanServerFactory.getMBeanServer().registerMBean(p, on);
                } catch (JMException e) {
                    Bytes.LOGGER.log(Level.WARNING, "JMX registration error: " + e +
                            "\nConsider setting com.mongodb.MongoOptions.alwaysUseMBeans property to true." +
                            "\nContinuing...");
                } catch (java.security.AccessControlException e) {
                    Bytes.LOGGER.log(Level.WARNING, "JMX registration error: " + e +
                            "\nContinuing...");
                }
            }

            return p;
        }

        private DBPortPool createPool(final ServerAddress addr) {
            if (isJava5 || _options.isAlwaysUseMBeans()) {
                return new Java5MongoConnectionPool(addr, _options);
            } else {
                return new MongoConnectionPool(addr, _options);
            }
        }

        void close(){
            synchronized ( _pools ){
                for ( DBPortPool p : _pools.values() ){
                    p.close();

                    try {
                        String on = createObjectName( p._addr );
                        if ( MBeanServerFactory.getMBeanServer().isRegistered(on) ){
                            MBeanServerFactory.getMBeanServer().unregisterMBean(on);
                        }
                    } catch ( JMException e ){
                        Bytes.LOGGER.log( Level.WARNING , "jmx de-registration error, continuing" , e );
                    }
                }
            }
        }

        private String createObjectName( ServerAddress addr ) {
            String name =  "com.mongodb:type=ConnectionPool,host=" + addr.toString().replace( ":" , ",port=" ) + ",instance=" + _serial;
            if ( _options.description != null )
                name += ",description=" + _options.description;
            return name;
        }

        static {
            isJava5 = System.getProperty("java.version").startsWith("1.5");
        }

        final MongoOptions _options;
        final Map<ServerAddress,DBPortPool> _pools = Collections.synchronizedMap( new HashMap<ServerAddress,DBPortPool>() );
        final int _serial = nextSerial.incrementAndGet();

        // we use this to give each Holder a different mbean name
        static AtomicInteger nextSerial = new AtomicInteger(0);
        static final boolean isJava5;
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

    protected long memSize( DBPort p ){
        return 0;
    }

    @Override
    protected int pick( int recommended, boolean couldCreate ){
        int id = System.identityHashCode(Thread.currentThread());
        for (int i = _avail.size() - 1; i >= 0; i--){
            if ( _avail.get(i)._lastThread == id )
                return i;
        }

        return couldCreate ? -1 : recommended;
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

        port._lastThread = System.identityHashCode(Thread.currentThread());
        return port;
    }

    // return true if the exception is recoverable
    boolean gotError( Exception e ){
        if (e instanceof java.nio.channels.ClosedByInterruptException){
            // this is probably a request that is taking too long
            // so usually doesn't mean there is a real db problem
            return true;
        }

        if ( e instanceof InterruptedIOException){
            // we don't want to clear the port pool for a connection timing out or interrupted
            return true;
        }
        Bytes.LOGGER.log( Level.WARNING , "emptying DBPortPool to " + getServerAddress() + " b/c of error" , e );

        // force close all sockets 

        List<DBPort> all = new ArrayList<DBPort>();
        while ( true ){
            try {
                DBPort temp = get(0);
                if ( temp == null )
                    break;
                all.add( temp );
            } catch (InterruptedException interruptedException) {
                throw new MongoInterruptedException(interruptedException);
            }
        }

        for ( DBPort p : all ){
            p.close();
            done(p);
        }

        return false;
    }

    @Override
    public void cleanup( DBPort p ){
        p.close();
    }

    @Override
    protected DBPort createNew(){
        return new DBPort( _addr , this , _options );
    }

    public ServerAddress getServerAddress() {
        return _addr;
    }

    final MongoOptions _options;
    final private Semaphore _waitingSem;
    final ServerAddress _addr;
    boolean _everWorked = false;
}
