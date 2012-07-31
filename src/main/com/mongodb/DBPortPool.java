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

import com.mongodb.util.SimplePool;
import com.mongodb.util.management.JMException;
import com.mongodb.util.management.MBeanServerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class DBPortPool extends SimplePool<DBPort> implements MongoConnectionPoolMXBean {

    public MongoConnection[] getInUseConnections() {
        List<MongoConnection> connectionList = new ArrayList<MongoConnection>();
        synchronized (_avail) {
            for (DBPort port : _all) {
                if (!_avail.contains(port)) {
                    OutMessage curOutMessage = port.getOutMessageBeingProcessed();
                    if (curOutMessage != null) {
                        connectionList.add(new MongoConnection(curOutMessage.getNamespace(),
                                curOutMessage.getOpCode(),
                                curOutMessage.getQuery() != null ? curOutMessage.getQuery().toString() : null));
                    }
                }
            }
        }
        return connectionList.toArray(new MongoConnection[connectionList.size()]);
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

                p = new DBPortPool( addr , _options );
                _pools.put( addr , p);

                try {
                    String on = createObjectName(addr);
                    if (MBeanServerFactory.getMBeanServer().isRegistered(on)) {
                        MBeanServerFactory.getMBeanServer().unregisterMBean(on);
                        Bytes.LOGGER.log(Level.INFO, "multiple Mongo instances for same host, jmx numbers might be off");
                    }
                    MBeanServerFactory.getMBeanServer().registerMBean(p, on);
                } catch (JMException e) {
                    Bytes.LOGGER.log(Level.WARNING, "jmx registration error: " + e + " continuing...");
                } catch (java.security.AccessControlException e) {
                    Bytes.LOGGER.log(Level.WARNING, "jmx registration error: " + e + " continuing...");
                }
            }

            return p;
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

        final MongoOptions _options;
        final Map<ServerAddress,DBPortPool> _pools = Collections.synchronizedMap( new HashMap<ServerAddress,DBPortPool>() );
        final int _serial = nextSerial.incrementAndGet();

        // we use this to give each Holder a different mbean name
        static AtomicInteger nextSerial = new AtomicInteger(0);
    }

    // ----

    public static class NoMoreConnection extends MongoInternalException {
        private static final long serialVersionUID = -4415279469780082174L;

        NoMoreConnection( String msg ){
	        super( msg );
	    }
    }

    public static class SemaphoresOut extends NoMoreConnection {
        private static final long serialVersionUID = -4415279469780082174L;
        SemaphoresOut(){
            super( "Out of semaphores to get db connection" );
        }
    }

    public static class ConnectionWaitTimeOut extends NoMoreConnection {
        private static final long serialVersionUID = -4415279469780082174L;
        ConnectionWaitTimeOut(int timeout) {
            super("Connection wait timeout after " + timeout + " ms");
        }
    }

    // ----

    DBPortPool( ServerAddress addr , MongoOptions options ){
        super( "DBPortPool-" + addr.toString() + ", options = " +  options.toString() , options.connectionsPerHost , options.connectionsPerHost );
        _options = options;
        _addr = addr;
        _waitingSem = new Semaphore( _options.connectionsPerHost * _options.threadsAllowedToBlockForConnectionMultiplier );
    }

    protected long memSize( DBPort p ){
        return 0;
    }

    protected int pick( int iThink , boolean couldCreate ){
        final int id = System.identityHashCode(Thread.currentThread());
        final int s = _availSafe.size();
        for ( int i=0; i<s; i++ ){
            DBPort p = _availSafe.get(i);
            if ( p._lastThread == id )
                return i;
        }

        if ( couldCreate )
            return -1;
        return iThink;
    }

    /**
     * @return
     * @throws MongoException
     */
    public DBPort get() {
        DBPort port = null;
        if ( ! _waitingSem.tryAcquire() )
            throw new SemaphoresOut();

        try {
            port = get( _options.maxWaitTime );
        }
        finally {
            _waitingSem.release();
        }

        if ( port == null )
            throw new ConnectionWaitTimeOut( _options.maxWaitTime );

            port._lastThread = System.identityHashCode(Thread.currentThread());
        return port;
    }

    // return true if the exception is recoverable
    boolean gotError( Exception e ){
        if ( e instanceof java.nio.channels.ClosedByInterruptException ||
             e instanceof InterruptedException ){
            // this is probably a request that is taking too long
            // so usually doesn't mean there is a real db problem
            return true;
        }

        if ( e instanceof java.net.SocketTimeoutException ){
            // we don't want to clear the port pool for a connection timing out
            return true;
        }
        Bytes.LOGGER.log( Level.WARNING , "emptying DBPortPool to " + getServerAddress() + " b/c of error" , e );

        // force close all sockets 

        List<DBPort> all = new ArrayList<DBPort>();
        while ( true ){
            DBPort temp = get(0);
            if ( temp == null )
                break;
            all.add( temp );
        }

        for ( DBPort p : all ){
            p.close();
            done(p);
        }

        return false;
    }

    void close(){
        clear();
    }

    public void cleanup( DBPort p ){
        p.close();
    }

    public boolean ok( DBPort t ){
        return _addr.getSocketAddress().equals( t._addr );
    }

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
