// DBTCPConnector.java

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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import org.bson.*;

class DBTCPConnector implements DBConnector {

    static Logger _logger = Logger.getLogger( Bytes.LOGGER.getName() + ".tcp" );
    static Logger _createLogger = Logger.getLogger( _logger.getName() + ".connect" );

    public DBTCPConnector( Mongo m , ServerAddress addr )
        throws MongoException {
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( addr );

        _createLogger.info( addr.toString() );

        if ( addr.isPaired() ){
            _allHosts = new ArrayList<ServerAddress>( addr.explode() );
            _rsStatus = new ReplicaSetStatus( _allHosts );
            _createLogger.info( "switching to replica set mode : " + _allHosts + " -> " + _curMaster  );
        }
        else {
            _set( addr );
            _allHosts = null;
            _rsStatus = null;
        }

    }

    public DBTCPConnector( Mongo m , ServerAddress ... all )
        throws MongoException {
        this( m , Arrays.asList( all ) );
    }

    public DBTCPConnector( Mongo m , List<ServerAddress> all )
        throws MongoException {
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( all );

        _allHosts = new ArrayList<ServerAddress>( all ); // make a copy so it can't be modified
        _rsStatus = new ReplicaSetStatus( _allHosts );

        _createLogger.info( all  + " -> " + _curMaster );
    }

    private static ServerAddress _checkAddress( ServerAddress addr ){
        if ( addr == null )
            throw new NullPointerException( "address can't be null" );
        return addr;
    }

    private static ServerAddress _checkAddress( List<ServerAddress> addrs ){
        if ( addrs == null )
            throw new NullPointerException( "addresses can't be null" );
        if ( addrs.size() == 0 )
            throw new IllegalArgumentException( "need to specify at least 1 address" );
        return addrs.get(0);
    }

    /**
     * Start a "request".
     *
     * A "request" is a group of operations in which order matters. Examples
     * include inserting a document and then performing a query which expects
     * that document to have been inserted, or performing an operation and
     * then using com.mongodb.Mongo.getLastError to perform error-checking
     * on that operation. When a thread performs operations in a "request", all
     * operations will be performed on the same socket, so they will be
     * correctly ordered.
     */
    public void requestStart(){
        _myPort.get().requestStart();
    }

    /**
     * End the current "request", if this thread is in one.
     *
     * By ending a request when it is safe to do so the built-in connection-
     * pool is allowed to reassign requests to different sockets in order to
     * more effectively balance load. See requestStart for more information.
     */
    public void requestDone(){
        _myPort.get().requestDone();
    }

    public void requestEnsureConnection(){
        _myPort.get().requestEnsureConnection();
    }

    void _checkClosed(){
        if ( _closed )
            throw new IllegalStateException( "this Mongo has been closed" );
    }

    WriteResult _checkWriteError( DB db , MyPort mp , DBPort port , WriteConcern concern )
        throws MongoException {

        CommandResult e = port.runCommand( db , concern.getCommand() );
        mp.done( port );
        
        Object foo = e.get( "err" );
        if ( foo == null )
            return new WriteResult( e , concern );
        
        int code = -1;
        if ( e.get( "code" ) instanceof Number )
            code = ((Number)e.get("code")).intValue();
        String s = foo.toString();
        if ( code == 11000 || code == 11001 ||
             s.startsWith( "E11000" ) ||
             s.startsWith( "E11001" ) )
            throw new MongoException.DuplicateKey( code , s );
        throw new MongoException( code , s );
    }

    public WriteResult say( DB db , OutMessage m , WriteConcern concern )
        throws MongoException {
        return say( db , m , concern , null );
    }
    
    public WriteResult say( DB db , OutMessage m , WriteConcern concern , ServerAddress hostNeeded )
        throws MongoException {

        _checkClosed();
        checkMaster( false , true );

        MyPort mp = _myPort.get();
        DBPort port = mp.get( true , false , hostNeeded );
        port.checkAuth( db );

        try {
            port.say( m );
            if ( concern.callGetLastError() ){
                return _checkWriteError( db , mp , port , concern );
            }
            else {
                mp.done( port );
                return new WriteResult( db , port , concern );
            }
        }
        catch ( IOException ioe ){
            mp.error( port , ioe );
            _error( ioe, false );

            if ( concern.raiseNetworkErrors() )
                throw new MongoException.Network( "can't say something" , ioe );
            
            CommandResult res = new CommandResult();
            res.put( "ok" , false );
            res.put( "$err" , "NETWORK ERROR" );
            return new WriteResult( res , concern );
        }
        catch ( MongoException me ){
            throw me;
        }
        catch ( RuntimeException re ){
            mp.error( port , re );
            throw re;
        }
        finally {
            m.doneWithMessage();
        }
    }
    
    public Response call( DB db , DBCollection coll , OutMessage m )
        throws MongoException {
        return call( db , coll , m , null , 2 );
    }

    public Response call( DB db , DBCollection coll , OutMessage m , ServerAddress hostNeeded ) 
        throws MongoException {
        return call( db , coll , m , hostNeeded , 2 );
    }

    public Response call( DB db , DBCollection coll , OutMessage m , ServerAddress hostNeeded , int retries )
        throws MongoException {
        boolean slaveOk = m.hasOption( Bytes.QUERYOPTION_SLAVEOK );
        _checkClosed();
        checkMaster( false , !slaveOk );
        
        final MyPort mp = _myPort.get();
        final DBPort port = mp.get( false , slaveOk, hostNeeded );
        
        port.checkAuth( db );
        
        Response res = null;
        try {
            res = port.call( m , coll );
            mp.done( port );
            if ( res._responseTo != m.getId() )
                throw new MongoException( "ids don't match" );
        }
        catch ( IOException ioe ){
            boolean shouldRetry = _error( ioe, slaveOk ) && ! coll._name.equals( "$cmd" ) && retries > 0;
            mp.error( port , ioe );
            if ( shouldRetry ){
                return call( db , coll , m , hostNeeded , retries - 1 );
            }
            throw new MongoException.Network( "can't call something" , ioe );
        }
        catch ( RuntimeException re ){
            mp.error( port , re );
            throw re;
        }
        
        ServerError err = res.getError();
        
        if ( err != null && err.isNotMasterError() ){
            checkMaster( true , true );
            if ( retries <= 0 ){
                throw new MongoException( "not talking to master and retries used up" );
            }
            return call( db , coll , m , hostNeeded , retries -1 );
        }
        
        m.doneWithMessage();
        return res;
    }

    public ServerAddress getAddress(){
        return _curMaster;
    }

    public List<ServerAddress> getAllAddress() {
        return _allHosts;
    }

    public String getConnectPoint(){
        return _curMaster.toString();
    }

    boolean _error( Throwable t, boolean slaveOk )
        throws MongoException {
        if ( _allHosts != null ){
            _logger.log( Level.WARNING , "replica set mode, switching master" , t );
            checkMaster( true , !slaveOk );
        }
        return true;
    }

    class MyPort {

        DBPort get( boolean keep , boolean slaveOk , ServerAddress hostNeeded ){
            
            if ( hostNeeded != null ){
                _pool = _portHolder.get( hostNeeded );
                return _pool.get();
            }

            if ( _port != null ){
                if ( _pool == _curPortPool )
                    return _port;
                _pool.done( _port );
                _port = null;
                _pool = null;
            }
            
            if ( slaveOk && _rsStatus != null ){
                ServerAddress slave = _rsStatus.getASecondary();
                if ( slave != null ){
                    _pool = _portHolder.get( slave );
                    return _pool.get();
                }
            }

            
            _pool = _curPortPool;
            DBPort p = _pool.get();
            if ( keep && _inRequest )
                _port = p;

            return p;
        }
        
        void done( DBPort p ){
            if ( p != _port ){
                _pool.done( p );
                _pool = null;
                _slave = null;
            }

        }

        void error( DBPort p , Exception e ){
            _pool.done( p );
            p.close();

            _port = null;
            _pool = null;

            _logger.log( Level.SEVERE , "MyPort.error called" , e );            
        }
        
        void requestEnsureConnection(){
            if ( ! _inRequest )
                return;

            if ( _port != null )
                return;
            
            if ( _pool == null )
                _pool = _curPortPool;

            _port = _pool.get();
        }

        void requestStart(){
            _inRequest = true;
        }

        void requestDone(){
            if ( _port != null )
                _pool.done( _port );
            _port = null;
            _pool = null;
            _inRequest = false;
        }

        DBPort _port;
        DBPortPool _pool;
        boolean _inRequest;
        ServerAddress _slave; // slave used for last read if any
    }
    
    void checkMaster( boolean force , boolean failIfNoMaster )
        throws MongoException {
        
        if ( _rsStatus != null ){
            if ( _curPortPool == null || force ){
                ReplicaSetStatus.Node n = _rsStatus.ensureMaster();
                if ( n == null ){
                    if ( failIfNoMaster )
                        throw new MongoException( "can't find a master" );
                }
                else {
                    _set( n._addr );
                }
            }
        }
    }

    void testMaster()
        throws MongoException {
        
        DBPort p = null;
        try {
            p = _curPortPool.get();
            p.runCommand( "admin" , new BasicDBObject( "nonce" , 1 ) );
        }
        finally {
            _curPortPool.done( p );
        }
    }

    private boolean _set( ServerAddress addr ){
        if ( _curMaster == addr )
            return false;
        _curMaster = addr;
        _curPortPool = _portHolder.get( addr );
        return true;
    }

    public String debugString(){
        StringBuilder buf = new StringBuilder( "DBTCPConnector: " );
        if ( _allHosts != null )
            buf.append( "replica set : " ).append( _allHosts );
        else
            buf.append( _curMaster ).append( " " ).append( _curMaster._addr );

        return buf.toString();
    }
    
    public void close(){
        _closed = true;
        if ( _portHolder != null )
            _portHolder.close();
        if ( _rsStatus != null )
            _rsStatus.close();
        _myPort = null;
    }

    public boolean isOpen(){
        return ! _closed;
    }

    private ServerAddress _curMaster;
    private DBPortPool _curPortPool;
    private DBPortPool.Holder _portHolder;
    private final List<ServerAddress> _allHosts;
    private final ReplicaSetStatus _rsStatus;
    private boolean _closed = false;

    private ThreadLocal<MyPort> _myPort = new ThreadLocal<MyPort>(){
        protected MyPort initialValue(){
            return new MyPort();
        }
    };

}
