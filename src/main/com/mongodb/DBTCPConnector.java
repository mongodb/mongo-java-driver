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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBTCPConnector implements DBConnector {

    static Logger _logger = Logger.getLogger( Bytes.LOGGER.getName() + ".tcp" );
    static Logger _createLogger = Logger.getLogger( _logger.getName() + ".connect" );

    public DBTCPConnector( Mongo m , ServerAddress addr )
        throws MongoException {
        _mongo = m;
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( addr );

        _createLogger.info( addr.toString() );

        if ( addr.isPaired() ){
            _allHosts = new ArrayList<ServerAddress>( addr.explode() );
            _rsStatus = new ReplicaSetStatus( m, _allHosts );
            _createLogger.info( "switching to replica set mode : " + _allHosts + " -> " + getAddress()  );
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
        _rsStatus = new ReplicaSetStatus( m, _allHosts );

        _createLogger.info( all  + " -> " + getAddress() );
    }
    
    public void start() {
        if (_rsStatus != null)
            _rsStatus.start();
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
        throws MongoException, IOException {
        CommandResult e = null;
        e = port.runCommand( db , concern.getCommand() );

        if ( ! e.hasErr() )
            return new WriteResult( e , concern );
        
        e.throwOnError();
        return null;
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

        try {
            port.checkAuth( db );
            port.say( m );
            if ( concern.callGetLastError() ){
                return _checkWriteError( db , mp , port , concern );
            }
            else {
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
            mp.done( port );
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
                
        Response res = null;
        boolean retry = false;
        try {
            port.checkAuth( db );
            res = port.call( m , coll );
            if ( res._responseTo != m.getId() )
                throw new MongoException( "ids don't match" );
        }
        catch ( IOException ioe ){
            mp.error( port , ioe );
            retry = retries > 0 && !coll._name.equals( "$cmd" )
                    && !(ioe instanceof SocketTimeoutException) && _error( ioe, slaveOk );
            if ( !retry ){
                throw new MongoException.Network( "can't call something" , ioe );
            }
        }
        catch ( RuntimeException re ){
            mp.error( port , re );
            throw re;
        } finally {
            mp.done( port );
        }

        if (retry)
            return call( db , coll , m , hostNeeded , retries - 1 );

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
        DBPortPool pool = _masterPortPool;
        return pool != null ? pool.getServerAddress() : null;
    }

    /**
     * Gets the list of seed server addresses
     * @return
     */
    public List<ServerAddress> getAllAddress() {
        return _allHosts;
    }

    /**
     * Gets the list of server addresses currently seen by the connector.
     * This includes addresses auto-discovered from a replica set.
     * @return
     */
    public List<ServerAddress> getServerAddressList() {
        if (_rsStatus != null) {
            return _rsStatus.getServerAddressList();
        }
        
        ServerAddress master = getAddress();
        if (master != null) {
            // single server
            List<ServerAddress> list = new ArrayList<ServerAddress>();
            list.add(master);
            return list;
        }
        return null;
    }

    public ReplicaSetStatus getReplicaSetStatus() {
        return _rsStatus;
    }

    public String getConnectPoint(){
        ServerAddress master = getAddress();
        return master != null ? master.toString() : null;
    }

    boolean _error( Throwable t, boolean slaveOk )
        throws MongoException {
        if ( _rsStatus.hasServerUp() ){
            // the replset has at least 1 server up, try to see if should switch master
            checkMaster( true , !slaveOk );
        }
        return _rsStatus.hasServerUp();
    }

    class MyPort {

        DBPort get( boolean keep , boolean slaveOk , ServerAddress hostNeeded ){
            
            if ( hostNeeded != null ){
                // asked for a specific host
                return _portHolder.get( hostNeeded ).get();
            }

            if ( _requestPort != null ){
                // we are within a request, and have a port, should stick to it
                if ( _requestPort.getPool() == _masterPortPool || !keep ) {
                    // if keep is false, it's a read, so we use port even if master changed
                    return _requestPort;
                }

                // it's write and master has changed
                // we fall back on new master and try to go on with request
                // this may not be best behavior if spec of request is to stick with same server
                _requestPort.getPool().done(_requestPort);
                _requestPort = null;
            }
            
            if ( slaveOk && _rsStatus != null ){
                // if slaveOk, try to use a secondary
                ServerAddress slave = _rsStatus.getASecondary();
                if ( slave != null ){
                    return _portHolder.get( slave ).get();
                }
            }

            if (_masterPortPool == null) {
                // this should only happen in rare case that no master was ever found
                // may get here at startup if it's a read, slaveOk=true, and ALL servers are down
                throw new MongoException("Rare case where master=null, probably all servers are down");
            }

            // use master
            DBPort p = _masterPortPool.get();
            if ( keep && _inRequest ) {
                // if within request, remember port to stick to same server
                _requestPort = p;
            }

            return p;
        }
        
        void done( DBPort p ){
            // keep request port
            if ( p != _requestPort ){
                p.getPool().done(p);
            }
        }

        /**
         * call this method when there is an IOException or other low level error on port.
         * @param p
         * @param e
         */
        void error( DBPort p , Exception e ){
            p.close();
            _requestPort = null;
//            _logger.log( Level.SEVERE , "MyPort.error called" , e );

            // depending on type of error, may need to close other connections in pool
            p.getPool().gotError(e);
        }
        
        void requestEnsureConnection(){
            if ( ! _inRequest )
                return;

            if ( _requestPort != null )
                return;
            
            _requestPort = _masterPortPool.get();
        }

        void requestStart(){
            _inRequest = true;
        }

        void requestDone(){
            if ( _requestPort != null )
                _requestPort.getPool().done( _requestPort );
            _requestPort = null;
            _inRequest = false;
        }

        DBPort _requestPort;
//        DBPortPool _requestPool;
        boolean _inRequest;
    }
    
    void checkMaster( boolean force , boolean failIfNoMaster )
        throws MongoException {
        
        if ( _rsStatus != null ){
            if ( _masterPortPool == null || force ){
                ReplicaSetStatus.Node n = _rsStatus.ensureMaster();
                if ( n == null ){
                    if ( failIfNoMaster )
                        throw new MongoException( "can't find a master" );
                }
                else {
                    _set( n._addr );
                    maxBsonObjectSize = _rsStatus.getMaxBsonObjectSize();
                }
            }
        } else {
            // single server, may have to obtain max bson size
            if (maxBsonObjectSize == 0)
                    maxBsonObjectSize = fetchMaxBsonObjectSize();
        }
    }

    /**
     * Fetches the maximum size for a BSON object from the current master server
     * @return the size, or 0 if it could not be obtained
     */
    int fetchMaxBsonObjectSize() {
        if (_masterPortPool == null)
            return 0;
        DBPort port = _masterPortPool.get();
        try {
            CommandResult res = port.runCommand(_mongo.getDB("admin"), new BasicDBObject("isMaster", 1));
            // max size was added in 1.8
            if (res.containsField("maxBsonObjectSize")) {
                maxBsonObjectSize = ((Integer) res.get("maxBsonObjectSize")).intValue();
            } else {
                maxBsonObjectSize = Bytes.MAX_OBJECT_SIZE;
            }
        } catch (Exception e) {
            _logger.log(Level.WARNING, "Exception determining maxBSON size using"+maxBsonObjectSize, e);
        } finally {
            port.getPool().done(port);
        }
        return maxBsonObjectSize;
    }


    void testMaster()
        throws MongoException {
        
        DBPort p = null;
        try {
            p = _masterPortPool.get();
            p.runCommand( _mongo.getDB("admin") , new BasicDBObject( "nonce" , 1 ) );
        } catch ( IOException ioe ){
            throw new MongoException.Network( ioe.getMessage() , ioe );
        } finally {
            _masterPortPool.done( p );
        }
    }

    private boolean _set( ServerAddress addr ){
        DBPortPool newPool = _portHolder.get( addr );
        if (newPool == _masterPortPool)
            return false;

        if ( _logger.isLoggable( Level.WARNING ) && _masterPortPool != null )
            _logger.log(Level.WARNING, "Master switching from " + _masterPortPool.getServerAddress() + " to " + addr);
        _masterPortPool = newPool;
        return true;
    }

    public String debugString(){
        StringBuilder buf = new StringBuilder( "DBTCPConnector: " );
        if ( _rsStatus != null ) {
            buf.append( "replica set : " ).append( _allHosts );
        } else {
            ServerAddress master = getAddress();
            buf.append( master ).append( " " ).append( master != null ? master._addr : null );
        }

        return buf.toString();
    }
    
    public void close(){
        _closed = true;
        if ( _portHolder != null ) {
            _portHolder.close();
            _portHolder = null;
        }
        if ( _rsStatus != null ) {
            _rsStatus.close();
            _rsStatus = null;
        }

        // below this will remove the myport for this thread only
        // client using thread pool in web framework may need to call close() from all threads
        _myPort.remove();
    }

    /**
     * Assigns a new DBPortPool for a given ServerAddress.
     * This is used to obtain a new pool when the resolved IP of a host changes, for example.
     * User application should not have to call this method directly.
     * @param addr
     */
    public void updatePortPool(ServerAddress addr) {
        // just remove from map, a new pool will be created lazily
        _portHolder._pools.remove(addr);
    }

    /**
     * Gets the DBPortPool associated with a ServerAddress.
     * @param addr
     * @return
     */
    public DBPortPool getDBPortPool(ServerAddress addr) {
        return _portHolder.get(addr);
    }

    public boolean isOpen(){
        return ! _closed;
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    private Mongo _mongo;
//    private ServerAddress _curMaster;
    private DBPortPool _masterPortPool;
    private DBPortPool.Holder _portHolder;
    private final List<ServerAddress> _allHosts;
    private ReplicaSetStatus _rsStatus;
    private boolean _closed = false;
    private int maxBsonObjectSize = 0;

    private ThreadLocal<MyPort> _myPort = new ThreadLocal<MyPort>(){
        protected MyPort initialValue(){
            return new MyPort();
        }
    };

}
