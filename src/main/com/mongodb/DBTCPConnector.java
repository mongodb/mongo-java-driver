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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBTCPConnector implements DBConnector {

    static Logger _logger = Logger.getLogger( Bytes.LOGGER.getName() + ".tcp" );
    static Logger _createLogger = Logger.getLogger( _logger.getName() + ".connect" );

    /**
     * @param m
     * @param addr
     * @throws MongoException
     */
    public DBTCPConnector( Mongo m , ServerAddress addr ){
        _mongo = m;
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( addr );

        _createLogger.info( addr.toString() );

        setMasterAddress(addr);
        _allHosts = null;
    }

    /**
     * @param m
     * @param all
     * @throws MongoException
     */
    public DBTCPConnector( Mongo m , ServerAddress ... all ){
        this(m, Arrays.asList(all));
    }

    /**
     * @param m
     * @param all
     * @throws MongoException
     */
    public DBTCPConnector( Mongo m , List<ServerAddress> all ) {
        _mongo = m;
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( all );

        _allHosts = new ArrayList<ServerAddress>( all ); // make a copy so it can't be modified
        _createLogger.info( all  + " -> " + getAddress() );

        _connectionStatus = new DynamicConnectionStatus(m, _allHosts);
    }

    public void start() {
        if (_connectionStatus != null) {
            _connectionStatus.start();
        }
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
    @Override
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
    @Override
    public void requestDone(){
        _myPort.get().requestDone();
    }

    /**
     * @throws MongoException
     */
    @Override
    public void requestEnsureConnection(){
        checkMaster( false , true );
        _myPort.get().requestEnsureConnection();
    }

    void _checkClosed(){
        if ( _closed.get() )
            throw new IllegalStateException( "this Mongo has been closed" );
    }

    WriteResult _checkWriteError( DB db, DBPort port , WriteConcern concern )
        throws IOException{
        CommandResult e = port.runCommand( db , concern.getCommand() );

        e.throwOnError();
        return new WriteResult( e , concern );
    }

    /**
     * @param db
     * @param m
     * @param concern
     * @return
     * @throws MongoException
     */
    @Override
    public WriteResult say( DB db , OutMessage m , WriteConcern concern ){
        return say( db , m , concern , null );
    }

    /**
     * @param db
     * @param m
     * @param concern
     * @param hostNeeded
     * @return
     * @throws MongoException
     */
    @Override
    public WriteResult say( DB db , OutMessage m , WriteConcern concern , ServerAddress hostNeeded ){

        if (concern == null) {
            throw new IllegalArgumentException("Write concern is null");
        }

        _checkClosed();
        checkMaster( false , true );

        MyPort mp = _myPort.get();
        DBPort port = mp.get( true , ReadPreference.primary(), hostNeeded );

        try {
            port.checkAuth( db );
            port.say( m );
            if ( concern.callGetLastError() ){
                return _checkWriteError( db , port , concern );
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

            CommandResult res = new CommandResult(port.serverAddress());
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

    /**
     * @param db
     * @param coll
     * @param m
     * @param hostNeeded
     * @param decoder
     * @return
     * @throws MongoException
     */
    @Override
    public Response call( DB db , DBCollection coll , OutMessage m, ServerAddress hostNeeded, DBDecoder decoder ){
        return call( db , coll , m , hostNeeded , 2, null, decoder );
    }

    /**
     * @param db
     * @param coll
     * @param m
     * @param hostNeeded
     * @param retries
     * @return
     * @throws MongoException
     */
    @Override
    public Response call( DB db , DBCollection coll , OutMessage m , ServerAddress hostNeeded , int retries ){
        return call( db, coll, m, hostNeeded, retries, null, null);
    }


    /**
     * @param db
     * @param coll
     * @param m
     * @param hostNeeded
     * @param readPref
     * @param decoder
     * @return
     * @throws MongoException
     */
    @Override
    public Response call( DB db, DBCollection coll, OutMessage m, ServerAddress hostNeeded, int retries,
                          ReadPreference readPref, DBDecoder decoder ){
        try {
            return innerCall(db, coll, m, hostNeeded, retries, readPref, decoder);
        } finally {
            m.doneWithMessage();
        }
    }

    // This method is recursive.  It calls itself to implement query retry logic.
    private Response innerCall(final DB db, final DBCollection coll, final OutMessage m, final ServerAddress hostNeeded,
                               final int retries, ReadPreference readPref, final DBDecoder decoder) {
        if (readPref == null)
            readPref = ReadPreference.primary();

        if (readPref == ReadPreference.primary() && m.hasOption( Bytes.QUERYOPTION_SLAVEOK ))
           readPref = ReadPreference.secondaryPreferred();

        boolean secondaryOk = !(readPref == ReadPreference.primary());

        _checkClosed();
        // Don't check master on secondary reads unless connected to a replica set
        if (!secondaryOk || getReplicaSetStatus() == null)
            checkMaster( false, !secondaryOk );

        final MyPort mp = _myPort.get();
        final DBPort port = mp.get( false , readPref, hostNeeded );

        Response res = null;
        boolean retry = false;
        try {
            port.checkAuth( db );
            res = port.call( m , coll, decoder );
            if ( res._responseTo != m.getId() )
                throw new MongoException( "ids don't match" );
        }
        catch ( IOException ioe ){
            mp.error( port , ioe );
            retry = retries > 0 && !coll._name.equals( "$cmd" )
                    && !(ioe instanceof SocketTimeoutException) && _error( ioe, secondaryOk );
            if ( !retry ){
                throw new MongoException.Network( "can't call something : " + port.host() + "/" + db,
                                                  ioe );
            }
        }
        catch ( RuntimeException re ){
            mp.error( port , re );
            throw re;
        } finally {
            mp.done( port );
        }

        if (retry)
            return innerCall( db , coll , m , hostNeeded , retries - 1 , readPref, decoder );

        ServerError err = res.getError();

        if ( err != null && err.isNotMasterError() ){
            checkMaster( true , true );
            if ( retries <= 0 ){
                throw new MongoException( "not talking to master and retries used up" );
            }
            return innerCall( db , coll , m , hostNeeded , retries -1, readPref, decoder );
        }

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
     * @throws MongoException 
     */
    public List<ServerAddress> getServerAddressList() {
        if (_connectionStatus != null) {
            return _connectionStatus.getServerAddressList();
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
        if (_connectionStatus == null) {
            return null;
        }
        return _connectionStatus.asReplicaSetStatus();
    }

    // This call can block if it's not yet known.
    // Be careful when modifying this method, as this method is using the fact that _isMongosDirectConnection
    // is of type Boolean and is null when uninitialized.
    boolean isMongosConnection() {
        if (_connectionStatus != null) {
            return _connectionStatus.asMongosStatus() != null;
        }

        if (_isMongosDirectConnection == null) {
            initDirectConnection();
        }

        return _isMongosDirectConnection != null ? _isMongosDirectConnection : false;
    }

    public String getConnectPoint(){
        ServerAddress master = getAddress();
        return master != null ? master.toString() : null;
    }

    /**
     * This method is called in case of an IOException.
     * It will potentially trigger a checkMaster() to check the status of all servers.
     * @param t the exception thrown
     * @param secondaryOk secondaryOk flag
     * @return true if the request should be retried, false otherwise
     * @throws MongoException
     */
    boolean _error( Throwable t, boolean secondaryOk ){
        if (_connectionStatus == null) {
            // single server, no need to retry
            return false;
        }

        // the replset has at least 1 server up, try to see if should switch master
        // if no server is up, we wont retry until the updater thread finds one
        // this is to cut down the volume of requests/errors when all servers are down
        if ( _connectionStatus.hasServerUp() ){
            checkMaster( true , !secondaryOk );
        }
        return _connectionStatus.hasServerUp();
    }

    class MyPort {

        DBPort get( boolean keep , ReadPreference readPref, ServerAddress hostNeeded ){

            if ( hostNeeded != null ){
                if (_requestPort != null && _requestPort.serverAddress().equals(hostNeeded)) {
                    return _requestPort;
                }

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

            DBPort p;
            if (getReplicaSetStatus() == null){
                if (_masterPortPool == null) {
                    // this should only happen in rare case that no master was ever found
                    // may get here at startup if it's a read, slaveOk=true, and ALL servers are down
                    throw new MongoException("Rare case where master=null, probably all servers are down");
                }
                p = _masterPortPool.get();
            }
            else {
                ReplicaSetStatus.ReplicaSet replicaSet = getReplicaSetStatus()._replicaSetHolder.get();
                ConnectionStatus.Node node = readPref.getNode(replicaSet);
            
                if (node == null)
                    throw new MongoException("No replica set members available in " +  replicaSet + " for " + readPref.toDBObject().toString());
            
                p = _portHolder.get(node.getServerAddress()).get();
            }

            if ( _inRequest ) {
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

            // depending on type of error, may need to close other connections in pool
            boolean recoverable = p.getPool().gotError(e);
            if (!recoverable && _connectionStatus != null && _masterPortPool._addr.equals(p.serverAddress())) {
                ConnectionStatus.Node newMaster = _connectionStatus.ensureMaster();
                if (newMaster != null) {
                    setMaster(newMaster);
                }
            }
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
        boolean _inRequest;
    }

    void checkMaster( boolean force , boolean failIfNoMaster ){

        if ( _connectionStatus != null ){
            if ( _masterPortPool == null || force ){
                ConnectionStatus.Node master = _connectionStatus.ensureMaster();
                if ( master == null ){
                    if ( failIfNoMaster )
                        throw new MongoException( "can't find a master" );
                }
                else {
                    setMaster(master);
                }
            }
        } else {
            // single server, may have to obtain max bson size
            if (_maxBsonObjectSize == 0)
                initDirectConnection();
        }
    }

    synchronized void setMaster(ConnectionStatus.Node master) {
        if (_closed.get()) {
            return;
        }
        setMasterAddress(master.getServerAddress());
        _maxBsonObjectSize = master.getMaxBsonObjectSize();
    }

    /**
     * Fetches the maximum size for a BSON object from the current master server
     * @return the size, or 0 if it could not be obtained
     */
    void initDirectConnection() {
        if (_masterPortPool == null)
            return;
        DBPort port = _masterPortPool.get();
        try {
            CommandResult res = port.runCommand(_mongo.getDB("admin"), new BasicDBObject("isMaster", 1));
            // max size was added in 1.8
            if (res.containsField("maxBsonObjectSize")) {
                _maxBsonObjectSize = (Integer) res.get("maxBsonObjectSize");
            } else {
                _maxBsonObjectSize = Bytes.MAX_OBJECT_SIZE;
            }

            String msg = res.getString("msg");
            _isMongosDirectConnection = msg != null && msg.equals("isdbgrid");
        } catch (Exception e) {
            _logger.log(Level.WARNING, "Exception executing isMaster command on " + port.serverAddress(), e);
        } finally {
            port.getPool().done(port);
        }
    }



    private synchronized boolean setMasterAddress(ServerAddress addr) {
        DBPortPool newPool = _portHolder.get( addr );
        if (newPool == _masterPortPool)
            return false;

        if (  _masterPortPool != null )
            _logger.log(Level.WARNING, "Primary switching from " + _masterPortPool.getServerAddress() + " to " + addr);
        _masterPortPool = newPool;
        return true;
    }

    public String debugString(){
        StringBuilder buf = new StringBuilder( "DBTCPConnector: " );
        if ( _connectionStatus != null ) {
            buf.append( "set : " ).append( _allHosts );
        } else {
            ServerAddress master = getAddress();
            buf.append( master ).append( " " ).append( master != null ? master.getSocketAddress() : null );
        }

        return buf.toString();
    }

    public void close(){
        _closed.set( true );
        if ( _portHolder != null ) {
            try {
                _portHolder.close();
                _portHolder = null;
            } catch (final Throwable t) { /* nada */ }
        }
        if ( _connectionStatus != null ) {
            try {
                _connectionStatus.close();
                _connectionStatus = null;
            } catch (final Throwable t) { /* nada */ }
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
        return ! _closed.get();
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    public int getMaxBsonObjectSize() {
        return _maxBsonObjectSize;
    }

    // expose for unit testing
    MyPort getMyPort() {
        return _myPort.get();
    }

    private volatile DBPortPool _masterPortPool;
    private final Mongo _mongo;
    private DBPortPool.Holder _portHolder;
    private final List<ServerAddress> _allHosts;
    private DynamicConnectionStatus _connectionStatus;

    private final AtomicBoolean _closed = new AtomicBoolean(false);

    private volatile int _maxBsonObjectSize;
    private volatile Boolean _isMongosDirectConnection;

    private ThreadLocal<MyPort> _myPort = new ThreadLocal<MyPort>(){
        protected MyPort initialValue(){
            return new MyPort();
        }
    };

}
