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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @deprecated This class is NOT part of the public API. It will be dropped in 3.x releases.
 */
@Deprecated
public class DBTCPConnector implements DBConnector {

    static Logger _logger = Logger.getLogger( Bytes.LOGGER.getName() + ".tcp" );

    /**
     * @param mongo the Mongo instance
     * @throws MongoException
     */
    public DBTCPConnector( Mongo mongo  ) {
        _mongo = mongo;
        _portHolder = new DBPortPool.Holder( mongo._options );
        MongoAuthority.Type type = mongo.getAuthority().getType();
        if (type == MongoAuthority.Type.Direct) {
            setMasterAddress(mongo.getAuthority().getServerAddresses().get(0));
        } else if (type == MongoAuthority.Type.Set) {
            _connectionStatus = new DynamicConnectionStatus(mongo, mongo.getAuthority().getServerAddresses());
        } else {
            throw new IllegalArgumentException("Unsupported authority type: " + type);
        }
    }

    public void start() {
        if (_connectionStatus != null) {
            _connectionStatus.start();
        }
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
        _myPort.requestStart();
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
        _myPort.requestDone();
    }

    /**
     * @throws MongoException
     */
    @Override
    public void requestEnsureConnection(){
        checkMaster( false , true );
        _myPort.requestEnsureConnection();
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

        DBPort port = _myPort.get(true, ReadPreference.primary(), hostNeeded);

        try {
            port.checkAuth( db.getMongo() );
            port.say( m );
            if ( concern.callGetLastError() ){
                return _checkWriteError( db , port , concern );
            }
            else {
                return new WriteResult( db , port , concern );
            }
        }
        catch ( IOException ioe ){
            _myPort.error(port, ioe);
            _error( ioe, false );

            if ( concern.raiseNetworkErrors() )
                throw new MongoException.Network("Write operation to server " + port.host() + " failed on database " + db , ioe );

            CommandResult res = new CommandResult(port.serverAddress());
            res.put( "ok" , false );
            res.put( "$err" , "NETWORK ERROR" );
            return new WriteResult( res , concern );
        }
        catch ( MongoException me ){
            throw me;
        }
        catch ( RuntimeException re ){
            _myPort.error(port, re);
            throw re;
        }
        finally {
            _myPort.done(port);
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

        final DBPort port = _myPort.get(false, readPref, hostNeeded);

        Response res = null;
        boolean retry = false;
        try {
            port.checkAuth( db.getMongo() );
            res = port.call( m , coll, decoder );
            if ( res._responseTo != m.getId() )
                throw new MongoException( "ids don't match" );
        }
        catch ( IOException ioe ){
            _myPort.error(port, ioe);
            retry = retries > 0 && !coll._name.equals( "$cmd" )
                    && !(ioe instanceof SocketTimeoutException) && _error( ioe, secondaryOk );
            if ( !retry ){
                throw  new MongoException.Network("Read operation to server " + port.host() + " failed on database " + db , ioe );
            }
        }
        catch ( RuntimeException re ){
            _myPort.error(port, re);
            throw re;
        } finally {
            _myPort.done(port);
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
        return _mongo._authority.getServerAddresses();
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
        if (_connectionStatus instanceof ReplicaSetStatus) {
            return (ReplicaSetStatus) _connectionStatus;
        } else if (_connectionStatus instanceof DynamicConnectionStatus) {
            return ((DynamicConnectionStatus) _connectionStatus).asReplicaSetStatus();
        } else {
            return null;
        }
    }

    // This call can block if it's not yet known.
    // Be careful when modifying this method, as this method is using the fact that _isMongosDirectConnection
    // is of type Boolean and is null when uninitialized.
    boolean isMongosConnection() {
        if (_connectionStatus instanceof MongosStatus) {
            return true;
        } else if (_connectionStatus instanceof DynamicConnectionStatus) {
            return ((DynamicConnectionStatus) _connectionStatus).asMongosStatus() != null;
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

            DBPort pinnedRequestPort = getPinnedRequestPortForThread();

            if ( hostNeeded != null ) {
                if (pinnedRequestPort != null && pinnedRequestPort.serverAddress().equals(hostNeeded)) {
                    return pinnedRequestPort;
                }

                // asked for a specific host
                return _portHolder.get( hostNeeded ).get();
            }

            if ( pinnedRequestPort != null ){
                // we are within a request, and have a port, should stick to it
                if ( pinnedRequestPort.getPool() == _masterPortPool || !keep ) {
                    // if keep is false, it's a read, so we use port even if master changed
                    return pinnedRequestPort;
                }

                // it's write and master has changed
                // we fall back on new master and try to go on with request
                // this may not be best behavior if spec of request is to stick with same server
                pinnedRequestPort.getPool().done(pinnedRequestPort);
                setPinnedRequestPortForThread(null);
            }

            DBPort port;
            if (getReplicaSetStatus() == null){
                if (_masterPortPool == null) {
                    // this should only happen in rare case that no master was ever found
                    // may get here at startup if it's a read, slaveOk=true, and ALL servers are down
                    throw new MongoException("Rare case where master=null, probably all servers are down");
                }
                port = _masterPortPool.get();
            }
            else {
                ReplicaSetStatus.ReplicaSet replicaSet = getReplicaSetStatus()._replicaSetHolder.get();
                ConnectionStatus.Node node = readPref.getNode(replicaSet);

                if (node == null)
                    throw new MongoException("No replica set members available in " +  replicaSet + " for " + readPref.toDBObject().toString());

                port = _portHolder.get(node.getServerAddress()).get();
            }

            // if within request, remember port to stick to same server
            if (threadHasPinnedRequest()) {
                setPinnedRequestPortForThread(port);
            }

            return port;
        }

        void done( DBPort port ) {
            DBPort requestPort = getPinnedRequestPortForThread();

            // keep request port
            if (port != requestPort) {
                port.getPool().done(port);
            }
        }

        /**
         * call this method when there is an IOException or other low level error on port.
         * @param port
         * @param e
         */
        void error( DBPort port , Exception e ){
            port.close();
            pinnedRequestStatusThreadLocal.remove();

            // depending on type of error, may need to close other connections in pool
            boolean recoverable = port.getPool().gotError(e);
            if (!recoverable && _connectionStatus != null && _masterPortPool._addr.equals(port.serverAddress())) {
                ConnectionStatus.Node newMaster = _connectionStatus.ensureMaster();
                if (newMaster != null) {
                    setMaster(newMaster);
                }
            }
        }

        void requestEnsureConnection(){
            if ( !threadHasPinnedRequest() )
                return;

            if ( getPinnedRequestPortForThread() != null )
                return;

            setPinnedRequestPortForThread(_masterPortPool.get());
        }

        void requestStart(){
            PinnedRequestStatus current = getPinnedRequestStatusForThread();
            if (current == null) {
                pinnedRequestStatusThreadLocal.set(new PinnedRequestStatus());
            }
            else {
                current.nestedBindings++;
            }
        }

        void requestDone(){
            PinnedRequestStatus current = getPinnedRequestStatusForThread();
            if (current != null) {
                if (current.nestedBindings > 0) {
                    current.nestedBindings--;
                }
                else  {
                    pinnedRequestStatusThreadLocal.remove();
                    if (current.requestPort != null)
                        current.requestPort.getPool().done(current.requestPort);
                }
            }
        }

        PinnedRequestStatus getPinnedRequestStatusForThread() {
            return pinnedRequestStatusThreadLocal.get();
        }

        boolean threadHasPinnedRequest() {
            return pinnedRequestStatusThreadLocal.get() != null;
        }

        DBPort getPinnedRequestPortForThread() {
            return threadHasPinnedRequest() ? pinnedRequestStatusThreadLocal.get().requestPort : null;
        }

        void setPinnedRequestPortForThread(final DBPort port) {
            pinnedRequestStatusThreadLocal.get().requestPort = port;
        }

        private final ThreadLocal<PinnedRequestStatus> pinnedRequestStatusThreadLocal = new ThreadLocal<PinnedRequestStatus>();
    }

    static class PinnedRequestStatus {
        DBPort requestPort;
        public int nestedBindings;
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
            buf.append( "set : " ).append( _mongo._authority.getServerAddresses() );
        } else {
            buf.append(getAddress());
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

    @Override
    public CommandResult authenticate(MongoCredential credentials) {
        checkMaster(false, true);
        final DBPort port = _myPort.get(false, ReadPreference.primaryPreferred(), null);

        try {
            CommandResult result = port.authenticate(_mongo, credentials);
            _mongo.getAuthority().getCredentialsStore().add(credentials);
            return result;
       } finally {
            _myPort.done(port);
        }
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
        return _myPort;
    }

    private volatile DBPortPool _masterPortPool;
    private final Mongo _mongo;
    private DBPortPool.Holder _portHolder;
    private ConnectionStatus _connectionStatus;

    private final AtomicBoolean _closed = new AtomicBoolean(false);

    private volatile int _maxBsonObjectSize;
    private volatile Boolean _isMongosDirectConnection;

    MyPort _myPort = new MyPort();
}
