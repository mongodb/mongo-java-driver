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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterConnectionMode.Single;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ClusterType.Sharded;
import static com.mongodb.MongoAuthority.Type.Direct;
import static org.bson.util.Assertions.isTrue;

/**
 * @deprecated This class is NOT part of the public API. It will be dropped in 3.x releases.
 */
@Deprecated
public class DBTCPConnector implements DBConnector {

    /**
     * @param mongo the Mongo instance
     * @throws MongoException
     */
    public DBTCPConnector( Mongo mongo  ) {
        _mongo = mongo;
        _portHolder = new DBPortPool.Holder( mongo._options );
    }

    public void start() {
        scheduledExecutorService = Executors.newScheduledThreadPool(_mongo.getAuthority().getServerAddresses().size());
        cluster = Clusters.create(ClusterSettings.builder()
                                                 .hosts(_mongo.getAuthority().getServerAddresses())
                                                 .mode(_mongo.getAuthority().getType() == Direct ? Single : Multiple)
                                                 .build(),
                                  ServerSettings.builder().build(),
                                  scheduledExecutorService, null,
                                  _mongo);
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
        _myPort.requestEnsureConnection();
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
        isTrue("open", !_closed.get());

        if (concern == null) {
            throw new IllegalArgumentException("Write concern is null");
        }

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
                               final int remainingRetries, ReadPreference readPref, final DBDecoder decoder) {
        if (readPref == null)
            readPref = ReadPreference.primary();

        if (readPref == ReadPreference.primary() && m.hasOption( Bytes.QUERYOPTION_SLAVEOK ))
           readPref = ReadPreference.secondaryPreferred();

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
            retry = shouldRetryQuery(readPref, coll, ioe, remainingRetries);
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
            return innerCall( db , coll , m , hostNeeded , remainingRetries - 1 , readPref, decoder );

        ServerError err = res.getError();

        if ( err != null && err.isNotMasterError() ){
            if ( remainingRetries <= 0 ){
                throw new MongoException( "not talking to master and retries used up" );
            }
            return innerCall( db , coll , m , hostNeeded , remainingRetries -1, readPref, decoder );
        }

        return res;
    }

    public ServerAddress getAddress() {
        ClusterDescription clusterDescription = cluster.getDescription();
        if (clusterDescription.getConnectionMode() == Single) {
            return clusterDescription.getAny().get(0).getAddress();
        }
        if (clusterDescription.getPrimaries().isEmpty()) {
            return null;
        }
        return clusterDescription.getPrimaries().get(0).getAddress();
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
        List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
        ClusterDescription clusterDescription = cluster.getDescription();
        for (ServerDescription serverDescription : clusterDescription.getAll()) {
            serverAddressList.add(serverDescription.getAddress());
        }
        return serverAddressList;
    }

    public ReplicaSetStatus getReplicaSetStatus() {
        return cluster.getDescription().getType() == ReplicaSet && cluster.getDescription().getConnectionMode() == Multiple
               ? new ReplicaSetStatus(cluster) : null;
    }

    // This call can block if it's not yet known.
    boolean isMongosConnection() {
        return cluster.getDescription().getType() == Sharded;
    }

    public String getConnectPoint(){
        ServerAddress master = getAddress();
        return master != null ? master.toString() : null;
    }

    boolean shouldRetryQuery(ReadPreference readPreference, final DBCollection coll, final IOException ioe, final int remainingRetries) {
        if (remainingRetries == 0) {
            return false;
        }
        if (coll._name.equals("$cmd")) {
            return false;
        }
        if (ioe instanceof SocketTimeoutException) {
            return false;
        }
        if (readPreference.equals(ReadPreference.primary())) {
            return false;
        }
        return cluster.getDescription().getConnectionMode() == Multiple && cluster.getDescription().getType() == ReplicaSet;
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
                if ( portIsAPrimary(pinnedRequestPort) || !keep ) {
                    // if keep is false, it's a read, so we use port even if primary changed
                    return pinnedRequestPort;
                }

                // it's write and primary has changed
                // we fall back on new primary and try to go on with request
                // this may not be best behavior if spec of request is to stick with same server
                pinnedRequestPort.getPool().done(pinnedRequestPort);
                setPinnedRequestPortForThread(null);
            }

            Server server = cluster.getServer(new ReadPreferenceServerSelector(readPref));
            DBPort port = _portHolder.get(server.getDescription().getAddress()).get();

            // if within request, remember port to stick to same server
            if (threadHasPinnedRequest()) {
                setPinnedRequestPortForThread(port);
            }

            return port;
        }

        private boolean portIsAPrimary(final DBPort pinnedRequestPort) {
            for (ServerDescription cur : cluster.getDescription().getPrimaries()) {
                if (cur.getAddress().equals(pinnedRequestPort.serverAddress())) {
                    return true;
                }
            }
            return false;
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
        }

        void requestEnsureConnection(){
            if ( !threadHasPinnedRequest() )
                return;

            if ( getPinnedRequestPortForThread() != null )
                return;

            ClusterDescription clusterDescription = cluster.getDescription();
            if (clusterDescription.getPrimaries().isEmpty()) {
                throw new MongoTimeoutException("Could not ensure a connection to a primary server");
            }
            setPinnedRequestPortForThread(_portHolder.get(clusterDescription.getPrimaries().get(0).getAddress()).get());
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


    public String debugString(){
        return cluster.getDescription().getShortDescription();
    }

    public void close(){
        _closed.set( true );
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
        if ( _portHolder != null ) {
            try {
                _portHolder.close();
                _portHolder = null;
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
        ClusterDescription clusterDescription = cluster.getDescription();
        if (clusterDescription.getPrimaries().isEmpty()) {
            return Bytes.MAX_OBJECT_SIZE;
        }
        return clusterDescription.getPrimaries().get(0).getMaxDocumentSize();
    }

    // expose for unit testing
    MyPort getMyPort() {
        return _myPort;
    }

    private final Mongo _mongo;
    private DBPortPool.Holder _portHolder;

    private final AtomicBoolean _closed = new AtomicBoolean(false);

    private ScheduledExecutorService scheduledExecutorService;
    private Cluster cluster;

    MyPort _myPort = new MyPort();
}
