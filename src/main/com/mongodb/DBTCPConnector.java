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

package com.mongodb;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterConnectionMode.Single;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ClusterType.Sharded;
import static com.mongodb.ClusterType.Unknown;
import static com.mongodb.MongoAuthority.Type.Set;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.bson.util.Assertions.isTrue;

/**
 * @deprecated This class is NOT part of the public API. It will be dropped in 3.x releases.
 */
@Deprecated
public class DBTCPConnector implements DBConnector {

    private static final AtomicInteger NEXT_CLUSTER_ID = new AtomicInteger(1);

    private volatile boolean _closed;

    private final Mongo _mongo;

    private Cluster cluster;

    private final MyPort _myPort = new MyPort();

    private final ClusterConnectionMode connectionMode;

    private ClusterType type = ClusterType.Unknown;
    private MongosHAServerSelector mongosHAServerSelector;

    /**
     * @param mongo the Mongo instance
     * @throws MongoException
     */
    public DBTCPConnector( Mongo mongo  ) {
        _mongo = mongo;
        connectionMode = _mongo.getAuthority().getType() == Set || _mongo.getMongoOptions().getRequiredReplicaSetName() != null ?
                         Multiple : Single;
    }

    public void start() {
        isTrue("open", !_closed);

        MongoOptions options = _mongo.getMongoOptions();

        String clusterId = Integer.toString(NEXT_CLUSTER_ID.getAndIncrement());
        cluster =
        Clusters.create(clusterId,
                        ClusterSettings.builder()
                                       .hosts(_mongo.getAuthority().getServerAddresses())
                                       .mode(connectionMode)
                                       .requiredReplicaSetName(_mongo.getMongoOptions().getRequiredReplicaSetName())
                                       .build(),
                        ServerSettings.builder()
                                      .heartbeatFrequency(options.heartbeatFrequencyMS, MILLISECONDS)
                                      .heartbeatConnectRetryFrequency(options.heartbeatConnectRetryFrequencyMS, MILLISECONDS)
                                      .heartbeatSocketSettings(SocketSettings.builder()
                                                                             .connectTimeout(options.heartbeatConnectTimeoutMS,
                                                                                             MILLISECONDS)
                                                                             .readTimeout(options.heartbeatReadTimeoutMS, MILLISECONDS)
                                                                             .socketFactory(_mongo.getMongoOptions().getSocketFactory())
                                                                             .build())
                                      .build(),
                        null, _mongo);
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
        isTrue("open", !_closed);
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
        isTrue("open", !_closed);
        _myPort.requestEnsureConnection();
    }

    private WriteResult _checkWriteError( DB db, DBPort port , WriteConcern concern )
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
        isTrue("open", !_closed);
        return say( db , m , concern , (ServerAddress) null);
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
        isTrue("open", !_closed);
        DBPort port = _myPort.get(true, ReadPreference.primary(), hostNeeded);

        try {
            return say(db, m, concern, port);
        }
        finally {
            _myPort.done(port);
        }
    }

    WriteResult say(final DB db, final OutMessage m, final WriteConcern concern, final DBPort port){
        isTrue("open", !_closed);

        if (concern == null) {
            throw new IllegalArgumentException("Write concern is null");
        }

        try {
            return doOperation(db, port, new DBPort.Operation<WriteResult>() {
                @Override
                public WriteResult execute() throws IOException {
                    port.say(m);
                    if (concern.callGetLastError()) {
                        return _checkWriteError(db, port, concern);
                    } else {
                        return new WriteResult(db, port, concern);
                    }
                }
            });
        } catch (MongoException.Network e) {
            if ( concern.raiseNetworkErrors() )
                throw e;

            CommandResult res = new CommandResult(port.serverAddress());
            res.put( "ok" , false );
            res.put( "$err" , "NETWORK ERROR" );
            return new WriteResult( res , concern );
        } finally {
            m.doneWithMessage();
        }
    }

    <T> T doOperation(final DB db, final DBPort port, final DBPort.Operation<T> operation){
        try {
            port.checkAuth( db.getMongo() );
            return port.doOperation(operation);
        }
        catch ( MongoException re ){
            throw re;
        }
        catch ( IOException ioe ){
            _myPort.error(port, ioe);
            throw new MongoException.Network("Operation on server " + port.getAddress() + " failed" , ioe );
        }
        catch ( RuntimeException re ){
            _myPort.error(port, re);
            throw re;
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
        isTrue("open", !_closed);
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
        isTrue("open", !_closed);
        return call(db, coll, m, hostNeeded, retries, null, null);
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
        isTrue("open", !_closed);
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
        isTrue("open", !_closed);
        ClusterDescription clusterDescription = getClusterDescription();
        if (connectionMode == Single) {
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
        isTrue("open", !_closed);
        return _mongo._authority.getServerAddresses();
    }

    /**
     * Gets the list of server addresses currently seen by the connector.
     * This includes addresses auto-discovered from a replica set.
     * @return
     * @throws MongoException
     */
    public List<ServerAddress> getServerAddressList() {
        isTrue("open", !_closed);
        List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();
        ClusterDescription clusterDescription = getClusterDescription();
        for (ServerDescription serverDescription : clusterDescription.getAll()) {
            serverAddressList.add(serverDescription.getAddress());
        }
        return serverAddressList;
    }

    public ReplicaSetStatus getReplicaSetStatus() {
        isTrue("open", !_closed);
        return getType() == ReplicaSet && connectionMode == Multiple ? new ReplicaSetStatus(getClusterDescription()) : null;
    }

    // This call can block if it's not yet known.
    boolean isMongosConnection() {
        isTrue("open", !_closed);
        return getType() == Sharded;
    }

    public String getConnectPoint(){
        isTrue("open", !_closed);
        ServerAddress master = getAddress();
        return master != null ? master.toString() : null;
    }

    private boolean shouldRetryQuery(ReadPreference readPreference, final DBCollection coll, final IOException ioe, final int remainingRetries) {
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
        return connectionMode == Multiple && getType() == ReplicaSet;
    }

    private ClusterDescription getClusterDescription() {
        return cluster.getDescription(getClusterWaitTimeMS(), MILLISECONDS);
    }

    private int getClusterWaitTimeMS() {
        return Math.min(_mongo.getMongoOptions().maxWaitTime, _mongo.getMongoOptions().connectTimeout);
    }

    private int getConnectionWaitTimeMS() {
        return _mongo.getMongoOptions().maxWaitTime;
    }

    DBPort getPrimaryPort() {
        isTrue("open", !_closed);
        return _myPort.get(true, ReadPreference.primary(), null);
    }

    void releasePort(final DBPort port) {
        _myPort.done(port);
    }

    ServerDescription getServerDescription(final ServerAddress address) {
        isTrue("open", !_closed);
        return getClusterDescription().getByServerAddress(address);
    }

    class MyPort {

        DBPort get( boolean keep , ReadPreference readPref, ServerAddress hostNeeded ){

            DBPort pinnedRequestPort = getPinnedRequestPortForThread();

            if ( hostNeeded != null ) {
                if (pinnedRequestPort != null && pinnedRequestPort.serverAddress().equals(hostNeeded)) {
                    return pinnedRequestPort;
                }

                // asked for a specific host
                return getConnection(new ServerAddressSelector(hostNeeded));
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
                pinnedRequestPort.getProvider().release(pinnedRequestPort);
                setPinnedRequestPortForThread(null);
            }

            DBPort port = getConnection(createServerSelector(readPref));

            // if within request, remember port to stick to same server
            if (threadHasPinnedRequest()) {
                setPinnedRequestPortForThread(port);
            }

            return port;
        }

        private boolean portIsAPrimary(final DBPort pinnedRequestPort) {
            for (ServerDescription cur : getClusterDescription().getPrimaries()) {
                if (cur.getAddress().equals(pinnedRequestPort.serverAddress())) {
                    return true;
                }
            }
            return false;
        }

        void done( DBPort port ) {
            Connection requestPort = getPinnedRequestPortForThread();

            // keep request port
            if (port != requestPort) {
                port.getProvider().release(port);
            }
        }

        /**
         * call this method when there is an IOException or other low level error on port.
         * @param port
         * @param e
         */
        void error( DBPort port , Exception e ){
            if (!(e instanceof InterruptedIOException)) {
                try {
                    // no need to wait if the server is no longer available
                    cluster.getServer(new ServerAddressSelector(port.getAddress()), 1, NANOSECONDS).invalidate();
                } catch (MongoTimeoutException timeoutException) {
                    // ignore this if the server is already no longer available
                }
            }
            port.close();
            pinnedRequestStatusThreadLocal.remove();
        }

        void requestEnsureConnection(){
            if ( !threadHasPinnedRequest() )
                return;

            if ( getPinnedRequestPortForThread() != null )
                return;

            setPinnedRequestPortForThread(getConnection(createServerSelector(ReadPreference.primary())));
        }

        private DBPort getConnection(final ServerSelector serverSelector) {
            return (DBPort) getServer(serverSelector).getConnection(getConnectionWaitTimeMS(), MILLISECONDS);
        }

        void requestStart() {
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
                        current.requestPort.getProvider().release(current.requestPort);
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

    private ServerSelector createServerSelector(final ReadPreference readPreference) {
        if (connectionMode == Multiple) {
            List<ServerSelector> serverSelectorList = new ArrayList<ServerSelector>();
            if (getType() == Sharded) {
                serverSelectorList.add(getMongosHAServerSelector());
            } else if (getType() == ReplicaSet) {
                serverSelectorList.add(new ReadPreferenceServerSelector(readPreference));
            } else {
                serverSelectorList.add(new AnyServerSelector());
            }
            serverSelectorList.add(new LatencyMinimizingServerSelector(_mongo.getMongoOptions().acceptableLatencyDifferenceMS,
                                                                       MILLISECONDS));
            return new CompositeServerSelector(serverSelectorList);
        } else {
            return new AnyServerSelector();
        }
    }

    private synchronized ClusterType getType() {
        if (type == Unknown) {
            type = getClusterDescription().getType();
        }
        return type;
    }

    // There needs to be just one instance of this because it's stateful between requests
    private synchronized MongosHAServerSelector getMongosHAServerSelector() {
        if (mongosHAServerSelector == null) {
            mongosHAServerSelector = new MongosHAServerSelector();
        }
        return mongosHAServerSelector;
    }

    static class PinnedRequestStatus {
        DBPort requestPort;
        public int nestedBindings;
    }


    public String debugString(){
        return getClusterDescription().getShortDescription();
    }

    public void close(){
        _closed = true;
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    /**
     * Assigns a new DBPortPool for a given ServerAddress.
     * This is used to obtain a new pool when the resolved IP of a host changes, for example.
     * User application should not have to call this method directly.
     * @param addr
     */
    public void updatePortPool(ServerAddress addr) {
    }

    /**
     * Gets the DBPortPool associated with a ServerAddress.
     * @param addr
     * @return
     */
    public DBPortPool getDBPortPool(ServerAddress addr) {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen(){
        return !_closed;
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
        ClusterDescription clusterDescription = getClusterDescription();
        if (clusterDescription.getPrimaries().isEmpty()) {
            return Bytes.MAX_OBJECT_SIZE;
        }
        return clusterDescription.getPrimaries().get(0).getMaxDocumentSize();
    }

    // expose for unit testing
    MyPort getMyPort() {
        return _myPort;
    }

    private Server getServer(final ServerSelector serverSelector) {
        return cluster.getServer(serverSelector, getClusterWaitTimeMS(), MILLISECONDS);
    }
}
