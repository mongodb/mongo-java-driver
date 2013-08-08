// DBPort.java

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

import com.mongodb.util.ThreadUtil;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * represents a Port to the database, which is effectively a single connection to a server
 * Methods implemented at the port level should throw the raw exceptions like IOException,
 * so that the connector above can make appropriate decisions on how to handle.
 *
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class DBPort {
    
    /**
     * the default port
     */
    public static final int PORT = 27017;
    static final boolean USE_NAGLE = false;
    
    static final long CONN_RETRY_TIME_MS = 15000;

    /**
     * creates a new DBPort
     * @param addr the server address
     */
    @SuppressWarnings("deprecation")
    public DBPort( ServerAddress addr ){
        this( addr , null , new MongoOptions() );
    }
    
    DBPort( ServerAddress addr, DBPortPool pool, MongoOptions options ){
        _options = options;
        _sa = addr;
        _addr = addr;
        _pool = pool;

        _logger = Logger.getLogger( _rootLogger.getName() + "." + addr.toString() );
        _decoder = _options.dbDecoderFactory.create();
    }

    Response call( OutMessage msg , DBCollection coll ) throws IOException{
        return go( msg, coll );
    }

    Response call(OutMessage msg, DBCollection coll, DBDecoder decoder) throws IOException{
        return go( msg, coll, false, decoder);
    }
    
    void say( OutMessage msg )
        throws IOException {
        go( msg , null );
    }
    
    private synchronized Response go( OutMessage msg , DBCollection coll )
        throws IOException {
        return go( msg , coll , false, null );
    }

    private synchronized Response go( OutMessage msg , DBCollection coll , DBDecoder decoder ) throws IOException{
        return go( msg, coll, false, decoder );
    }

    private synchronized Response go(OutMessage msg, DBCollection coll, boolean forceResponse, DBDecoder decoder)
        throws IOException {

        if ( _processingResponse ){
            if ( coll == null ){
                // this could be a pipeline and should be safe
            }
            else {
                // this could cause issues since we're reading data off the wire
                throw new IllegalStateException( "DBPort.go called and expecting a response while processing another response" );
            }
        }

        _calls.incrementAndGet();

        if ( _socket == null )
            _open();
        
        if ( _out == null )
            throw new IllegalStateException( "_out shouldn't be null" );

        try {
            msg.prepare();
            _activeState = new ActiveState(msg);
            msg.pipe( _out );

            if ( _pool != null )
                _pool._everWorked = true;
            
            if ( coll == null && ! forceResponse )
                return null;
            
            _processingResponse = true;
            return new Response( _sa , coll , _in , (decoder == null ? _decoder : decoder) );
        }
        catch ( IOException ioe ){
            close();
            throw ioe;
        }
        finally {
            _activeState = null;
            _processingResponse = false;
        }
    }

    synchronized CommandResult getLastError( DB db , WriteConcern concern ) throws IOException{
        DBApiLayer dbAL = (DBApiLayer) db;
        return runCommand( dbAL, concern.getCommand() );
    }

    synchronized private Response findOne( DB db , String coll , DBObject q ) throws IOException {
        OutMessage msg = OutMessage.query( db.getCollection(coll) , 0 , 0 , -1 , q , null );
        try {
            Response res = go( msg , db.getCollection( coll ) , null );
            return res;
        } finally {
            msg.doneWithMessage();
        }
    }

    synchronized CommandResult runCommand( DB db , DBObject cmd ) throws IOException {
        Response res = findOne(db, "$cmd", cmd);
        return convertToCommandResult(cmd, res);
    }

    private CommandResult convertToCommandResult(DBObject cmd, Response res) {
        if ( res.size() == 0 )
            return null;
        if ( res.size() > 1 )
            throw new MongoInternalException( "something is wrong.  size:" + res.size() );

        DBObject data =  res.get(0);
        if ( data == null )
            throw new MongoInternalException( "something is wrong, no command result" );

        CommandResult cr = new CommandResult(res.serverUsed());
        cr.putAll( data );
        return cr;
    }

    synchronized CommandResult tryGetLastError( DB db , long last, WriteConcern concern) throws IOException {
        if ( last != _calls.get() )
            return null;
        
        return getLastError(db, concern);
    }

    /**
     * makes sure that a connection to the server has been opened
     * @throws IOException
     */
    public synchronized void ensureOpen()
        throws IOException {
        
        if ( _socket != null )
            return;
        
        _open();
    }

    void _open() throws IOException {
        
        long sleepTime = 100;

        long maxAutoConnectRetryTime = CONN_RETRY_TIME_MS;
        if (_options.maxAutoConnectRetryTime > 0) {
            maxAutoConnectRetryTime = _options.maxAutoConnectRetryTime;
        }

        boolean successfullyConnected = false;
        final long start = System.currentTimeMillis();
        do {
            try {
                _socket = _options.socketFactory.createSocket();
                _socket.connect( _addr.getSocketAddress() , _options.connectTimeout );

                _socket.setTcpNoDelay( ! USE_NAGLE );
                _socket.setKeepAlive( _options.socketKeepAlive );
                _socket.setSoTimeout( _options.socketTimeout );
                _in = new BufferedInputStream( _socket.getInputStream() );
                _out = _socket.getOutputStream();
                successfullyConnected = true;
            }
            catch ( IOException e ){
                close();

                if (!_options.autoConnectRetry || (_pool != null && !_pool._everWorked))
                    throw e;

                long waitSoFar = System.currentTimeMillis() - start;

                if (waitSoFar >= maxAutoConnectRetryTime)
                    throw e;

                if (sleepTime + waitSoFar > maxAutoConnectRetryTime)
                    sleepTime = maxAutoConnectRetryTime - waitSoFar;

                _logger.log(Level.WARNING, "Exception connecting to " + serverAddress().getHost() + ": " + e +
                        ".  Total wait time so far is " + waitSoFar + " ms.  Will retry after sleeping for " + sleepTime + " ms.");
                ThreadUtil.sleep(sleepTime);
                sleepTime *= 2;
            }
        } while (!successfullyConnected);
    }

    @Override
    public int hashCode(){
        return _addr.hashCode();
    }
    
    /**
     * returns a String representation of the target host
     * @return
     */
    public String host(){
        return _addr.toString();
    }
    
    /**
     * @return the server address for this port
     */
    public ServerAddress serverAddress() {
        return _sa;
    }

    @Override
    public String toString(){
        return "{DBPort  " + host() + "}";
    }
    
    @Override
    protected void finalize() throws Throwable{
        super.finalize();
        close();
    }

    ActiveState getActiveState() {
        return _activeState;
    }

    int getLocalPort() {
        return _socket != null ? _socket.getLocalPort() : -1;
    }

    /**
     * closes the underlying connection and streams
     */
    protected void close(){
        authenticatedDatabases.clear();
                
        if ( _socket != null ){
            try {
                _socket.close();
            }
            catch ( Exception e ){
                // don't care
            }
        }
        
        _in = null;
        _out = null;
        _socket = null;
    }

    CommandResult authenticate(Mongo mongo, final MongoCredential credentials) {
        Authenticator authenticator;
        if (credentials.getMechanism().equals(MongoCredential.MONGODB_CR_MECHANISM)) {
            authenticator = new NativeAuthenticator(mongo, credentials);
        } else if (credentials.getMechanism().equals(MongoCredential.GSSAPI_MECHANISM)) {
            authenticator = new GSSAPIAuthenticator(mongo, credentials);
        } else {
            throw new IllegalArgumentException("Unsupported authentication protocol: " + credentials.getMechanism());
        }
        CommandResult res = authenticator.authenticate();
        authenticatedDatabases.add(credentials.getSource());
        return res;
    }

    void checkAuth(Mongo mongo) throws IOException {
        // get the difference between the set of credentialed databases and the set of authenticated databases on this connection
        Set<String> unauthenticatedDatabases = new HashSet<String>(mongo.getAuthority().getCredentialsStore().getDatabases());
        unauthenticatedDatabases.removeAll(authenticatedDatabases);

        for (String databaseName : unauthenticatedDatabases) {
            authenticate(mongo, mongo.getAuthority().getCredentialsStore().get(databaseName));
        }
    }

    /**
     * Gets the pool that this port belongs to.
     * @return the pool that this port belongs to.
     */
    public DBPortPool getPool() {
        return _pool;
    }

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );

    final ServerAddress _sa;
    final ServerAddress _addr;
    final DBPortPool _pool;
    final MongoOptions _options;
    final Logger _logger;
    final DBDecoder _decoder;
    
    private volatile Socket _socket;
    private volatile InputStream _in;
    private volatile OutputStream _out;

    private volatile boolean _processingResponse;

    // needs synchronization to ensure that modifications are published.
    final Set<String> authenticatedDatabases = Collections.synchronizedSet(new HashSet<String>());

    volatile int _lastThread;
    final AtomicLong _calls = new AtomicLong();
    private volatile ActiveState _activeState;
    private volatile Boolean useCRAMAuthenticationProtocol;

    class ActiveState {
       ActiveState(final OutMessage outMessage) {
           this.outMessage = outMessage;
           this.startTime = System.nanoTime();
           this.threadName = Thread.currentThread().getName();
       }
       final OutMessage outMessage;
       final long startTime;
       final String threadName;
    }

    class GenericSaslAuthenticator extends SaslAuthenticator {
        static final String CRAM_MD5 = "CRAM-MD5";

        private final String mechanism;

        GenericSaslAuthenticator(final Mongo mongo, MongoCredential credentials, String mechanism) {
            super(mongo, credentials);
            this.mechanism = mechanism;
        }

        @Override
        protected SaslClient createSaslClient() {
            try {
                return Sasl.createSaslClient(new String[]{mechanism},
                        credential.getUserName(), MONGODB_PROTOCOL,
                        serverAddress().getHost(), null, new CredentialsHandlingCallbackHandler());
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            }
        }

        @Override
        protected DB getDatabase() {
            return mongo.getDB(credential.getSource());
        }

        @Override
        public String getMechanismName() {
            return mechanism;
        }

        class CredentialsHandlingCallbackHandler implements CallbackHandler {

            public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback callback : callbacks) {
                    if (callback instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback) callback;
                        nameCallback.setName(credential.getUserName());
                    }
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback) callback;
                        String hashedPassword = new String(NativeAuthenticationHelper.createHash(
                                credential.getUserName(), credential.getPassword()));
                        passwordCallback.setPassword(hashedPassword.toCharArray());
                    }
                }
            }
        }
    }

    class GSSAPIAuthenticator extends SaslAuthenticator {
        public static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
        public static final String GSSAPI_MECHANISM = MongoCredential.GSSAPI_MECHANISM;

        GSSAPIAuthenticator(final Mongo mongo, final MongoCredential credentials) {
            super(mongo, credentials);

            if (!this.credential.getMechanism().equals(MongoCredential.GSSAPI_MECHANISM)) {
                throw new MongoException("Incorrect mechanism: " + this.credential.getMechanism());
            }
        }

        @Override
        protected SaslClient createSaslClient() {
            try {
                Map<String, Object> props = new HashMap<String, Object>();
                props.put(Sasl.CREDENTIALS, getGSSCredential(credential.getUserName()));

                return Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, credential.getUserName(), MONGODB_PROTOCOL,
                        serverAddress().getHost(), props, null);
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            } catch (GSSException e) {
                throw new MongoException("Exception initializing GSSAPI credentials", e);
            }
        }

        @Override
        protected DB getDatabase() {
            return mongo.getDB(credential.getSource());
        }

        @Override
        public String getMechanismName() {
            return "GSSAPI";
        }

        private GSSCredential getGSSCredential(String userName) throws GSSException {
            Oid krb5Mechanism = new Oid(GSSAPI_OID);
            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
            return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME,
                    krb5Mechanism, GSSCredential.INITIATE_ONLY);
        }
    }

    abstract class SaslAuthenticator extends Authenticator {
        public static final String MONGODB_PROTOCOL = "mongodb";

        SaslAuthenticator(final Mongo mongo, MongoCredential credentials) {
            super(mongo, credentials);
        }

        public CommandResult authenticate()  {
            SaslClient saslClient = createSaslClient();
            try {
                byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                CommandResult res = sendSaslStart(response);
                res.throwOnError();

                int conversationId = (Integer) res.get("conversationId");

                while (! (Boolean) res.get("done")) {
                    response = saslClient.evaluateChallenge((byte[]) res.get("payload"));

                    if (response == null) {
                        throw new MongoException("SASL protocol error: no client response to challenge");
                    }

                    res = sendSaslContinue(conversationId, response);
                    res.throwOnError();
                }
                return res;
            } catch (IOException e) {
                throw new MongoException.Network("IOException authenticating the connection", e);
            } finally {
                try {
                    saslClient.dispose();
                } catch (SaslException e) {
                    // ignore
                }
            }
        }

        protected abstract SaslClient createSaslClient();

        protected abstract DB getDatabase();

        private CommandResult sendSaslStart(final byte[] outToken) throws IOException {
            DBObject cmd = new BasicDBObject("saslStart", 1).
                    append("mechanism", getMechanismName())
                    .append("payload", outToken != null ? outToken : new byte[0]);
            return runCommand(getDatabase(), cmd);
        }

        private CommandResult sendSaslContinue(final int conversationId, final byte[] outToken) throws IOException {
            DB adminDB = getDatabase();
            DBObject cmd = new BasicDBObject("saslContinue", 1).append("conversationId", conversationId).
                    append("payload", outToken);
            return runCommand(adminDB, cmd);
        }

        public abstract String getMechanismName();
    }

    class NativeAuthenticator extends Authenticator {
        NativeAuthenticator(Mongo mongo, MongoCredential credentials) {
            super(mongo, credentials);
        }

        @Override
        public CommandResult authenticate() {
            try {
                DB db = mongo.getDB(credential.getSource());
                CommandResult res = runCommand(db, NativeAuthenticationHelper.getNonceCommand());
                res.throwOnError();

                res = runCommand(db, NativeAuthenticationHelper.getAuthCommand(credential.getUserName(),
                        credential.getPassword(), res.getString("nonce")));
                res.throwOnError();
                return res;
            } catch (IOException e) {
                throw new MongoException.Network("IOException authenticating the connection", e);
            }
        }
    }

    abstract class Authenticator {
        protected final Mongo mongo;
        protected final MongoCredential credential;

        Authenticator(Mongo mongo, MongoCredential credential) {
            this.mongo = mongo;
            this.credential = credential;
        }

        abstract CommandResult authenticate();
    }
}
