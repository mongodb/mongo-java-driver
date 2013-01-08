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
import com.mongodb.util.Util;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * represents a Port to the database, which is effectively a single connection to a server
 * Methods implemented at the port level should throw the raw exceptions like IOException,
 * so that the connector above can make appropriate decisions on how to handle.
 */
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
    public DBPort( ServerAddress addr ){
        this( addr , null , new MongoOptions() );
    }
    
    DBPort( ServerAddress addr, DBPortPool pool, MongoOptions options ){
        _options = options;
        _sa = addr;
        _addr = addr.getSocketAddress();
        _pool = pool;

        _hashCode = _addr.hashCode();

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
        Response res = go( msg , db.getCollection( coll ) , null );
        return res;
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

        CommandResult cr = new CommandResult(cmd, res.serverUsed());
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

    boolean _open()
        throws IOException {
        
        long sleepTime = 100;

        long maxAutoConnectRetryTime = CONN_RETRY_TIME_MS;
        if (_options.maxAutoConnectRetryTime > 0) {
            maxAutoConnectRetryTime = _options.maxAutoConnectRetryTime;
        }

        final long start = System.currentTimeMillis();
        while ( true ){
            
            IOException lastError = null;

            try {
                _socket = _options.socketFactory.createSocket();
                _socket.connect( _addr , _options.connectTimeout );

                _socket.setTcpNoDelay( ! USE_NAGLE );
                _socket.setKeepAlive( _options.socketKeepAlive );
                _socket.setSoTimeout( _options.socketTimeout );
                _in = new BufferedInputStream( _socket.getInputStream() );
                _out = _socket.getOutputStream();
                return true;
            }
            catch ( IOException ioe ){
                lastError = new IOException( "couldn't connect to [" + _addr + "] bc:" + ioe );
                _logger.log( Level.INFO , "connect fail to : " + _addr , ioe );
                close();
            }
            
            if ( ! _options.autoConnectRetry || ( _pool != null && ! _pool._everWorked ) )
                throw lastError;
            
            long sleptSoFar = System.currentTimeMillis() - start;

            if ( sleptSoFar >= maxAutoConnectRetryTime )
                throw lastError;
            
            if ( sleepTime + sleptSoFar > maxAutoConnectRetryTime )
                sleepTime = maxAutoConnectRetryTime - sleptSoFar;

            _logger.severe( "going to sleep and retry.  total sleep time after = " + ( sleptSoFar + sleptSoFar ) + "ms  this time:" + sleepTime + "ms" );
            ThreadUtil.sleep( sleepTime );
            sleepTime *= 2;
            
        }
    }

    @Override
    public int hashCode(){
        return _hashCode;
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
        if (_saslAuthenticator != null) {
           _saslAuthenticator.clear();
        }
        _authed.clear();
                
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
    
    void checkAuth(DB db) throws IOException {
        if (db.getMongo().getCredentials() != null) {
            if (_saslAuthenticator == null) {
                if (db.getMongo().getCredentials().getMechanism().equals(MongoCredentials.GSSAPI_MECHANISM)) {
                   _saslAuthenticator = new GSSAPIAuthenticator(db.getMongo());
                } else if (db.getMongo().getCredentials().getMechanism().equals(MongoCredentials.CRAM_MD5_MECHANISM)) {
                    _saslAuthenticator = new CRAMMD5Authenticator(db.getMongo());
                } else {
                    throw new MongoException("Unsupported authentication mechanism: " + db.getMongo().getCredentials().getMechanism());
                }
                _saslAuthenticator.authenticate();
            }
        }
        else {
            DB.AuthenticationCredentials credentials = db.getAuthenticationCredentials();
            if (credentials == null) {
                if (db._name.equals("admin"))
                    return;
                checkAuth(db._mongo.getDB("admin"));
                return;
            }
            if (_authed.containsKey(db))
                return;

            CommandResult res = runCommand(db, credentials.getNonceCommand());
            res.throwOnError();

            res = runCommand(db, credentials.getAuthCommand(res.getString("nonce")));
            res.throwOnError();

            _authed.put(db, true);
        }
    }

    /**
     * Gets the pool that this port belongs to
     * @return
     */
    public DBPortPool getPool() {
        return _pool;
    }

    final int _hashCode;
    final ServerAddress _sa;
    final InetSocketAddress _addr;
    final DBPortPool _pool;
    final MongoOptions _options;
    final Logger _logger;
    final DBDecoder _decoder;
    
    private volatile Socket _socket;
    private volatile InputStream _in;
    private volatile OutputStream _out;

    private volatile boolean _processingResponse;

    private volatile SaslAuthenticator _saslAuthenticator;
    private final Map<DB,Boolean> _authed = new ConcurrentHashMap<DB, Boolean>( );

    volatile int _lastThread;
    final AtomicLong _calls = new AtomicLong();
    private volatile ActiveState _activeState;

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

    class CRAMMD5Authenticator extends SaslAuthenticator {
        public static final String CRAM_MD5_MECHANISM = "CRAM-MD5";

        CRAMMD5Authenticator(final Mongo mongo) {
            super(mongo);

            if (!mongo.getCredentials().getMechanism().equals(MongoCredentials.CRAM_MD5_MECHANISM)) {
                throw new MongoException("Incorrect mechanism: " + mongo.getCredentials().getMechanism());
            }
        }

        @Override
        protected SaslClient createSaslClient() {
            try {
                return Sasl.createSaslClient(new String[]{CRAM_MD5_MECHANISM}, mongo.getCredentials().getUserName(), MONGODB_PROTOCOL,
                        serverAddress().getHost(), null, new CredentialsHandlingCallbackHandler());
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            }
        }

        @Override
        protected Object getMechanism() {
            return CRAM_MD5_MECHANISM;
        }

        @Override
        protected String getUserNameForMechanism() {
            return mongo.getCredentials().getUserName();
        }

        @Override
        protected DB getDatabase() {
            return mongo.getDB(mongo.getCredentials().getDatabase());
        }

        class CredentialsHandlingCallbackHandler implements CallbackHandler {

            public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback callback : callbacks) {
                    if (callback instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback) callback;
                        nameCallback.setName(getUserNameForMechanism());
                    }
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback) callback;
                        String hashedPassword = new String(createHash(mongo.getCredentials().getUserName(),
                                mongo.getCredentials().getPassword()));
                        passwordCallback.setPassword(hashedPassword.toCharArray());
                    }
                }
            }
        }

        // TODO: copied from DB.AuthenticationCredentials
        byte[] createHash( String userName , char[] password ){
            ByteArrayOutputStream bout = new ByteArrayOutputStream( userName.length() + 20 + password.length );
            try {
                bout.write(userName.getBytes());
                bout.write( ":mongo:".getBytes() );
                for (final char ch : password) {
                    if (ch >= 128)
                        throw new IllegalArgumentException("can't handle non-ascii passwords yet");
                    bout.write((byte) ch);
                }
            }
            catch ( IOException ioe ){
                throw new RuntimeException( "impossible" , ioe );
            }
            return Util.hexMD5(bout.toByteArray()).getBytes();
        }
    }

    class GSSAPIAuthenticator extends SaslAuthenticator {
        public static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
        public static final String GSSAPI_MECHANISM = "GSSAPI";

        GSSAPIAuthenticator(final Mongo mongo) {
            super(mongo);

            if (!mongo.getCredentials().getMechanism().equals(MongoCredentials.GSSAPI_MECHANISM)) {
                throw new MongoException("Incorrect mechanism: " + mongo.getCredentials().getMechanism());
            }
        }

        @Override
        protected SaslClient createSaslClient() {
            try {
                Map<String, Object> props = new HashMap<String, Object>();
                props.put(Sasl.CREDENTIALS, getGSSCredential(mongo.getCredentials().getUserName()));

                return Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, mongo.getCredentials().getUserName(), MONGODB_PROTOCOL,
                        serverAddress().getHost(), props, null);
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            } catch (GSSException e) {
                throw new MongoException("Exception initializing GSSAPI credentials", e);
            }
        }

        @Override
        protected Object getMechanism() {
            return GSSAPI_MECHANISM;
        }

        @Override
        protected String getUserNameForMechanism() {
            return mongo.getCredentials().getUserName();
        }

        @Override
        protected DB getDatabase() {
            return mongo.getDB("$external");
        }

        private GSSCredential getGSSCredential(String userName) throws GSSException {
            Oid krb5Mechanism = new Oid(GSSAPI_OID);
            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
            return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME,
                    krb5Mechanism, GSSCredential.INITIATE_ONLY);
        }
    }

    abstract class SaslAuthenticator {
        public static final String MONGODB_PROTOCOL = "mongodb";

        protected final Mongo mongo;
        private final Map<DB, Boolean> authorizeDatabases = new ConcurrentHashMap<DB, Boolean>();
        private volatile boolean authenticated;

        SaslAuthenticator(final Mongo mongo) {
            this.mongo = mongo;
        }

        void clear() {
            authorizeDatabases.clear();
            authenticated = false;
        }

        void authenticate()  {
            if (authenticated) {
                return;
            }

            SaslClient saslClient = createSaslClient();
            try {
                // Get optional initial response
                byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                CommandResult res = sendSaslStart(response);
                res.throwOnError();

                int conversationId = (Integer) res.get("conversationId");

                while (!saslClient.isComplete() && (res.get("done").equals(Boolean.FALSE))) {
                    // Evaluate server challenge
                    response = saslClient.evaluateChallenge((byte[]) res.get("payload"));

                    if (res.get("done").equals(Boolean.TRUE)) {
                        // done; server doesn't expect any more SASL data
                        if (response != null) {
                            throw new MongoException("SASL protocol error: attempting to send response after completion");
                        }
                        break;
                    } else {
                        res = sendSaslContinue(conversationId, response);
                        res.throwOnError();
                    }
                }
                authenticated = true;
            } catch (IOException e) {
                // TODO: Fix exception message.
                throw new MongoException("Exception authenticating with SASL", e);
            } finally {
                try {
                    saslClient.dispose();
                } catch (SaslException e) {
                    // ignore
                }
            }
        }

        protected abstract SaslClient createSaslClient();

        protected abstract Object getMechanism();

        protected abstract String getUserNameForMechanism();

        protected abstract DB getDatabase();

        private CommandResult sendSaslStart(final byte[] outToken) throws IOException {
            DBObject cmd = new BasicDBObject("saslStart", 1).append("mechanism", getMechanism()).append("payload",
                    outToken != null ? outToken : new byte[0]);
            return runCommand(getDatabase(), cmd);
        }

        private CommandResult sendSaslContinue(final int conversationId, final byte[] outToken) throws IOException {
            DB adminDB = getDatabase();
            DBObject cmd = new BasicDBObject("saslContinue", 1).append("conversationId", conversationId).
                    append("payload", outToken);
            return runCommand(adminDB, cmd);
        }

    }

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );
}
