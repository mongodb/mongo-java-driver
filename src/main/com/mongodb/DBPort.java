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

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private boolean globallyAuthed = false;

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

        _calls++;

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
        if ( last != _calls )
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
    
    void checkAuth(DB db, final boolean writePrivilegesRequired) throws IOException {
        // TODO: add support for retry when credentials ticket times out
        if (db.getMongo().getCredentials() != null) {
            if (!globallyAuthed) {
                if (!db.getMongo().getCredentials().getMechanism().equals(MongoClientCredentials.GSSAPI_MECHANISM)) {
                    throw new MongoException("Unsupported authentication mechanism: " + db.getMongo().getCredentials().getMechanism());
                }
                new GSSAPIAuthenticator(this, db.getMongo()).authenticate();
                globallyAuthed = true;
            }
            saslAquireDatabasePrivileges(db, writePrivilegesRequired);
        }
        else {
            DB.AuthenticationCredentials credentials = db.getAuthenticationCredentials();
            if (credentials == null) {
                if (db._name.equals("admin"))
                    return;
                checkAuth(db._mongo.getDB("admin"), writePrivilegesRequired);
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

    private void saslAquireDatabasePrivileges(final DB db, final boolean writePrivilegesRequired) throws IOException {
        BasicDBObject acquirePrivilegeCmd = new BasicDBObject("acquirePrivilege", 1).
                append("principal", db.getMongo().getCredentials().getUserName()).append("resource", db.getName());
        if (writePrivilegesRequired) {
            if (_saslWriteAuthed.get(db) == null) {
                acquirePrivilegeCmd.append("actions", Arrays.asList("oldWrite"));
                CommandResult res = runCommand(db.getSisterDB("admin"), acquirePrivilegeCmd);
                res.throwOnError();
                _saslWriteAuthed.put(db, true);
                _saslReadAuthed.put(db, true);
            }
        } else {
            if (_saslReadAuthed.get(db) == null) {
                acquirePrivilegeCmd.append("actions", Arrays.asList("oldRead"));
                CommandResult res = runCommand(db.getSisterDB("admin"), acquirePrivilegeCmd);
                res.throwOnError();
                _saslReadAuthed.put(db, true);
            }
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
    
    private Socket _socket;
    private InputStream _in;
    private OutputStream _out;

    private boolean _processingResponse;

    private Map<DB,Boolean> _authed = new ConcurrentHashMap<DB, Boolean>( );
    private Map<DB,Boolean> _saslReadAuthed = new ConcurrentHashMap<DB, Boolean>( );
    private Map<DB,Boolean> _saslWriteAuthed = new ConcurrentHashMap<DB, Boolean>( );
    int _lastThread;
    long _calls = 0;
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

    //TODO: add password support
    static class GSSAPIAuthenticator extends SaslAuthenticator {
        public static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
        public static final String GSSAPI_MECHANISM = "GSSAPI";
        public static final String MONGODB_PROTOCOL = "mongodb";

        GSSAPIAuthenticator(DBPort port, final Mongo mongo) {
            super(port, mongo);
            try {
                Map<String, Object> props = new HashMap<String, Object>();
                // NOTE: I couldn't find this part documented anywhere
                props.put(Sasl.CREDENTIALS, getGSSCredential(mongo.getCredentials().getUserName()));

                saslClient = Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, mongo.getCredentials().getUserName(), MONGODB_PROTOCOL,
                        port.serverAddress().getHost(), props, null);
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            } catch (GSSException e) {
                throw new MongoException("Exception initializing GSSAPI credentials", e);
            }
        }

        private GSSCredential getGSSCredential(String userName) throws GSSException {
            Oid krb5Mechanism = new Oid(GSSAPI_OID);
            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
            return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME,
                    krb5Mechanism, GSSCredential.INITIATE_ONLY);
        }
    }

    static class SaslAuthenticator {
        final DBPort port;
        final Mongo mongo;
        Integer conversationId;
        SaslClient saslClient;

        SaslAuthenticator(final DBPort port, final Mongo mongo) {
            this.port = port;
            this.mongo = mongo;
        }

        void authenticate() throws SaslException {
            try {
            // Get optional initial response
            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            CommandResult res = sendSaslStart(response);
            res.throwOnError();

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
                    res = sendSaslContinue(response);
                    res.throwOnError();
                }
            }
            } catch (IOException e) {
                // TODO: Fix exception message.
                throw new MongoException("", e);
            }
        }

        private CommandResult sendSaslStart(final byte[] outToken) throws IOException {
//            System.out.println("Sending saslStart with num bytes: " + outToken.length);
            DB adminDB = mongo.getDB("admin");
            DBObject cmd = new BasicDBObject("saslStart", 1).append("mechanism", "GSSAPI").append("payload", outToken);
            CommandResult res = port.runCommand(adminDB, cmd);
            conversationId = (Integer) res.get("conversationId");
            return res;
        }

        private CommandResult sendSaslContinue(final byte[] outToken) throws IOException {
//            System.out.println("Sending saslContinue with num bytes: " + outToken.length);
            DB adminDB = mongo.getDB("admin");
            DBObject cmd = new BasicDBObject("saslContinue", 1).append("conversationId", conversationId).
                    append("payload", outToken);
            return port.runCommand(adminDB, cmd);
        }
    }

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );
}
