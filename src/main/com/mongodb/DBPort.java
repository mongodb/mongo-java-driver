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

// DBPort.java

package com.mongodb;

import com.mongodb.util.Base64Codec;
import com.mongodb.util.ThreadUtil;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.bson.util.Assertions.isTrue;

/**
 * represents a Port to the database, which is effectively a single connection to a server
 * Methods implemented at the port level should throw the raw exceptions like IOException,
 * so that the connector above can make appropriate decisions on how to handle.
 *
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class DBPort implements Connection {
    
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
        this( addr , null , new MongoOptions(), 0 );
    }
    
    DBPort( ServerAddress addr, PooledConnectionProvider pool, MongoOptions options, int generation ) {
        _options = options;
        _sa = addr;
        _addr = addr;
        provider = pool;
        this.generation = generation;

        _logger = Logger.getLogger( _rootLogger.getName() + "." + addr.toString() );
        try {
            ensureOpen();
            _decoder = _options.dbDecoderFactory.create();
            openedAt = System.currentTimeMillis();
            lastUsedAt = openedAt;
        } catch (IOException e) {
            throw new MongoException.Network("Exception opening the socket", e);
        }
    }

    /**
     * Gets the generation of this connection.  This can be used by connection pools to track whether the connection is stale.
     *
     * @return the generation.
     */
    @Override
    public int getGeneration() {
        return generation;
    }

    /**
     * Returns the time at which this connection was opened, or {@code Long.MAX_VALUE} if it has not yet been opened.
     *
     * @return the time when this connection was opened, in milliseconds since the epoch.
     */
    @Override
    public long getOpenedAt() {
        return openedAt;
    }

    /**
     * Returns the time at which this connection was last used, or {@code Long.MAX_VALUE} if it has not yet been used.
     *
     * @return the time when this connection was last used, in milliseconds since the epoch.
     */
    @Override
    public long getLastUsedAt() {
        return lastUsedAt;
    }

    Response call( OutMessage msg , DBCollection coll ) throws IOException{
        isTrue("open", !closed);
        return call(msg, coll, null);
    }

    Response call(final OutMessage msg, final DBCollection coll, final DBDecoder decoder) throws IOException{
        isTrue("open", !closed);
        return doOperation(new Operation<Response>() {
            @Override
            public Response execute() throws IOException {
                setActiveState(new ActiveState(msg));
                msg.prepare();
                msg.pipe(_out);
                return new Response(_sa, coll, _in, (decoder == null ? _decoder : decoder));
            }
        });
    }
    
    void say( final OutMessage msg ) throws IOException {
        isTrue("open", !closed);
        doOperation(new Operation<Void>() {
            @Override
            public Void execute() throws IOException {
                setActiveState(new ActiveState(msg));
                msg.prepare();
                msg.pipe(_out);
                return null;
            }
        });
    }

    synchronized <T> T doOperation(Operation<T> operation) throws IOException {
        isTrue("open", !closed);
        usageCount++;

        try {
            return operation.execute();
        }
        catch ( IOException ioe ){
            close();
            throw ioe;
        }
        finally {
            lastUsedAt = System.currentTimeMillis();
            _activeState = null;
        }
    }

    void setActiveState(ActiveState activeState) {
        isTrue("open", !closed);
        _activeState = activeState;
    }

    synchronized CommandResult getLastError( DB db , WriteConcern concern ) throws IOException{
        isTrue("open", !closed);
        return runCommand(db, concern.getCommand() );
    }

    synchronized private Response findOne( DB db , String coll , DBObject q ) throws IOException {
        OutMessage msg = OutMessage.query( db.getCollection(coll) , 0 , 0 , -1 , q , null, Bytes.MAX_OBJECT_SIZE );
        try {
            return call(msg, db.getCollection(coll), null);
        } finally {
            msg.doneWithMessage();
        }
    }

    synchronized CommandResult runCommand( DB db , DBObject cmd ) throws IOException {
        isTrue("open", !closed);
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
        isTrue("open", !closed);
        if ( last != usageCount )
            return null;
        
        return getLastError(db, concern);
    }

    OutputStream getOutputStream() throws IOException {
        isTrue("open", !closed);
        return _out;
    }

    InputStream getInputStream() throws IOException {
        isTrue("open", !closed);
        return _in;
    }

    /**
     * makes sure that a connection to the server has been opened
     * @throws IOException
     */
    public synchronized void ensureOpen() throws IOException {

        if ( _socket != null )
            return;

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

                if (!_options.autoConnectRetry || (provider != null && !provider.hasWorked()))
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
    
    ActiveState getActiveState() {
        isTrue("open", !closed);
        return _activeState;
    }

    int getLocalPort() {
        isTrue("open", !closed);
        return _socket != null ? _socket.getLocalPort() : -1;
    }

    ServerAddress getAddress() {
        return _addr;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
    /**
     * closes the underlying connection and streams
     */
    @Override
    public void close(){
        closed = true;
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
        } else if (credentials.getMechanism().equals(MongoCredential.PLAIN_MECHANISM)) {
            authenticator = new PlainAuthenticator(mongo, credentials);
        } else if (credentials.getMechanism().equals(MongoCredential.MONGODB_X509_MECHANISM)) {
            authenticator = new X509Authenticator(mongo, credentials);
        } else if (credentials.getMechanism().equals(MongoCredential.SCRAM_SHA_1_MECHANISM)) {
            authenticator = new ScramSha1Authenticator(mongo, credentials);
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
        return null;
    }

    public long getUsageCount() {
        return usageCount;
    }

    PooledConnectionProvider getProvider() {
        return provider;
    }

    Set<String> getAuthenticatedDatabases() {
        return Collections.unmodifiableSet(authenticatedDatabases);
    }

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );

    private volatile boolean closed;
    private final long openedAt;
    private volatile long lastUsedAt;
    private final int generation;
    private final PooledConnectionProvider provider;

    private final ServerAddress _sa;
    private final ServerAddress _addr;
    private final MongoOptions _options;
    private final Logger _logger;
    private final DBDecoder _decoder;
    
    private volatile Socket _socket;
    private volatile InputStream _in;
    private volatile OutputStream _out;

    // needs synchronization to ensure that modifications are published.
    private final Set<String> authenticatedDatabases = Collections.synchronizedSet(new HashSet<String>());

    private volatile long usageCount;
    private volatile ActiveState _activeState;

    class ActiveState {
        ActiveState(final OutMessage outMessage) {
            namespace = outMessage.getNamespace();
            opCode = outMessage.getOpCode();
            query = outMessage.getQuery();
            numDocuments = outMessage.getNumDocuments();
            this.startTime = System.nanoTime();
            this.threadName = Thread.currentThread().getName();
        }

        String getNamespace() {
            return namespace;
        }

        OutMessage.OpCode getOpCode() {
            return opCode;
        }

        DBObject getQuery() {
            return query;
        }

        int getNumDocuments() {
            return numDocuments;
        }

        long getStartTime() {
            return startTime;
        }

        String getThreadName() {
            return threadName;
        }

        private final String namespace;
        private final OutMessage.OpCode opCode;
        private final DBObject query;
        private int numDocuments;
        private final long startTime;
        private final String threadName;
    }

    class PlainAuthenticator extends SaslAuthenticator {
        private static final String MECHANISM = MongoCredential.PLAIN_MECHANISM;
        private static final String DEFAULT_PROTOCOL = "mongodb";

        PlainAuthenticator(final Mongo mongo, final MongoCredential credentials) {
            super(mongo, credentials);
        }

        @Override
        protected SaslClient createSaslClient() {
            try {
                return Sasl.createSaslClient(new String[]{MongoCredential.PLAIN_MECHANISM}, credential.getUserName(),
                                             DEFAULT_PROTOCOL, serverAddress().getHost(), null, new CallbackHandler() {
                    @Override
                    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback callback : callbacks) {
                            if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword(credential.getPassword());
                            }
                            else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName(credential.getUserName());
                            }
                        }
                    }
                });
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            }
        }

        @Override
        public String getMechanismName() {
            return MECHANISM;
        }
    }

    class GSSAPIAuthenticator extends SaslAuthenticator {
        public static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
        public static final String GSSAPI_MECHANISM = MongoCredential.GSSAPI_MECHANISM;
        public static final String SERVICE_NAME_KEY = "SERVICE_NAME";
        public static final String SERVICE_NAME_DEFAULT_VALUE = "mongodb";
        public static final String CANONICALIZE_HOST_NAME_KEY = "CANONICALIZE_HOST_NAME";

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

                return Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, credential.getUserName(),
                                             credential.getMechanismProperty(SERVICE_NAME_KEY, SERVICE_NAME_DEFAULT_VALUE),
                                             getHostName(), props, null);
            } catch (SaslException e) {
                throw new MongoException("Exception initializing SASL client", e);
            } catch (GSSException e) {
                throw new MongoException("Exception initializing GSSAPI credentials", e);
            } catch (UnknownHostException e) {
                throw new MongoException("Unknown host " + serverAddress().getHost(), e);
            }
        }

        @Override
        public String getMechanismName() {
            return "GSSAPI";
        }

        private String getHostName() throws UnknownHostException {
            return credential.getMechanismProperty(CANONICALIZE_HOST_NAME_KEY, false)
                   ? InetAddress.getByName(serverAddress().getHost()).getCanonicalHostName()
                   : serverAddress().getHost();
        }

        private GSSCredential getGSSCredential(String userName) throws GSSException {
            Oid krb5Mechanism = new Oid(GSSAPI_OID);
            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
            return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME,
                    krb5Mechanism, GSSCredential.INITIATE_ONLY);
        }
    }

    class ScramSha1Authenticator extends SaslAuthenticator {

        ScramSha1Authenticator(final Mongo mongo, final MongoCredential credential) {
            super(mongo, credential);

            if (!this.credential.getMechanism().equals(MongoCredential.SCRAM_SHA_1_MECHANISM)) {
                throw new MongoException("Incorrect mechanism: " + this.credential.getMechanism());
            }
        }

        @Override
        protected SaslClient createSaslClient() {
            return new ScramSha1SaslClient(this.credential);
        }

        @Override
        public String getMechanismName() {
            return MongoCredential.SCRAM_SHA_1_MECHANISM;
        }

        class ScramSha1SaslClient implements SaslClient {

            private static final String gs2Header = "n,,";
            private static final int randomLength = 24;

            private final Base64Codec base64Codec;
            private final MongoCredential credential;
            private String clientFirstMessageBare;
            private String rPrefix;
            private byte[] serverSignature;
            private int step;

            ScramSha1SaslClient(MongoCredential credential) {
                this.credential = credential;
                this.base64Codec = new Base64Codec();
            }

            public String getMechanismName() {
                return MongoCredential.SCRAM_SHA_1_MECHANISM;
            }

            public boolean hasInitialResponse() {
                return true;
            }

            public byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
                if(this.step == 0) {
                    this.step++;

                    return computeClientFirstMessage();
                }
                else if(this.step == 1) {
                    this.step++;

                    return computeClientFinalMessage(challenge);
                }
                else if(this.step == 2) {
                    this.step++;

                    String serverResponse = encodeUTF8(challenge);
                    HashMap<String, String> map = parseServerResponse(serverResponse);

                    if(!map.get("v").equals(encodeBase64(this.serverSignature))) {
                        throw new SaslException("Server signature was invalid.");
                    }

                    return challenge;
                }
                else {
                    throw new SaslException("Too many steps involved in the SCRAM-SHA-1 negotiation.");
                }
            }

            public boolean isComplete() {
                return this.step > 2;
            }

            public byte[] unwrap(final byte[] incoming, final int offset, final int len) throws SaslException {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            public byte[] wrap(final byte[] outgoing, final int offset, final int len) throws SaslException {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            public Object getNegotiatedProperty(final String propName) {
                throw new UnsupportedOperationException("Not implemented yet!");
            }

            public void dispose() throws SaslException {
                // nothing to do
            }

            private byte[] computeClientFirstMessage() throws SaslException {
                String userName = "n=" + prepUserName(this.credential.getUserName());
                this.rPrefix = generateRandomString();
                String nonce = "r=" + this.rPrefix;

                this.clientFirstMessageBare = userName + "," + nonce;
                String clientFirstMessage = gs2Header + this.clientFirstMessageBare;

                return decodeUTF8(clientFirstMessage);
            }

            private byte[] computeClientFinalMessage(final byte[] challenge) throws SaslException {
                String serverFirstMessage = encodeUTF8(challenge);

                HashMap<String, String> map = parseServerResponse(serverFirstMessage);
                String r = map.get("r");
                if(!r.startsWith(this.rPrefix)) {
                    throw new SaslException("Server sent an invalid nonce.");
                }

                String s = map.get("s");
                String i = map.get("i");

                String channelBinding = "c=" + encodeBase64(decodeUTF8(gs2Header));
                String nonce = "r=" + r;
                String clientFinalMessageWithoutProof = channelBinding + "," + nonce;

                byte[] saltedPassword = Hi(
                                          NativeAuthenticationHelper.createHash(this.credential.getUserName(),
                                                                                this.credential.getPassword()),
                                          decodeBase64(s),
                                          Integer.parseInt(i)
                                          );
                byte[] clientKey = HMAC(saltedPassword, "Client Key");
                byte[] storedKey = H(clientKey);
                String authMessage = this.clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
                byte[] clientSignature = HMAC(storedKey, authMessage);
                byte[] clientProof = XOR(clientKey, clientSignature);
                byte[] serverKey = HMAC(saltedPassword, "Server Key");
                this.serverSignature = HMAC(serverKey, authMessage);

                String proof = "p=" + encodeBase64(clientProof);
                String clientFinalMessage = clientFinalMessageWithoutProof + "," + proof;

                return decodeUTF8(clientFinalMessage);
            }

            private byte[] decodeBase64(String str) {
                return this.base64Codec.decode(str);
            }

            private byte[] decodeUTF8(String str) throws SaslException {
                try {
                    return str.getBytes("UTF-8");
                }
                catch (UnsupportedEncodingException e) {
                    throw new SaslException("UTF-8 is not a supported encoding.", e);
                }
            }

            private String encodeBase64(byte[] bytes) {
                return this.base64Codec.encode(bytes);
            }

            private String encodeUTF8(byte[] bytes) throws SaslException {
                try {
                    return new String(bytes, "UTF-8");
                }
                catch (UnsupportedEncodingException e) {
                    throw new SaslException("UTF-8 is not a supported encoding.", e);
                }
            }

            private String generateRandomString() {
                final int comma = 44;
                final int low = 33;
                final int high = 126;
                final int range = high - low;

                Random random = new SecureRandom();
                char[] text = new char[randomLength];
                for (int i = 0; i < randomLength; i++)
                {
                    int next = random.nextInt(range) + low;
                    while(next == comma) {
                        next = random.nextInt(range) + low;
                    }
                    text[i] = (char)next;
                }
                return new String(text);
            }

            private byte[] H(byte[] data) throws SaslException {
                try {
                    return MessageDigest.getInstance("SHA-1").digest(data);
                }
                catch (NoSuchAlgorithmException e) {
                    throw new SaslException("SHA-1 could not be found.", e);
                }
            }

            private byte[] Hi(byte[] password, byte[] salt, int iterations) throws SaslException {
                PBEKeySpec spec = new PBEKeySpec(encodeUTF8(password).toCharArray(), salt, iterations, 20 * 8);

                SecretKeyFactory keyFactory;
                try {
                    keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
                }
                catch (NoSuchAlgorithmException e) {
                    throw new SaslException("Unable to find PBKDF2WithHmacSHA1.", e);
                }

                try {
                    return keyFactory.generateSecret(spec).getEncoded();
                }
                catch (InvalidKeySpecException e) {
                    throw new SaslException("Invalid key spec for PBKDC2WithHmacSHA1.", e);
                }
            }

            private byte[] HMAC(byte[] bytes, String key) throws SaslException {
                SecretKeySpec signingKey = new SecretKeySpec(bytes, "HmacSHA1");

                Mac mac;
                try {
                    mac = Mac.getInstance("HmacSHA1");
                }
                catch (NoSuchAlgorithmException e) {
                    throw new SaslException("Could not find HmacSHA1.", e);
                }

                try {
                    mac.init(signingKey);
                }
                catch (InvalidKeyException e) {
                    throw new SaslException("Could not initialize mac.", e);
                }

                return mac.doFinal(decodeUTF8(key));
            }

            private HashMap<String, String> parseServerResponse(String response) {
                HashMap<String, String> map = new HashMap<String, String>();
                String[] pairs = response.split(",");
                for(String pair : pairs) {
                    String[] parts = pair.split("=", 2);
                    map.put(parts[0], parts[1]);
                }

                return map;
            }

            private String prepUserName(String userName) {
                return userName.replace("=", "=3D").replace(",","=2D");
            }

            private byte[] XOR(byte[] a, byte[] b) {
                byte[] result = new byte[a.length];

                for(int i = 0; i < a.length; i++) {
                    result[i] = (byte)(a[i] ^ b[i]);
                }

                return result;
            }
        }
    }

    abstract class SaslAuthenticator extends Authenticator {

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

        protected DB getDatabase() {
            return mongo.getDB(credential.getSource());
        }

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

    class X509Authenticator extends Authenticator {
        X509Authenticator(final Mongo mongo, final MongoCredential credential) {
            super(mongo, credential);
        }

        @Override
        CommandResult authenticate() {
            try {
                DB db = mongo.getDB(credential.getSource());
                CommandResult res = runCommand(db, getAuthCommand());
                res.throwOnError();
                return res;
            } catch (IOException e) {
                throw new MongoException.Network("IOException authenticating the connection", e);
            }
        }

        private DBObject getAuthCommand() {
            return new BasicDBObject("authenticate", 1)
                   .append("user", credential.getUserName())
                   .append("mechanism", MongoCredential.MONGODB_X509_MECHANISM);
        }
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

    interface Operation<T> {
        T execute() throws IOException;
    }
}
