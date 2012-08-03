// DB.java

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

import com.mongodb.DBApiLayer.Result;
import com.mongodb.util.Util;
import org.bson.BSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * an abstract class that represents a logical database on a server
 * @dochub databases
 */
public abstract class DB {
    
    private static final Set<String> _obedientCommands = new HashSet<String>();
    
    static {
        _obedientCommands.add("group");
        _obedientCommands.add("aggregate");
        _obedientCommands.add("collStats");
        _obedientCommands.add("dbStats");
        _obedientCommands.add("count");
        _obedientCommands.add("distinct");
        _obedientCommands.add("geoNear");
        _obedientCommands.add("geoSearch");
        _obedientCommands.add("geoWalk");
    }

    /**
     * @param mongo the mongo instance
     * @param name the database name
     */
    public DB( Mongo mongo , String name ){
        _mongo = mongo;
    	_name = name;
        _options = new Bytes.OptionHolder( _mongo._netOptions );
    }

    /**
     * Tests if database commands are read preference obedient
     * @param command the <code>DBObject</code> to test obedience
     * @return true if the command is obedient
     * @see com.mongodb.ReadPreference
     */
    ReadPreference getCommandReadPreference(DBObject command, ReadPreference requestedPreference){
        String comString = command.keySet().iterator().next();

        if (comString.equals("getnonce") || comString.equals("authenticate")) {
            return ReadPreference.primaryPreferred();
        }

        boolean primaryRequired;

        // explicitly check mapreduce commands are inline
        if(comString.equals("mapreduce")) {
            Object out = command.get("out");
            if (out instanceof BSONObject ){
                BSONObject outMap = (BSONObject) out;
                primaryRequired = outMap.get("inline") == null;
            }
            else
                primaryRequired = true;
        } else {
           primaryRequired =  !_obedientCommands.contains(comString);
        }

        if (primaryRequired) {
            return ReadPreference.primary();
        } else {
            return requestedPreference;
        }
    }

    /**
     * starts a new "consistent request".
     * Following this call and until requestDone() is called, all db operations should use the same underlying connection.
     * This is useful to ensure that operations happen in a certain order with predictable results.
     */
    public abstract void requestStart();

    /**
     * ends the current "consistent request"
     */
    public abstract void requestDone();

    /**
     * ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a replica set)
     */
    public abstract void requestEnsureConnection();

    /**
     * Returns the collection represented by the string &lt;dbName&gt;.&lt;collectionName&gt;.
     * @param name the name of the collection
     * @return the collection
     */
    protected abstract DBCollection doGetCollection( String name );

    /**
     * Gets a collection with a given name.
     * If the collection does not exist, a new collection is created.
     * @param name the name of the collection to return
     * @return the collection
     */
    public DBCollection getCollection( String name ){
        DBCollection c = doGetCollection( name );
        return c;
    }

    /**
     * Creates a collection with a given name and options.
     * If the collection does not exist, a new collection is created.
     * Note that if the options parameter is null, the creation will be deferred to when the collection is written to.
     * Possible options:
     * <dl>
     * <dt>capped</dt><dd><i>boolean</i>: if the collection is capped</dd>
     * <dt>size</dt><dd><i>int</i>: collection size (in bytes)</dd>
     * <dt>max</dt><dd><i>int</i>: max number of documents</dd>
     * </dl>
     * @param name the name of the collection to return
     * @param options options
     * @return the collection
     * @throws MongoException
     */
    public DBCollection createCollection( String name, DBObject options ){
        if ( options != null ){
            DBObject createCmd = new BasicDBObject("create", name);
            createCmd.putAll(options);
            CommandResult result = command(createCmd);
            result.throwOnError();
        }
        return getCollection(name);
    }


    /**
     * Returns a collection matching a given string.
     * @param s the name of the collection
     * @return the collection
     */
    public DBCollection getCollectionFromString( String s ){
        DBCollection foo = null;

        int idx = s.indexOf( "." );
        while ( idx >= 0 ){
            String b = s.substring( 0 , idx );
            s = s.substring( idx + 1 );
            if ( foo == null )
                foo = getCollection( b );
            else
                foo = foo.getCollection( b );
            idx = s.indexOf( "." );
        }

        if ( foo != null )
            return foo.getCollection( s );
        return getCollection( s );
    }

    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int) } with 0 as query option.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd ){
        return command( cmd, 0 );
    }


    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.DBEncoder) } with 0 as query option.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param encoder 
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd, DBEncoder encoder ){
        return command( cmd, 0, encoder );
    }

    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with a null readPrefs.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param options query options to use
     * @param encoder 
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, DBEncoder encoder ){
        return command(cmd, options, getReadPreference(), encoder);
    }

    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with a default encoder.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param options query options to use
     * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs ){
        return command(cmd, options, readPrefs, DefaultDBEncoder.FACTORY.create());
    }

    /**
     * Executes a database command.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param options query options to use
     * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
     * @param encoder
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs, DBEncoder encoder ){

        readPrefs = getCommandReadPreference(cmd, readPrefs);
        
        Iterator<DBObject> i =
                getCollection("$cmd").__find(cmd, new BasicDBObject(), 0, -1, 0, options, readPrefs ,
                        DefaultDBDecoder.FACTORY.create(), encoder);
        if ( i == null || ! i.hasNext() )
            return null;

        DBObject res = i.next();
        ServerAddress sa = (i instanceof Result) ? ((Result) i).getServerAddress() : null;
        CommandResult cr = new CommandResult(cmd, sa);
        cr.putAll( res );
        return cr;
    }

    /**
     * Executes a database command.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd dbobject representing the command to execute
     * @param options query options to use
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options ){
    	return command(cmd, options, getReadPreference());
    }
    
    /**
     * Executes a database command.
     * This method constructs a simple dbobject and calls {@link DB#command(com.mongodb.DBObject) }
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd command to execute
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( String cmd ){
        return command( new BasicDBObject( cmd , Boolean.TRUE ) );
    }

    /**
     * Executes a database command.
     * This method constructs a simple dbobject and calls {@link DB#command(com.mongodb.DBObject, int)  }
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @param cmd command to execute
     * @param options query options to use
     * @return result of command from the database
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( String cmd, int options  ){
        return command( new BasicDBObject( cmd , Boolean.TRUE ), options );
    }

    /**
     * evaluates a function on the database.
     * This is useful if you need to touch a lot of data lightly, in which case network transfer could be a bottleneck.
     * @param code the function in javascript code
     * @param args arguments to be passed to the function
     * @return The command result
     * @throws MongoException
     */
    public CommandResult doEval( String code , Object ... args ){

        return command( BasicDBObjectBuilder.start()
                        .add( "$eval" , code )
                        .add( "args" , args )
                        .get() );
    }

    /**
     * calls {@link DB#doEval(java.lang.String, java.lang.Object[]) }.
     * If the command is successful, the "retval" field is extracted and returned.
     * Otherwise an exception is thrown.
     * @param code the function in javascript code
     * @param args arguments to be passed to the function
     * @return The object
     * @throws MongoException
     */
    public Object eval( String code , Object ... args ){

        CommandResult res = doEval( code , args );
        res.throwOnError();
        return res.get( "retval" );
    }

    /**
     * Returns the result of "dbstats" command
     * @return
     * @throws MongoException
     */
    public CommandResult getStats() {
        return command("dbstats");
    }

    /**
     * Returns the name of this database.
     * @return the name
     */
    public String getName(){
	return _name;
    }

    /**
     * Makes this database read-only.
     * Important note: this is a convenience setting that is only known on the client side and not persisted.
     * @param b if the database should be read-only
     */
    public void setReadOnly( Boolean b ){
        _readOnly = b;
    }

    /**
     * Returns a set containing the names of all collections in this database.
     * @return the names of collections in this database
     * @throws MongoException
     */
    public Set<String> getCollectionNames(){

        DBCollection namespaces = getCollection("system.namespaces");
        if (namespaces == null)
            throw new RuntimeException("this is impossible");

        Iterator<DBObject> i = namespaces.__find(new BasicDBObject(), null, 0, 0, 0, getOptions(), getReadPreference(), null);
        if (i == null)
            return new HashSet<String>();

        List<String> tables = new ArrayList<String>();

        for (; i.hasNext();) {
            DBObject o = i.next();
            if ( o.get( "name" ) == null ){
                throw new MongoException( "how is name null : " + o );
            }
            String n = o.get("name").toString();
            int idx = n.indexOf(".");

            String root = n.substring(0, idx);
            if (!root.equals(_name))
                continue;

            if (n.indexOf("$") >= 0)
                continue;

            String table = n.substring(idx + 1);

            tables.add(table);
        }

        Collections.sort(tables);

        return new LinkedHashSet<String>(tables);
    }

    /**
     * Checks to see if a collection by name %lt;name&gt; exists.
     * @param collectionName The collection to test for existence
     * @return false if no collection by that name exists, true if a match to an existing collection was found
     * @throws MongoException
     */
    public boolean collectionExists(String collectionName)
    {
        if (collectionName == null || "".equals(collectionName))
            return false;

        Set<String> collections = getCollectionNames();
        if (collections.isEmpty())
            return false;

        for (String collection : collections)
        {
            if (collectionName.equalsIgnoreCase(collection))
                return true;
        }

        return false;
    }


    /**
     * Returns the name of this database.
     * @return the name
     */
    @Override
    public String toString(){
        return _name;
    }

    /**
     * Gets the the error (if there is one) from the previous operation on this connection.
     * The result of this command will look like
     *
     * <pre>
     * { "err" :  errorMessage  , "ok" : 1.0 }
     * </pre>
     *
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     *
     * Important note: when calling this method directly, it is undefined which connection "getLastError" is called on.
     * You may need to explicitly use a "consistent Request", see {@link DB#requestStart()}
     * For most purposes it is better not to call this method directly but instead use {@link WriteConcern}
     *
     * @return DBObject with error and status information
     * @throws MongoException
     */
    public CommandResult getLastError(){
        return command(new BasicDBObject("getlasterror", 1));
    }

    /**
     * @see {@link DB#getLastError() }
     * @param concern the concern associated with "getLastError" call
     * @return
     * @throws MongoException
     */
    public CommandResult getLastError( com.mongodb.WriteConcern concern ){
        return command( concern.getCommand() );
    }

    /**
     * @see {@link DB#getLastError(com.mongodb.WriteConcern) }
     * @param w
     * @param wtimeout
     * @param fsync
     * @return The command result
     * @throws MongoException
     */
    public CommandResult getLastError( int w , int wtimeout , boolean fsync ){
        return command( (new com.mongodb.WriteConcern( w, wtimeout , fsync )).getCommand() );
    }


    /**
     * Sets the write concern for this database. It Will be used for
     * writes to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     * @param concern write concern to use
     */
    public void setWriteConcern( com.mongodb.WriteConcern concern ){
        if (concern == null) throw new IllegalArgumentException();
        _concern = concern;
    }

    /**
     * Gets the write concern for this database.
     * @return
     */
    public com.mongodb.WriteConcern getWriteConcern(){
        if ( _concern != null )
            return _concern;
        return _mongo.getWriteConcern();
    }

    /**
     * Sets the read preference for this database. Will be used as default for
     * reads from any collection in this database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference( ReadPreference preference ){
        _readPref = preference;
    }

    /**
     * Gets the default read preference
     * @return
     */
    public ReadPreference getReadPreference(){
        if ( _readPref != null )
            return _readPref;
        return _mongo.getReadPreference();
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     * @throws MongoException
     */
    public void dropDatabase(){

        CommandResult res = command(new BasicDBObject("dropDatabase", 1));
        res.throwOnError();
        _mongo._dbs.remove(this.getName());
    }

    /**
     * Returns true if a user has been authenticated
     *
     * @return true if authenticated, false otherwise
     * @dochub authenticate
     */
    public boolean isAuthenticated() {
        return authenticationCredentialsReference.get() != null;
    }

    /**
     *  Authenticates to db with the given name and password
     *
     * @param username name of user for this database
     * @param password password of user for this database
     * @return true if authenticated, false otherwise
     * @throws MongoException
     * @dochub authenticate
     */
    public boolean authenticate(String username, char[] password ){

        if (authenticationCredentialsReference.get() != null) {
            throw new IllegalStateException("can't authenticate twice on the same database");
        }

        AuthenticationCredentials newCredentials = new AuthenticationCredentials(username, password);
        CommandResult res = newCredentials.authenticate();
        if (!res.ok())
            return false;

        boolean wasNull = authenticationCredentialsReference.compareAndSet(null, newCredentials);
        if (!wasNull) {
            throw new IllegalStateException("can't authenticate twice on the same database");
        }
        return true;
    }

    /**
     *  Authenticates to db with the given name and password
     *
     * @param username name of user for this database
     * @param password password of user for this database
     * @return the CommandResult from authenticate command
     * @throws MongoException if authentication failed due to invalid user/pass, or other exceptions like I/O
     * @dochub authenticate
     */
    public synchronized CommandResult authenticateCommand(String username, char[] password ){

        if (authenticationCredentialsReference.get() != null) {
            throw new IllegalStateException( "can't authenticate twice on the same database" );
        }

        AuthenticationCredentials newCredentials = new AuthenticationCredentials(username, password);
        CommandResult res = newCredentials.authenticate();
        res.throwOnError();
        boolean wasNull = authenticationCredentialsReference.compareAndSet(null, newCredentials);
        if (!wasNull) {
            throw new IllegalStateException("can't authenticate twice on the same database");
        }
        return res;
    }

    /**
     * Adds a new user for this db
     * @param username
     * @param passwd
     * @throws MongoException
     */
    public WriteResult addUser( String username , char[] passwd ){
        return addUser(username, passwd, false);
    }

    /**
     * Adds a new user for this db
     * @param username
     * @param passwd
     * @param readOnly if true, user will only be able to read
     * @throws MongoException
     */
    public WriteResult addUser( String username , char[] passwd, boolean readOnly ){
        DBCollection c = getCollection( "system.users" );
        DBObject o = c.findOne( new BasicDBObject( "user" , username ) );
        if ( o == null )
            o = new BasicDBObject( "user" , username );
        o.put( "pwd" , _hash( username , passwd ) );
        o.put( "readOnly" , readOnly );
        return c.save( o );
    }

    /**
     * Removes a user for this db
     * @param username
     * @throws MongoException
     */
    public WriteResult removeUser( String username ){
        DBCollection c = getCollection( "system.users" );
        return c.remove(new BasicDBObject( "user" , username ));
    }

    String _hash( String username , char[] passwd ){
        ByteArrayOutputStream bout = new ByteArrayOutputStream( username.length() + 20 + passwd.length );
        try {
            bout.write( username.getBytes() );
            bout.write( ":mongo:".getBytes() );
            for ( int i=0; i<passwd.length; i++ ){
                if ( passwd[i] >= 128 )
                    throw new IllegalArgumentException( "can't handle non-ascii passwords yet" );
                bout.write( (byte)passwd[i] );
            }
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "impossible" , ioe );
        }
        return Util.hexMD5( bout.toByteArray() );
    }

    /**
     *  Returns the last error that occurred since start of database or a call to <code>resetError()</code>
     *
     *  The return object will look like
     *
     *  <pre>
     * { err : errorMessage, nPrev : countOpsBack, ok : 1 }
     *  </pre>
     *
     * The value for errorMessage will be null of no error has occurred, otherwise the error message.
     * The value of countOpsBack will be the number of operations since the error occurred.
     *
     * Care must be taken to ensure that calls to getPreviousError go to the same connection as that
     * of the previous operation.
     * See {@link DB#requestStart()} for more information.
     *
     * @return DBObject with error and status information
     * @throws MongoException
     */
    public CommandResult getPreviousError(){
        return command(new BasicDBObject("getpreverror", 1));
    }

    /**
     * Resets the error memory for this database.
     * Used to clear all errors such that {@link DB#getPreviousError()} will return no error.
     * @throws MongoException
     */
    public void resetError(){
        command(new BasicDBObject("reseterror", 1));
    }

    /**
     * For testing purposes only - this method forces an error to help test error handling
     * @throws MongoException
     */
    public void forceError(){
        command(new BasicDBObject("forceerror", 1));
    }

    /**
     * Gets the Mongo instance
     * @return
     */
    public Mongo getMongo(){
        return _mongo;
    }

    /**
     * Gets another database on same server
     * @param name name of the database
     * @return
     */
    public DB getSisterDB( String name ){
        return _mongo.getDB( name );
    }

    /**
     * Makes it possible to execute "read" queries on a slave node
     *
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     * @see ReadPreference#secondaryPreferred()
     */
    @Deprecated
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * Adds the give option
     * @param option
     */
    public void addOption( int option ){
        _options.add( option );
    }

    /**
     * Sets the query options
     * @param options
     */
    public void setOptions( int options ){
        _options.set( options );
    }

    /**
     * Resets the query options
     */
    public void resetOptions(){
        _options.reset();
    }

    /**
     * Gets the query options
     * @return
     */
    public int getOptions(){
        return _options.get();
    }

    public abstract void cleanCursors( boolean force );

    AuthenticationCredentials getAuthenticationCredentials() {
        return authenticationCredentialsReference.get();
    }

    final Mongo _mongo;
    final String _name;

    protected boolean _readOnly = false;
    private com.mongodb.WriteConcern _concern;
    private com.mongodb.ReadPreference _readPref;
    final Bytes.OptionHolder _options;

    private AtomicReference<AuthenticationCredentials> authenticationCredentialsReference =
            new AtomicReference<AuthenticationCredentials>();

    /**
     * Encapsulate everything relating to authorization of a user on a database
     */
    class AuthenticationCredentials {
        private final String userName;
        private final byte[] authHash;

        private AuthenticationCredentials(final String userName, final char[] password) {
            if (userName == null) {
                throw new IllegalArgumentException("userName can not be null");
            }
            if (password == null) {
                throw new IllegalArgumentException("password can not be null");
            }
            this.userName = userName;
            this.authHash = createHash(userName, password);
        }

        CommandResult authenticate() {
            requestStart();
            try {
               CommandResult res = command(getNonceCommand());
               res.throwOnError();

               return command(getAuthCommand(res.getString("nonce")));
            } finally {
                requestDone();
            }
        }

        DBObject getAuthCommand( String nonce ){
            String key = nonce + userName + new String( authHash );

            BasicDBObject cmd = new BasicDBObject();

            cmd.put("authenticate", 1);
            cmd.put("user", userName);
            cmd.put("nonce", nonce);
            cmd.put("key", Util.hexMD5(key.getBytes()));

            return cmd;
        }

        BasicDBObject getNonceCommand() {
            return new BasicDBObject("getnonce", 1);
        }

        private byte[] createHash( String userName , char[] password ){
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
            return Util.hexMD5( bout.toByteArray() ).getBytes();
        }
    }
}
