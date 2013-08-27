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

/**
 * An abstract class that represents a logical database on a server.
 * Thread-safe.
 * <p/>
 * DB instance can be achieved from {@link MongoClient} using code like:<br>
 * <code>
 * Mongo m = new Mongo();<br>
 * DB db = m.getDB("mydb");
 * </code>
 *
 * @dochub databases
 */
public abstract class DB {

    private static final Set<String> _obedientCommands = new HashSet<String>();

    static {
        _obedientCommands.add("group");
        _obedientCommands.add("aggregate");
        _obedientCommands.add("collstats");
        _obedientCommands.add("dbstats");
        _obedientCommands.add("count");
        _obedientCommands.add("distinct");
        _obedientCommands.add("geonear");
        _obedientCommands.add("geosearch");
        _obedientCommands.add("geowalk");
        _obedientCommands.add("text");
    }

    /**
     * Constructs a new instance of the {@code DB}.
     *
     * @param mongo the mongo instance
     * @param name  the database name
     */
    public DB( Mongo mongo , String name ){
        _mongo = mongo;
    	_name = name;
        _options = new Bytes.OptionHolder( _mongo._netOptions );
    }

    /**
     * Determines the read preference that should be used for the given command.
     *
     * @param command             the {@link DBObject} representing the command
     * @param requestedPreference the preference requested by the client.
     * @return the read preference to use for the given command.  It will never return {@code null}.
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
           primaryRequired =  !_obedientCommands.contains(comString.toLowerCase());
        }

        if (primaryRequired) {
            return ReadPreference.primary();
        } else if (requestedPreference == null) {
            return ReadPreference.primary();
        } else {
            return requestedPreference;
        }
    }

    /**
     * Starts a new 'consistent request'.
     * <p/>
     * Following this call and until {@link com.mongodb.DB#requestDone()} is called,
     * all db operations will use the same underlying connection.
     * <p/>
     * This is useful to ensure that operations happen in a certain order with predictable results.
     */
    public abstract void requestStart();

    /**
     * Ends the current 'consistent request'.
     */
    public abstract void requestDone();

    /**
     * Ensure that a connection is assigned to the current 'consistent request'
     * (from primary pool, if connected to a replica set)
     */
    public abstract void requestEnsureConnection();

    /**
     * Gets a collection with a given name.
     * If the collection does not exist, a new collection is created.
     * <p/>
     * This class is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.
     *
     * @param name the name of the collection
     * @return the collection
     */
    protected abstract DBCollection doGetCollection( String name );

    /**
     * Gets a collection with a given name.
     * If the collection does not exist, a new collection is created.
     *
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
     * <p/>
     * Possible options:
     * <ul>
     * <li>
     * <b>capped</b> ({@code boolean}) - Enables a collection cap.
     * False by default. If enabled, you must specify a size parameter.
     * </li>
     * <li>
     * <b>size</b> ({@code int}) - If capped is true, size specifies a maximum size in bytes for the capped collection.
     * When capped is false, you may use size to preallocate space.
     * </li>
     * <li>
     * <b>max</b> ({@code int}) -   Optional. Specifies a maximum "cap" in number of documents for capped collections.
     * You must also specify size when specifying max.
     * </li>
     * <p/>
     * </ul>
     * <p/>
     * Note that if the {@code options} parameter is {@code null},
     * the creation will be deferred to when the collection is written to.
     *
     * @param name    the name of the collection to return
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
     *
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
     * This method calls {@link DB#command(DBObject, int)} } with 0 as query option.
     *
     * @param cmd {@code DBObject} representation of the command to be executed
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd ){
        return command( cmd, 0 );
    }


    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.DBEncoder) } with 0 as query option.
     *
     * @param cmd     {@code DBObject} representation of the command to be executed
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd, DBEncoder encoder ){
        return command( cmd, 0, encoder );
    }

    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with a null readPrefs.
     *
     * @param cmd     {@code DBObject} representation the command to be executed
     * @param options query options to use
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, DBEncoder encoder ){
        return command(cmd, options, getReadPreference(), encoder);
    }

    /**
     * Executes a database command.
     * This method calls {@link DB#command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with a default encoder.
     *
     * @param cmd       {@code DBObject} representation the command to be executed
     * @param options   query options to use
     * @param readPrefs {@link ReadPreference} for this command (nodes selection is the biggest part of this)
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs ){
        return command(cmd, options, readPrefs, DefaultDBEncoder.FACTORY.create());
    }

    /**
     * Executes a database command.
     *
     * @param cmd       {@code DBObject} representation the command to be executed
     * @param options   query options to use
     * @param readPrefs {@link ReadPreference} for this command (nodes selection is the biggest part of this)
     * @param encoder   {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs, DBEncoder encoder ){
        readPrefs = getCommandReadPreference(cmd, readPrefs);
        cmd = wrapCommand(cmd, readPrefs);

        Iterator<DBObject> i =
                getCollection("$cmd").__find(cmd, new BasicDBObject(), 0, -1, 0, options, readPrefs ,
                        DefaultDBDecoder.FACTORY.create(), encoder);
        if ( i == null || ! i.hasNext() )
            return null;

        DBObject res = i.next();
        ServerAddress sa = (i instanceof Result) ? ((Result) i).getServerAddress() : null;
        CommandResult cr = new CommandResult(sa);
        cr.putAll( res );
        return cr;
    }

    // Only append $readPreference meta-operator if connected to a mongos, read preference is not primary
    // or secondary preferred,
    // and command is an instance of BasicDBObject.  The last condition is unfortunate, but necessary in case
    // the encoder is not capable of encoding a BasicDBObject
    // Due to issues with compatibility between different versions of mongos, also wrap the command in a
    // $query field, so that the $readPreference is not rejected
    private DBObject wrapCommand(DBObject cmd, final ReadPreference readPrefs) {
        if (getMongo().isMongosConnection() &&
                !(ReadPreference.primary().equals(readPrefs) || ReadPreference.secondaryPreferred().equals(readPrefs)) &&
                cmd instanceof BasicDBObject) {
            cmd = new BasicDBObject("$query", cmd)
                    .append(QueryOpBuilder.READ_PREFERENCE_META_OPERATOR, readPrefs.toDBObject());
        }
        return cmd;
    }

    /**
     * Executes a database command.
     *
     * @param cmd     {@code DBObject} representation the command to be executed
     * @param options query options to use
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options ){
    	return command(cmd, options, getReadPreference());
    }

    /**
     * Executes a database command.
     * This method constructs a simple dbobject and calls {@link DB#command(com.mongodb.DBObject) }
     *
     * @param cmd name of the command to be executed
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( String cmd ){
        return command( new BasicDBObject( cmd , Boolean.TRUE ) );
    }

    /**
     * Executes a database command.
     * This method constructs a simple dbobject and calls {@link DB#command(com.mongodb.DBObject, int)  }
     *
     * @param cmd     name of the command to be executed
     * @param options query options to use
     * @return result of the command execution
     * @throws MongoException
     * @dochub commands
     */
    public CommandResult command( String cmd, int options  ){
        return command( new BasicDBObject( cmd , Boolean.TRUE ), options );
    }

    /**
     * Evaluates JavaScript functions on the database server.
     * This is useful if you need to touch a lot of data lightly, in which case network transfer could be a bottleneck.
     *
     * @param code @{code String} representation of JavaScript function
     * @param args arguments to pass to the JavaScript function
     * @return result of the command execution
     * @throws MongoException
     */
    public CommandResult doEval( String code , Object ... args ){

        return command( BasicDBObjectBuilder.start()
                        .add( "$eval" , code )
                        .add( "args" , args )
                        .get() );
    }

    /**
     * Calls {@link DB#doEval(java.lang.String, java.lang.Object[]) }.
     * If the command is successful, the "retval" field is extracted and returned.
     * Otherwise an exception is thrown.
     *
     * @param code @{code String} representation of JavaScript function
     * @param args arguments to pass to the JavaScript function
     * @return result of the execution
     * @throws MongoException
     */
    public Object eval( String code , Object ... args ){

        CommandResult res = doEval( code , args );
        res.throwOnError();
        return res.get( "retval" );
    }

    /**
     * Helper method for calling a 'dbStats' command.
     * It returns storage statistics for a given database.
     *
     * @return result of the execution
     * @throws MongoException
     */
    public CommandResult getStats() {
        return command("dbstats");
    }

    /**
     * Returns the name of this database.
     *
     * @return the name
     */
    public String getName(){
	return _name;
    }

    /**
     * Makes this database read-only.
     * Important note: this is a convenience setting that is only known on the client side and not persisted.
     *
     * @param b if the database should be read-only
     * @deprecated Avoid making database read-only via this method.
     *             Connect with a user credentials that has a read-only access to a server instead.
     */
    @Deprecated
    public void setReadOnly(Boolean b) {
        _readOnly = b;
    }

    /**
     * Returns a set containing all collections in the existing database.
     *
     * @return an set of names
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
     * Checks to see if a collection with a given name exists on a server.
     *
     * @param collectionName a name of the collection to test for existence
     * @return {@code false} if no collection by that name exists, {@code true} if a match to an existing collection was found
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
     *
     * @return the name
     */
    @Override
    public String toString(){
        return _name;
    }

    /**
     * Returns the error status of the last operation on the current connection.
     * <p/>
     * The result of this command will look like:
     * <p/>
     * <code>
     * { "err" :  errorMessage  , "ok" : 1.0 }
     * </code>
     * <p/>
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     * <p/>
     * Important note: when calling this method directly, it is undefined which connection "getLastError" is called on.
     * You may need to explicitly use a "consistent Request", see {@link DB#requestStart()}
     * For most purposes it is better not to call this method directly but instead use {@link WriteConcern}
     *
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     */
    public CommandResult getLastError(){
        return command(new BasicDBObject("getlasterror", 1));
    }

    /**
     * Returns the error status of the last operation on the current connection.
     *
     * @param concern a {@link WriteConcern} to be used while checking for the error status.
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @see {@link DB#getLastError() }
     */
    public CommandResult getLastError( com.mongodb.WriteConcern concern ){
        return command( concern.getCommand() );
    }

    /**
     * Returns the error status of the last operation on the current connection.
     *
     * @param w        when running with replication, this is the number of servers to replicate to before returning. A <b>w</b> value of <b>1</b> indicates the primary only. A <b>w</b> value of <b>2</b> includes the primary and at least one secondary, etc. In place of a number, you may also set <b>w</b> to majority to indicate that the command should wait until the latest write propagates to a majority of replica set members. If using <b>w</b>, you should also use <b>wtimeout</b>. Specifying a value for <b>w</b> without also providing a <b>wtimeout</b> may cause {@code getLastError} to block indefinitely.
     * @param wtimeout a value in milliseconds that controls how long to wait for write propagation to complete. If replication does not complete in the given timeframe, the getLastError command will return with an error status.
     * @param fsync    if <b>true</b>, wait for {@code mongod} to write this data to disk before returning. Defaults to <b>false</b>.
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @see {@link DB#getLastError(com.mongodb.WriteConcern) }
     */
    public CommandResult getLastError( int w , int wtimeout , boolean fsync ){
        return command( (new com.mongodb.WriteConcern( w, wtimeout , fsync )).getCommand() );
    }


    /**
     * Sets the write concern for this database. It will be used for
     * write operations to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param concern {@code WriteConcern} to use
     */
    public void setWriteConcern( com.mongodb.WriteConcern concern ){
        if (concern == null) throw new IllegalArgumentException();
        _concern = concern;
    }

    /**
     * Gets the write concern for this database.
     *
     * @return {@code WriteConcern} to be used for write operations, if not specified explicitly
     */
    public com.mongodb.WriteConcern getWriteConcern(){
        if ( _concern != null )
            return _concern;
        return _mongo.getWriteConcern();
    }

    /**
     * Sets the read preference for this database. Will be used as default for
     * read operations from any collection in this database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param preference {@code ReadPreference} to use
     */
    public void setReadPreference( ReadPreference preference ){
        _readPref = preference;
    }

    /**
     * Gets the read preference for this database.
     *
     * @return {@code ReadPreference} to be used for read operations, if not specified explicitly
     */
    public ReadPreference getReadPreference(){
        if ( _readPref != null )
            return _readPref;
        return _mongo.getReadPreference();
    }

    /**
     * Drops this database, deleting the associated data files. Use with caution.
     *
     * @throws MongoException
     */
    public void dropDatabase(){

        CommandResult res = command(new BasicDBObject("dropDatabase", 1));
        res.throwOnError();
        _mongo._dbs.remove(this.getName());
    }

    /**
     * Returns {@code true} if a user has been authenticated on this database.
     *
     * @return {@code true} if authenticated, {@code false} otherwise
     * @dochub authenticate
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, java.util.List)} to create a client, which
     *             will authentificate all connections to server
     */
    @Deprecated
    public boolean isAuthenticated() {
        return getAuthenticationCredentials() != null;
    }

    /**
     * Authenticates to db with the given credentials.  If this method (or {@code authenticateCommand}) has already been
     * called with the same credentials and the authentication test succeeded, this method will return {@code true}.  If this method
     * has already been called with different credentials and the authentication test succeeded,
     * this method will throw an {@code IllegalStateException}.  If this method has already been called with any credentials
     * and the authentication test failed, this method will re-try the authentication test with the
     * given credentials.
     *
     * @param username name of user for this database
     * @param password password of user for this database
     * @return true if authenticated, false otherwise
     * @throws MongoException        if authentication failed due to invalid user/pass, or other exceptions like I/O
     * @throws IllegalStateException if authentiation test has already succeeded with different credentials
     * @dochub authenticate
     * @see #authenticateCommand(String, char[])
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, java.util.List)} to create a client, which
     *             will authentificate all connections to server
     */
    @Deprecated
    public boolean authenticate(String username, char[] password) {
        return authenticateCommandHelper(username, password).failure == null;
    }

    /**
     * Authenticates to db with the given credentials.  If this method (or {@code authenticate}) has already been
     * called with the same credentials and the authentication test succeeded, this method will return true.  If this method
     * has already been called with different credentials and the authentication test succeeded,
     * this method will throw an {@code IllegalStateException}.  If this method has already been called with any credentials
     * and the authentication test failed, this method will re-try the authentication test with the
     * given credentials.
     *
     * @param username name of user for this database
     * @param password password of user for this database
     * @return the CommandResult from authenticate command
     * @throws MongoException        if authentication failed due to invalid user/pass, or other exceptions like I/O
     * @throws IllegalStateException if authentiation test has already succeeded with different credentials
     * @dochub authenticate
     * @see #authenticate(String, char[])
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, java.util.List)} to create a client, which
     *             will authentificate all connections to server
     */
    @Deprecated
    public synchronized CommandResult authenticateCommand(String username, char[] password) {
        CommandResultPair commandResultPair = authenticateCommandHelper(username, password);
        if (commandResultPair.failure != null) {
            throw commandResultPair.failure;
        }
        return commandResultPair.result;
    }

    private CommandResultPair authenticateCommandHelper(String username, char[] password) {
        MongoCredential credentials =
                MongoCredential.createMongoCRCredential(username, getName(), password);
        if (getAuthenticationCredentials() != null) {
            if (getAuthenticationCredentials().equals(credentials)) {
                if (authenticationTestCommandResult != null) {
                    return new CommandResultPair(authenticationTestCommandResult);
                }
            } else {
                throw new IllegalStateException("can't authenticate twice on the same database");
            }
        }

        try {
            authenticationTestCommandResult = doAuthenticate(credentials);
            return new CommandResultPair(authenticationTestCommandResult);
        } catch (CommandFailureException commandFailureException) {
            return new CommandResultPair(commandFailureException);
        }
    }

    class CommandResultPair {
        CommandResult result;
        CommandFailureException failure;

        public CommandResultPair(final CommandResult result) {
            this.result = result;
        }

        public CommandResultPair(final CommandFailureException failure) {
            this.failure = failure;
        }
    }

    abstract CommandResult doAuthenticate(MongoCredential credentials);

    /**
     * Adds a new user for this db
     *
     * @param username
     * @param passwd
     * @throws MongoException
     */
    public WriteResult addUser( String username , char[] passwd ){
        return addUser(username, passwd, false);
    }

    /**
     * Adds privilege documents to the {@code system.users} collection in a database,
     * which creates database credentials in MongoDB.
     *
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
     * Removes the specified username from the database.
     *
     * @param username user to be removed
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
     * Returns the last error that occurred since start of database or a call to {@link com.mongodb.DB#resetError()}
     * <p/>
     * The return object will look like:
     * <p/>
     * <code>
     * { err : errorMessage, nPrev : countOpsBack, ok : 1 }
     * </code>
     * <p/>
     * The value for errorMessage will be null of no error has occurred, otherwise the error message.
     * The value of countOpsBack will be the number of operations since the error occurred.
     * <p/>
     * Care must be taken to ensure that calls to getPreviousError go to the same connection as that
     * of the previous operation.
     * See {@link DB#requestStart()} for more information.
     *
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     */
    public CommandResult getPreviousError(){
        return command(new BasicDBObject("getpreverror", 1));
    }

    /**
     * Resets the error memory for this database.
     * Used to clear all errors such that {@link DB#getPreviousError()} will return no error.
     *
     * @throws MongoException
     */
    public void resetError(){
        command(new BasicDBObject("reseterror", 1));
    }

    /**
     * For testing purposes only - this method forces an error to help test error handling
     *
     * @throws MongoException
     */
    public void forceError(){
        command(new BasicDBObject("forceerror", 1));
    }

    /**
     * Gets the {@link Mongo} instance
     *
     * @return the instance of {@link Mongo} this database belongs to
     */
    public Mongo getMongo(){
        return _mongo;
    }

    /**
     * Gets another database on same server
     *
     * @param name name of the database
     * @return the database
     */
    public DB getSisterDB( String name ){
        return _mongo.getDB( name );
    }

    /**
     * Makes it possible to execute "read" queries on a slave node
     *
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * Adds the given flag to the query options.
     *
     * @param option value to be added
     */
    public void addOption( int option ){
        _options.add( option );
    }

    /**
     * Sets the query options, overwriting previous value.
     *
     * @param options bit vector of query options
     */
    public void setOptions( int options ){
        _options.set( options );
    }

    /**
     * Resets the query options.
     */
    public void resetOptions(){
        _options.reset();
    }

    /**
     * Gets the query options
     *
     * @return bit vector of query options
     */
    public int getOptions(){
        return _options.get();
    }

    public abstract void cleanCursors( boolean force );

    MongoCredential getAuthenticationCredentials() {
        return getMongo().getAuthority().getCredentialsStore().get(getName());
    }

    final Mongo _mongo;
    final String _name;


    /**
     * @deprecated See {@link #setReadOnly(Boolean)}
     */
    @Deprecated
    protected boolean _readOnly = false;
    private com.mongodb.WriteConcern _concern;
    private com.mongodb.ReadPreference _readPref;
    final Bytes.OptionHolder _options;

    // cached authentication command result, to return in case of multiple calls to authenticateCommand with the
    // same credentials
    private volatile CommandResult authenticationTestCommandResult;
}
