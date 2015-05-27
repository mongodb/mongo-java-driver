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

import com.mongodb.util.Util;
import org.bson.BSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A thread-safe client view of a logical database in a MongoDB cluster. A DB instance can be achieved from a {@link MongoClient} instance
 * using code like:
 * <pre>
 * {@code
 * MongoClient mongoClient = new MongoClient();
 * DB db = mongoClient.getDB("<db name>");
 * }</pre>
 *
 * @mongodb.driver.manual reference/glossary/#term-database Database
 * @see MongoClient
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
        _obedientCommands.add("parallelcollectionscan");
        _obedientCommands.add("listindexes");
        _obedientCommands.add("listcollections");
    }

    /**
     * Constructs a new instance of the {@code DB}.
     *
     * @param mongo the mongo instance
     * @param name  the database name - must not be empty and cannot contain spaces
     */
    public DB(final Mongo mongo, final String name) {
        if(!isValidName(name))
            throw new IllegalArgumentException("Invalid database name format. Database name is either empty or it contains spaces.");
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
        if (_mongo.getReplicaSetStatus() == null) {
            return requestedPreference;
        }

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
            } else {
                primaryRequired = true;
            }
        } else if(comString.equals("aggregate")) {
            @SuppressWarnings("unchecked")
            List<DBObject> pipeline = (List<DBObject>) command.get("pipeline");
            primaryRequired = pipeline.get(pipeline.size()-1).get("$out") != null;
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
     * <p>Starts a new 'consistent request'</p>.
     *
     * <p>Following this call and until {@link com.mongodb.DB#requestDone()} is called, all db operations will use the same underlying
     * connection.</p>
     *
     * @deprecated  The main use case for this method is to ensure that applications can read their own unacknowledged writes,
     * but this is no longer so prevalent since the driver started defaulting to acknowledged writes. The other main use case is to
     * ensure that related read operations are all routed to the same server when using a non-primary read preference.  But this is
     * dangerous because mongos does not provide this guarantee.  For these reasons, this method is now deprecated and will be
     * removed in the next major release.
     */
    @Deprecated
    public abstract void requestStart();

    /**
     * Ends the current 'consistent request'.
     * @deprecated  The main use case for this method is to ensure that applications can read their own unacknowledged writes,
     * but this is no longer so prevalent since the driver started defaulting to acknowledged writes. The other main use case is to
     * ensure that related read operations are all routed to the same server when using a non-primary read preference.  But this is
     * dangerous because mongos does not provide this guarantee.  For these reasons, this method is now deprecated and will be
     * removed in the next major release.
     */
    @Deprecated
    public abstract void requestDone();

    /**
     * Ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a replica set).
     *
     * @deprecated  The main use case for this method is to ensure that applications can read their own unacknowledged writes,
     * but this is no longer so prevalent since the driver started defaulting to acknowledged writes. The other main use case is to
     * ensure that related read operations are all routed to the same server when using a non-primary read preference.  But this is
     * dangerous because mongos does not provide this guarantee.  For these reasons, this method is now deprecated and will be
     * removed in the next major release.
     */
    @Deprecated
    public abstract void requestEnsureConnection();

    /**
     * <p>Gets a collection with a given name. If the collection does not exist, a new collection is created.</p>
     *
     * <p>This class is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.</p>
     *
     * @param name the name of the collection
     * @return the collection
     */
    protected abstract DBCollection doGetCollection( String name );

    /**
     * Gets a collection with a given name.
     *
     * @param name the name of the collection to return
     * @return the collection
     */
    public DBCollection getCollection( String name ){
        DBCollection c = doGetCollection( name );
        return c;
    }

    /**
     * <p>Creates a collection with a given name and options. If the collection already exists, this throws a
     * {@code CommandFailureException}.</p>
     *
     * <p>Possible options:</p>
     * <ul>
     *     <li> <b>capped</b> ({@code boolean}) - Enables a collection cap. False by default. If enabled,
     *     you must specify a size parameter. </li>
     *     <li> <b>size</b> ({@code int}) - If capped is true, size specifies a maximum size in bytes for the capped collection. When
     *     capped is false, you may use size to preallocate space. </li>
     *     <li> <b>max</b> ({@code int}) -   Optional. Specifies a maximum "cap" in number of documents for capped collections. You must
     *     also specify size when specifying max. </li>
     * </ul>
     * <p>Note that if the {@code options} parameter is {@code null}, the creation will be deferred to when the collection is written
     * to.</p>
     *
     * @param name    the name of the collection to return
     * @param options options
     * @return the collection
     * @throws MongoException
     * @mongodb.driver.manual reference/method/db.createCollection/ createCollection()
     */
    public DBCollection createCollection(final String name, final DBObject options) {
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
    public DBCollection getCollectionFromString(String s) {
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
     * Executes a database command. This method calls {@link DB#command(DBObject, ReadPreference) } with the default read preference for the
     * database.
     *
     * @param cmd {@code DBObject} representation of the command to be executed
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject cmd) {
        return command( cmd, 0 );
    }


    /**
     * Executes a database command. This method calls {@link DB#command(DBObject, ReadPreference, DBEncoder) } with the default read
     * preference for the database.
     *
     * @param cmd     {@code DBObject} representation of the command to be executed
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject cmd, final DBEncoder encoder) {
        return command( cmd, 0, encoder );
    }

    /**
     * Executes a database command. This method calls
     * {@link #command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with the database default read
     * preference.  The only option used by this method was "slave ok", therefore this method has been replaced with
     * {@link #command(DBObject, ReadPreference, DBEncoder)}.
     *
     * @param cmd     {@code DBObject} representation the command to be executed
     * @param options query options to use
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @deprecated Use {@link com.mongodb.DB#command(DBObject, ReadPreference, DBEncoder)} instead.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult command(final DBObject cmd, final int options, final DBEncoder encoder) {
        return command(cmd, options, getReadPreference(), encoder);
    }

    /**
     * Executes a database command. This method calls
     * {@link #command(com.mongodb.DBObject, int, com.mongodb.ReadPreference, com.mongodb.DBEncoder) } with a default encoder.  The only
     * option used by this method was "slave ok", therefore this method has been replaced
     * with {@link #command(DBObject, ReadPreference)}.
     *
     * @param cmd            A {@code DBObject} representation the command to be executed
     * @param options        The query options to use
     * @param readPreference The {@link ReadPreference} for this command (nodes selection is the biggest part of this)
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @deprecated Use {@link com.mongodb.DB#command(DBObject, ReadPreference)} instead.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult command( final DBObject cmd , final int options, final ReadPreference readPreference ){
        return command(cmd, options, readPreference, DefaultDBEncoder.FACTORY.create());
    }

    /**
     * Executes a database command.  The only option used by this method was "slave ok", therefore this method has been replaced with
     * {@link #command(DBObject, ReadPreference, DBEncoder)}.
     *
     * @param cmd            A {@code DBObject} representation the command to be executed
     * @param options        The query options to use
     * @param readPreference The {@link ReadPreference} for this command (nodes selection is the biggest part of this)
     * @param encoder        A {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @deprecated Use {@link com.mongodb.DB#command(DBObject, ReadPreference, DBEncoder)} instead.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult command(DBObject cmd, final int options, final ReadPreference readPreference, final DBEncoder encoder) {
        ReadPreference effectiveReadPrefs = getCommandReadPreference(cmd, readPreference);
        cmd = wrapCommand(cmd, effectiveReadPrefs);

        QueryResultIterator i = getCollection("$cmd").find(cmd, new BasicDBObject(), 0, -1, 0, options, effectiveReadPrefs,
                                           DefaultDBDecoder.FACTORY.create(), encoder);
        if (!i.hasNext()) {
            return null;
        }

        CommandResult cr = new CommandResult(i.getServerAddress());
        cr.putAll(i.next());
        return cr;
    }

    /**
     * Executes a database command with the selected readPreference, and encodes the command using the given encoder.
     *
     * @param cmd            The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @param encoder        The DBEncoder that knows how to serialise the command
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject cmd, final ReadPreference readPreference, final DBEncoder encoder) {
        return command(cmd, 0, readPreference, encoder);
    }

    // Only append $readPreference meta-operator if connected to a mongos, read preference is not primary
    // or secondary preferred,
    // and command is an instance of BasicDBObject.  The last condition is unfortunate, but necessary in case
    // the encoder is not capable of encoding a BasicDBObject
    // Due to issues with compatibility between different versions of mongos, also wrap the command in a
    // $query field, so that the $readPreference is not rejected
    private DBObject wrapCommand(DBObject cmd, final ReadPreference readPreference) {
        if (getMongo().isMongosConnection() &&
                !(ReadPreference.primary().equals(readPreference) || ReadPreference.secondaryPreferred().equals(readPreference)) &&
                cmd instanceof BasicDBObject) {
            cmd = new BasicDBObject("$query", cmd)
                    .append(QueryOpBuilder.READ_PREFERENCE_META_OPERATOR, readPreference.toDBObject());
        }
        return cmd;
    }

    /**
     * Executes a database command with the given query options.  The only option used by this method was "slave ok", therefore this method
     * has been replaced with {@link #command(DBObject, ReadPreference)}.
     *
     * @param cmd     The {@code DBObject} representation the command to be executed
     * @param options The query options to use
     * @return The result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @deprecated Use {@link com.mongodb.DB#command(DBObject, ReadPreference)} instead.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult command(final DBObject cmd, final int options) {
        return command(cmd, options, getReadPreference());
    }

    /**
     * Executes the command against the database with the given read preference.  This method is the preferred way of setting read
     * preference, use this instead of {@link #command(com.mongodb.DBObject, int) }
     *
     * @param cmd            The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject cmd, final ReadPreference readPreference) {
        return command(cmd, 0, readPreference);
    }

    /**
     * Executes a database command. This method constructs a simple DBObject using {@code command} as the field name and {@code true} as its
     * value, and calls {@link #command(DBObject, ReadPreference) } with the default read preference for the database.
     *
     * @param cmd command to execute
     * @return result of command from the database
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final String cmd) {
        return command( new BasicDBObject( cmd , Boolean.TRUE ) );
    }

    /**
     * Executes a database command. This method constructs a simple DBObject and calls {@link #command(com.mongodb.DBObject, int)}
     *
     * @param cmd     name of the command to be executed
     * @param options query options to use
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @deprecated Use {@link com.mongodb.DB#command(String, ReadPreference)} instead.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult command(final String cmd, final int options) {
        return command( new BasicDBObject( cmd , Boolean.TRUE ), options );
    }

    /**
     * Executes a database command. This method constructs a simple {@link DBObject} and calls {@link #command(DBObject, ReadPreference) }.
     *
     * @param cmd            The name of the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final String cmd, final ReadPreference readPreference) {
        return command(new BasicDBObject(cmd, Boolean.TRUE), 0, readPreference);
    }

    /**
     * Evaluates JavaScript functions on the database server. This is useful if you need to touch a lot of data lightly, in which case
     * network transfer could be a bottleneck.
     *
     * @param code {@code String} representation of JavaScript function
     * @param args arguments to pass to the JavaScript function
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual reference/method/db.eval/ db.eval()
     */
    public CommandResult doEval( String code , Object ... args ){

        return command( BasicDBObjectBuilder.start()
                        .add( "$eval" , code )
                        .add( "args" , args )
                        .get() );
    }

    /**
     * Calls {@link DB#doEval(java.lang.String, java.lang.Object[]) }. If the command is successful, the "retval" field is extracted and
     * returned. Otherwise an exception is thrown.
     *
     * @param code {@code String} representation of JavaScript function
     * @param args arguments to pass to the JavaScript function
     * @return result of the execution
     * @throws MongoException
     * @mongodb.driver.manual reference/method/db.eval/ db.eval()
     */
    public Object eval( String code , Object ... args ){

        CommandResult res = doEval( code , args );
        res.throwOnError();
        return res.get( "retval" );
    }

    /**
     * Helper method for calling a 'dbStats' command. It returns storage statistics for a given database.
     *
     * @return result of the execution
     * @throws MongoException
     * @mongodb.driver.manual reference/command/dbStats/ Database Stats
     */
    public CommandResult getStats() {
        CommandResult result = command("dbstats");
        result.throwOnError();
        return result;
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
     * Returns a set containing the names of all collections in this database.
     *
     * @return the names of collections in this database
     * @throws MongoException
     * @mongodb.driver.manual reference/method/db.getCollectionNames/ getCollectionNames()
     */
    public abstract Set<String> getCollectionNames();

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
     * Returns the error status of the last operation on the current connection. The result of this command will look like:
     * <pre>
     * {@code
     * { "err" :  errorMessage  , "ok" : 1.0 }
     * }</pre>
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     * <p> Important note: when calling this method directly, it is undefined which connection "getLastError" is called on. You may need
     * to explicitly use a "consistent Request", see {@link DB#requestStart()} It is better not to call this method directly but instead
     * use {@link WriteConcern} </p>
     *
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @see WriteConcern#ACKNOWLEDGED
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     */
    @Deprecated
    public CommandResult getLastError(){
        return command(new BasicDBObject("getlasterror", 1));
    }

    /**
     * Returns the error status of the last operation on the current connection.
     *
     * @param concern a {@link WriteConcern} to be used while checking for the error status.
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     * @see WriteConcern#ACKNOWLEDGED
     */
    @Deprecated
    public CommandResult getLastError(final com.mongodb.WriteConcern concern) {
        return command( concern.getCommand() );
    }

    /**
     * Returns the error status of the last operation on the current connection.
     *
     * @param w        when running with replication, this is the number of servers to replicate to before returning. A {@code w} value
     *                 of {@code 1} indicates the primary only. A {@code w} value of {@code 2} includes the primary and at least one
     *                 secondary, etc. In place of a number, you may also set {@code w} to majority to indicate that the command should
     *                 wait until the latest write propagates to a majority of replica set members. If using {@code w},
     *                 you should also use {@code wtimeout}. Specifying a value for {@code w} without also providing a {@code wtimeout} may
     *                 cause {@code getLastError} to block indefinitely.
     * @param wtimeout a value in milliseconds that controls how long to wait for write propagation to complete. If replication does not
     *                 complete in the given time frame, the getLastError command will return with an error status.
     * @param fsync    if {@code true}, wait for MongoDB to write this data to disk before returning. Defaults to {@code false}.
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     * @see WriteConcern#ACKNOWLEDGED
     */
    @Deprecated
    public CommandResult getLastError(final int w, final int wtimeout, final boolean fsync) {
        return command( (new com.mongodb.WriteConcern( w, wtimeout , fsync )).getCommand() );
    }


    /**
     * Sets the write concern for this database. It will be used for write operations to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern {@code WriteConcern} to use
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        if (writeConcern == null) {
            throw new IllegalArgumentException();
        }
        _concern = writeConcern;
    }

    /**
     * Gets the write concern for this database.
     *
     * @return {@code WriteConcern} to be used for write operations, if not specified explicitly
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public WriteConcern getWriteConcern(){
        if ( _concern != null )
            return _concern;
        return _mongo.getWriteConcern();
    }

    /**
     * Sets the read preference for this database. Will be used as default for read operations from any collection in this database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference {@code ReadPreference} to use
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public void setReadPreference(final ReadPreference readPreference) {
        _readPref = readPreference;
    }

    /**
     * Gets the read preference for this database.
     *
     * @return {@code ReadPreference} to be used for read operations, if not specified explicitly
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public ReadPreference getReadPreference(){
        if ( _readPref != null )
            return _readPref;
        return _mongo.getReadPreference();
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     *
     * @throws MongoException
     * @mongodb.driver.manual reference/command/dropDatabase/ Drop Database
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
     *             will authenticate all connections to server
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
     * @throws IllegalStateException if authentication test has already succeeded with different credentials
     * @dochub authenticate
     * @see #authenticateCommand(String, char[])
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, java.util.List)} to create a client, which
     *             will authenticate all connections to server
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
     * @throws IllegalStateException if authentication test has already succeeded with different credentials
     * @dochub authenticate
     * @see #authenticate(String, char[])
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, java.util.List)} to create a client, which
     *             will authenticate all connections to server
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
                MongoCredential.createCredential(username, getName(), password);
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
     * Adds or updates a user for this database
     *
     * @param username the user name
     * @param passwd   the password
     * @return the result of executing this operation
     * @throws MongoException
     * @mongodb.driver.manual administration/security-access-control/  Access Control
     * @mongodb.driver.manual reference/command/createUser createUser
     * @mongodb.driver.manual reference/command/updateUser updateUser
     * @deprecated Use {@code DB.command} to call either the createUser or updateUser command
     */
    @Deprecated
    public WriteResult addUser( String username , char[] passwd ){
        return addUser(username, passwd, false);
    }

    /**
     * Adds or updates a user for this database
     *
     * @param username the user name
     * @param passwd the password
     * @param readOnly if true, user will only be able to read
     * @return the result of executing this operation
     * @throws MongoException
     * @mongodb.driver.manual administration/security-access-control/  Access Control
     * @mongodb.driver.manual reference/command/createUser createUser
     * @mongodb.driver.manual reference/command/updateUser updateUser
     * @deprecated Use {@code DB.command} to call either the createUser or updateUser command
     */
    @Deprecated
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
     * Removes the specified user from the database.
     *
     * @param username user to be removed
     * @return the result of executing this operation
     * @throws MongoException
     * @mongodb.driver.manual administration/security-access-control/  Access Control
     * @mongodb.driver.manual reference/command/dropUser dropUser
     * @deprecated Use {@code DB.command} to call the dropUser command
     */
    @Deprecated
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
     * Returns the last error that occurred since start of database or a call to {@link com.mongodb.DB#resetError()} The return object
     * will look like:
     * <pre>
     * {@code
     * { err : errorMessage, nPrev : countOpsBack, ok : 1 }
     * }</pre>
     * The value for errorMessage will be null of no error has occurred, otherwise the error message.
     * The value of countOpsBack will be the number of operations since the error occurred.
     * <p> Care must be taken to ensure that calls to getPreviousError go to the same connection as that
     * of the previous operation. See {@link DB#requestStart()} for more information.</p>
     *
     * @return {@code DBObject} with error and status information
     * @throws MongoException
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     * @see WriteConcern#ACKNOWLEDGED
     */
    @Deprecated
    public CommandResult getPreviousError(){
        return command(new BasicDBObject("getpreverror", 1));
    }

    /**
     * Resets the error memory for this database.
     * Used to clear all errors such that {@link DB#getPreviousError()} will return no error.
     *
     * @throws MongoException
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     * @see WriteConcern#ACKNOWLEDGED
     */
    @Deprecated
    public void resetError(){
        command(new BasicDBObject("reseterror", 1));
    }

    /**
     * For testing purposes only - this method forces an error to help test error handling
     *
     * @throws MongoException
     * @deprecated The getlasterror command will not be supported in future versions of MongoDB.  Use acknowledged writes instead.
     * @see WriteConcern#ACKNOWLEDGED
     */
    @Deprecated
    public void forceError(){
        command(new BasicDBObject("forceerror", 1));
    }

    /**
     * Gets the Mongo instance
     *
     * @return the mongo instance that this database was created from.
     */
    public Mongo getMongo(){
        return _mongo;
    }

    /**
     * Gets another database on same server
     *
     * @param name name of the database
     * @return the DB for the given name
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
     * Adds the given flag to the default query options.
     *
     * @param option value to be added
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public void addOption( int option ){
        _options.add( option );
    }

    /**
     * Sets the default query options, overwriting previous value.
     *
     * @param options bit vector of query options
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public void setOptions( int options ){
        _options.set( options );
    }

    /**
     * Resets the query options.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public void resetOptions(){
        _options.reset();
    }

    /**
     * Gets the default query options
     *
     * @return bit vector of query options
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public int getOptions(){
        return _options.get();
    }

    private boolean isValidName(String dbname){
        return dbname.length() != 0 && !dbname.contains(" ");
    }

    /**
     * Forcefully kills any cursors leaked by neglecting to call {@code DBCursor.close}
     *
     * @param force true if should clean regardless of number of dead cursors
     * @see com.mongodb.DBCursor#close()
     * @deprecated Clients should ensure that {@link DBCursor#close()} is called.
     */
    @Deprecated
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
