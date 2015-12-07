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

import org.bson.io.PoolOutputBuffer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * A database connection with internal connection pooling. For most applications, you should have one Mongo instance
 * for the entire JVM.
 * <p>
 * The following are equivalent, and all connect to the local database running on the default port:
 * <pre>
 * Mongo mongo1 = new Mongo();
 * Mongo mongo1 = new Mongo("localhost");
 * Mongo mongo2 = new Mongo("localhost", 27017);
 * Mongo mongo4 = new Mongo(new ServerAddress("localhost"));
 * </pre>
 * <p>
 * You can connect to a
 * <a href="http://www.mongodb.org/display/DOCS/Replica+Sets">replica set</a> using the Java driver by passing
 * a ServerAddress list to the Mongo constructor. For example:
 * <pre>
 * Mongo mongo = new Mongo(Arrays.asList(
 *   new ServerAddress("localhost", 27017),
 *   new ServerAddress("localhost", 27018),
 *   new ServerAddress("localhost", 27019)));
 * </pre>
 * You can connect to a sharded cluster using the same constructor.  Mongo will auto-detect whether the servers are
 * a list of replica set members or a list of mongos servers.
 * <p>
 * By default, all read and write operations will be made on the primary,
 * but it's possible to read from secondaries by changing the read preference:
 * <p>
 * <pre>
 * mongo.setReadPreference(ReadPreference.secondary());
 * </pre>
 * By default, write operations will not throw exceptions on failure, but that is easily changed too:
 * <p>
 * <pre>
 * mongo.setWriteConcern(WriteConcern.SAFE);
 * </pre>
 *
 * Note: This class has been superseded by {@code MongoClient}, and may be deprecated in a future release.
 *
 * @see MongoClient
 * @see ReadPreference
 * @see WriteConcern
 */
public class Mongo {

    static Logger logger = Logger.getLogger(Bytes.LOGGER.getName() + ".Mongo");


    // Make sure you don't change the format of these two static variables. A preprocessing regexp
    // is applied and updates the version based on configuration in build.properties.

    /**
     * @deprecated Replaced by {@link #getMajorVersion()}
     */
    @Deprecated
    public static final int MAJOR_VERSION = 2;

    /**
     * @deprecated Replaced by {@link #getMinorVersion()}
     */
    @Deprecated
    public static final int MINOR_VERSION = 15;

    private static final String FULL_VERSION = "2.15.0-SNAPSHOT";

    static int cleanerIntervalMS;

    private static final String ADMIN_DATABASE_NAME = "admin";

    static {
        cleanerIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.cleanerIntervalMS", "1000"));
    }

    /**
     * Gets the major version of this library
     *
     * @return the major version, e.g. 2
     * @deprecated Please use {@link #getVersion()} instead.
     */
    @Deprecated
    public static int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * Gets the minor version of this library
     *
     * @return the minor version, e.g. 8
     * @deprecated Please use {@link #getVersion()} instead.
     */
    @Deprecated
    public static int getMinorVersion() {
        return MINOR_VERSION;
    }

    /**
     * Connect to the MongoDB instance at the given address, select and return the {@code DB} specified in the {@code DBAddress} parameter.
     *
     * @param addr The details of the server and database to connect to
     * @return the DB requested in the addr parameter.
     * @throws MongoException
     * @deprecated Please use {@link MongoClient#getDB(String)} instead.
     */
    @Deprecated
    public static DB connect( DBAddress addr ){
        return new Mongo( addr ).getDB( addr.getDBName() );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (localhost, default port)
     *
     * @throws UnknownHostException This exception is no longer thrown, but leaving in throws clause so as not to break source
     *                              compatibility.  The exception will be removed from the declaration in the next major release of the
     *                              driver.
     * @deprecated Replaced by {@link MongoClient#MongoClient()})
     */
    @Deprecated
    public Mongo() throws UnknownHostException {
        this(new ServerAddress() );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     *
     * @param host server to connect to
     * @throws UnknownHostException This exception is no longer thrown, but leaving in throws clause so as not to break source
     *                              compatibility.  The exception will be removed from the declaration in the next major release of the
     *                              driver.
     * @deprecated Replaced by {@link MongoClient#MongoClient(String)}
     */
    @Deprecated
    public Mongo(String host)
        throws UnknownHostException{
        this( new ServerAddress( host ) );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     *
     * @param host    server to connect to
     * @param options default query options
     * @throws UnknownHostException This exception is no longer thrown, but leaving in throws clause so as not to break source
     *                              compatibility.  The exception will be removed from the declaration in the next major release of the
     *                              driver.
     * @deprecated Replaced by {@link MongoClient#MongoClient(String, MongoClientOptions)}
     */
    @Deprecated
    public Mongo(String host, MongoOptions options)
        throws UnknownHostException {
        this( new ServerAddress( host ) , options );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param host the database's host address
     * @param port the port on which the database is running
     * @throws UnknownHostException This exception is no longer thrown, but leaving in throws clause so as not to break source
     *                              compatibility.  The exception will be removed from the declaration in the next major release of the
     *                              driver.
     * @deprecated Replaced by {@link MongoClient#MongoClient(String, int)}
     */
    @Deprecated
    public Mongo(String host, int port)
        throws UnknownHostException {
        this( new ServerAddress( host , port ) );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param addr the database address
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(ServerAddress)}
     */
    @Deprecated
    public Mongo(ServerAddress addr) {
        this(addr, new MongoOptions());
    }

    /**
     * Creates a Mongo instance based on a (single) mongo node using a given ServerAddress
     *
     * @param addr    the database address
     * @param options default query options
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(ServerAddress, MongoClientOptions)}
     */
    @Deprecated
    public Mongo(ServerAddress addr, MongoOptions options) {
        this(MongoAuthority.direct(addr), options);
    }

    /**
     * Creates a Mongo in paired mode. This will also work for a replica set and will find all members (the master will be used by
     * default).
     *
     * @param left  left side of the pair
     * @param right right side of the pair
     * @throws MongoException
     */
    @Deprecated
    public Mongo(ServerAddress left, ServerAddress right) {
        this(left, right, new MongoOptions());
    }

    /**
     * Creates a Mongo connection in paired mode. This will also work for a replica set and will find all members (the master will be used
     * by default).
     *
     * @param left    left side of the pair
     * @param right   right side of the pair
     * @param options the optional settings for the Mongo instance
     * @see com.mongodb.ServerAddress
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, MongoClientOptions)} instead.
     */
    @Deprecated
    public Mongo(ServerAddress left, ServerAddress right, MongoOptions options) {
        this(MongoAuthority.dynamicSet(Arrays.asList(left, right)), options);
    }

    /**
     * <p>Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.</p>
     *
     * <p>If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all requests to, and automatically
     * fail over to the next server if the closest is down.</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(java.util.List)}
     */
    @Deprecated
    public Mongo(List<ServerAddress> seeds) {
        this( seeds , new MongoOptions() );
    }

    /**
     * <p>Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.</p>
     *
     * <p>If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all requests to, and automatically
     * fail over to the next server if the closest is down.</p>
     *
     * @param seeds   Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of
     *                mongod servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param options for configuring this Mongo instance
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(java.util.List, MongoClientOptions)}
     */
    @Deprecated
    public Mongo( List<ServerAddress> seeds , MongoOptions options ) {
        this(MongoAuthority.dynamicSet(seeds), options);
    }

    /**
     * <p>Creates a Mongo described by a URI. If only one address is used it will only connect to that node, otherwise it will discover all
     * nodes. If the URI contains database credentials, the database will be authenticated lazily on first use with those credentials.</p>
     * 
     * <p>Examples:</p> 
     * <ul>
     *   <li>mongodb://localhost</li>
     *   <li>mongodb://fred:foobar@localhost/</li>
     * </ul>
     *
     * @param uri URI to connect to, optionally containing additional information like credentials
     * @mongodb.driver.manual reference/connection-string Connection String URI Format
     * @see MongoURI 
     * @deprecated Replaced by {@link MongoClient#MongoClient(MongoClientURI)}
     */
    @Deprecated
    public Mongo(MongoURI uri) throws UnknownHostException {
        this(getMongoAuthorityFromURI(uri), uri.getOptions());
    }

    /**
     * <p>Creates a Mongo based on an authority and options.</p>
     * 
     * <p>Note: This constructor is provisional and is subject to change before the final release</p>
     *
     * @param authority the authority
     * @param options the options
     */
    Mongo(MongoAuthority authority, MongoOptions options) {
        logger.info("Creating Mongo instance (driver version " + getVersion() + ") with authority " + authority + " and options " + options);
        _authority = authority;
        _options = options;
        _applyMongoOptions();

        _connector = new DBTCPConnector( this  );

        _connector.start();
        if (_options.cursorFinalizerEnabled) {
            _cleaner = new CursorCleanerThread();
            _cleaner.start();
        } else {
            _cleaner = null;
        }
    }

    /**
     * Gets a database object from this MongoDB instance.
     *
     * @param dbname the name of the database to retrieve
     * @return a DB representing the specified database
     */
    public DB getDB( String dbname ){

        DB db = _dbs.get( dbname );
        if ( db != null )
            return db;

        db = new DBApiLayer( this , dbname , _connector );
        DB temp = _dbs.putIfAbsent( dbname , db );
        if ( temp != null )
            return temp;
        return db;
    }

    /**
     * Returns the list of databases used by the driver since this Mongo instance was created. This may include DBs that exist in the client
     * but not yet on the server.
     *
     * @return a collection of database objects
     */
    public Collection<DB> getUsedDatabases(){
        return _dbs.values();
    }

    /**
     * Gets a list of the names of all databases on the connected server.
     *
     * @return list of database names
     * @throws MongoException
     */
    public List<String> getDatabaseNames(){

        BasicDBObject cmd = new BasicDBObject();
        cmd.put("listDatabases", 1);


        CommandResult res = getDB(ADMIN_DATABASE_NAME).command(cmd, getOptions());
        res.throwOnError();

        List l = (List)res.get("databases");

        List<String> list = new ArrayList<String>();

        for (Object o : l) {
            list.add(((BasicDBObject)o).getString("name"));
        }
        return list;
    }


    /**
     * Drops the database if it exists.
     *
     * @param dbName name of database to drop
     * @throws MongoException
     */
    public void dropDatabase(String dbName){

        getDB( dbName ).dropDatabase();
    }

    /**
     * Gets this driver version.
     *
     * @return the full version string of this driver, e.g. "2.8.0"
     * @deprecated There is no replacement for this method.
     */
    @Deprecated
    public String getVersion(){
        return FULL_VERSION;
    }

    /**
     * Get a String for debug purposes.
     *
     * @return a string representing the hosts used in this Mongo instance
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public String debugString(){
        return _connector.debugString();
    }

    /**
     * Gets a {@code String} representation of current connection point, i.e. master.
     *
     * @return server address in a host:port form
     */
    public String getConnectPoint(){
        return _connector.getConnectPoint();
    }

    /**
     * Gets the underlying TCP connector
     *
     * @return A DBTCPConnector representing the connection to MongoDB
     * @deprecated {@link DBTCPConnector} is NOT part of the public API. It will be dropped in 3.x releases.
     */
    @Deprecated
    public DBTCPConnector getConnector() {
        return _connector;
    }

    /**
     * Get the status of the replica set cluster.
     *
     * @return replica set status information
     */
    public ReplicaSetStatus getReplicaSetStatus() {
        return _connector.getReplicaSetStatus();
    }

    /**
     * Gets the address of the current master
     *
     * @return the address
     */
    public ServerAddress getAddress(){
        return _connector.getAddress();
    }

    /**
     * Gets a list of all server addresses used when this Mongo was created
     *
     * @return list of server addresses
     * @throws MongoException
     */
    public List<ServerAddress> getAllAddress() {
        List<ServerAddress> result = _connector.getAllAddress();
        if (result == null) {
            return Arrays.asList(getAddress());
        }
        return result;
    }

    /**
     * Gets the list of server addresses currently seen by this client. This includes addresses auto-discovered from a replica set.
     *
     * @return list of server addresses
     * @throws MongoException
     */
    public List<ServerAddress> getServerAddressList() {
        return _connector.getServerAddressList();
    }

    /**
     * Closes the underlying connector, which in turn closes all open connections. Once called, this Mongo instance can no longer be used.
     */
    public void close(){

        try {
            _connector.close();
        } catch (final Throwable t) { /* nada */ }

        if (_cleaner != null) {
            _cleaner.interrupt();

            try {
                _cleaner.join();
            } catch (InterruptedException e) {
                //end early
            }
        }
    }

    /**
     * Sets the write concern for this database. Will be used as default for writes to any collection in any database. See the documentation
     * for {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    public void setWriteConcern( WriteConcern concern ){
        _concern = concern;
    }

    /**
     * Gets the default write concern
     *
     * @return the default write concern
     */
    public WriteConcern getWriteConcern(){
        return _concern;
    }

    /**
     * Sets the read preference for this database. Will be used as default for reads from any collection in any database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference(ReadPreference preference) {
        _readPref = preference;
    }

    /**
     * Gets the default read preference
     *
     * @return the default read preference
     */
    public ReadPreference getReadPreference(){
        return _readPref;
    }

    /**
     * Makes it possible to run read queries on secondary nodes
     *
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public void slaveOk() {
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * Add a default query option keeping any previously added options.
     *
     * @param option value to be added to current options
     */
    public void addOption( int option ){
        _netOptions.add( option );
    }

    /**
     * Set the default query options.  Overrides any existing options.
     *
     * @param options value to be set
     */
    public void setOptions( int options ){
        _netOptions.set( options );
    }

    /**
     * Reset the default query options
     */
    public void resetOptions(){
        _netOptions.reset();
    }

    /**
     * Gets the default query options
     *
     * @return an int representing the options to be used by queries
     */
    public int getOptions(){
        return _netOptions.get();
    }

    /**
     * Helper method for setting up MongoOptions at instantiation so that any options which affect this connection can be set.
     */
    @SuppressWarnings("deprecation")
    void _applyMongoOptions() {
        if (_options.slaveOk) {
            slaveOk();
        }
        if (_options.getReadPreference() != null) {
           setReadPreference(_options.getReadPreference());
        }
        setWriteConcern(_options.getWriteConcern());
    }

    /**
     * Returns the mongo options.
     *
     * @return A {@link com.mongodb.MongoOptions} containing the settings for this MongoDB instance.
     * @deprecated Please use {@link MongoClient} and corresponding {@link com.mongodb.MongoClient#getMongoClientOptions()}
     */
    @Deprecated
    public MongoOptions getMongoOptions() {
        return _options;
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server. Note that this value may change over time depending
     * on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException
     */
    public int getMaxBsonObjectSize() {
        return _connector.getMaxBsonObjectSize();
    }

    boolean isMongosConnection() {
        return _connector.isMongosConnection();
    }

    private static MongoAuthority getMongoAuthorityFromURI(final MongoURI uri) throws UnknownHostException {
        if ( uri.getHosts().size() == 1 ){
            return MongoAuthority.direct(new ServerAddress(uri.getHosts().get(0)), uri.getCredentials());
        }
        else {
            List<ServerAddress> replicaSetSeeds = new ArrayList<ServerAddress>(uri.getHosts().size());
            for ( String host : uri.getHosts() )
                replicaSetSeeds.add( new ServerAddress( host ) );
            return MongoAuthority.dynamicSet(replicaSetSeeds, uri.getCredentials());
        }
    }

    final MongoOptions _options;
    final DBTCPConnector _connector;
    final ConcurrentMap<String,DB> _dbs = new ConcurrentHashMap<String,DB>();
    private WriteConcern _concern = WriteConcern.NORMAL;
    private ReadPreference _readPref = ReadPreference.primary();
    final Bytes.OptionHolder _netOptions = new Bytes.OptionHolder( null );
    final CursorCleanerThread _cleaner;
    final MongoAuthority _authority;


    org.bson.util.SimplePool<PoolOutputBuffer> _bufferPool =
        new org.bson.util.SimplePool<PoolOutputBuffer>( 1000 ){

        protected PoolOutputBuffer createNew(){
            return new PoolOutputBuffer();
        }

    };

    /**
     * Forces the master server to fsync the RAM data to disk This is done automatically by the server at intervals, but can be forced for
     * better reliability.
     *
     * @param async if true, the fsync will be done asynchronously on the server.
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public CommandResult fsync(boolean async) {
        DBObject cmd = new BasicDBObject("fsync", 1);
        if (async) {
            cmd.put("async", 1);
        }
        CommandResult result = getDB(ADMIN_DATABASE_NAME).command(cmd);
        result.throwOnError();
        return result;
    }

    /**
     * Forces the master server to fsync the RAM data to disk, then lock all writes. The database will be read-only after this command
     * returns.
     *
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public CommandResult fsyncAndLock() {
        DBObject cmd = new BasicDBObject("fsync", 1);
        cmd.put("lock", 1);
        CommandResult result = getDB(ADMIN_DATABASE_NAME).command(cmd);
        result.throwOnError();
        return result;
    }

    /**
     * Unlocks the database, allowing the write operations to go through. This command may be asynchronous on the server, which means there
     * may be a small delay before the database becomes writable.
     *
     * @return {@code DBObject} in the following form {@code {"ok": 1,"info": "unlock completed"}}
     * @throws MongoException
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public DBObject unlock() {
        DB db = getDB(ADMIN_DATABASE_NAME);
        DBCollection col = db.getCollection("$cmd.sys.unlock");
        return col.findOne();
    }

    /**
     * Returns true if the database is locked (read-only), false otherwise.
     *
     * @return result of the command execution
     * @throws MongoException
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public boolean isLocked() {
        DB db = getDB(ADMIN_DATABASE_NAME);
        DBCollection col = db.getCollection("$cmd.sys.inprog");
        BasicDBObject res = (BasicDBObject) col.findOne();
        if (res.containsField("fsyncLock")) {
            return res.getInt("fsyncLock") == 1;
        }
        return false;
    }

    // -------

    /**
     * Mongo.Holder can be used as a static place to hold several instances of Mongo. Security is not enforced at this level, and needs to
     * be done on the application side.
     */
    public static class Holder {

        /**
         * Attempts to find an existing MongoClient instance matching that URI in the holder, and returns it if exists.
         * Otherwise creates a new Mongo instance based on this URI and adds it to the holder.
         *
         * @param uri the Mongo URI
         * @return the client
         * @throws MongoException
         * @throws UnknownHostException
         *
         * @deprecated Please use {@link #connect(MongoClientURI)} instead.
         */
        @Deprecated
        public Mongo connect(final MongoURI uri) throws UnknownHostException {
            return connect(uri.toClientURI());
        }

        /**
         * Attempts to find an existing MongoClient instance matching that URI in the holder, and returns it if exists.
         * Otherwise creates a new Mongo instance based on this URI and adds it to the holder.
         *
         * @param uri the Mongo URI
         * @return the client
         * @throws MongoException
         * @throws UnknownHostException
         */
        public Mongo connect(final MongoClientURI uri) throws UnknownHostException {

            final String key = toKey(uri);

            Mongo client = _mongos.get(key);

            if (client == null) {
                final Mongo newbie = new MongoClient(uri);
                client = _mongos.putIfAbsent(key, newbie);
                if (client == null) {
                    client = newbie;
                } else {
                    newbie.close();
                }
            }

            return client;
        }

        private String toKey(final MongoClientURI uri) {
            return uri.toString();
        }

        public static Holder singleton() { return _default; }

        private static Holder _default = new Holder();
        private final ConcurrentMap<String,Mongo> _mongos = new ConcurrentHashMap<String,Mongo>();

    }

    class CursorCleanerThread extends Thread {

        CursorCleanerThread() {
            setDaemon(true);
            setName("MongoCleaner" + hashCode());
        }

        public void run() {
            while (_connector.isOpen()) {
                try {
                    try {
                        Thread.sleep(cleanerIntervalMS);
                    } catch (InterruptedException e) {
                        //caused by the Mongo instance being closed -- proceed with cleanup
                    }
                    for (DB db : _dbs.values()) {
                        db.cleanCursors(true);
                    }
                } catch (Throwable t) {
                    // thread must never die
                }
            }
        }
    }

    @Override
    public String toString() {
        return "Mongo{" +
                "authority=" + _authority +
                ", options=" + _options +
                '}';
    }

    /**
     * Gets the authority, which includes the connection type, the server address(es), and the credentials.
     * 
     * @return the authority
     */
    MongoAuthority getAuthority() {
        return _authority;
    }
}
