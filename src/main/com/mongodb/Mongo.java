// Mongo.java

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

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.bson.io.PoolOutputBuffer;

/**
 * A database connection with internal pooling.
 * For most application, you should have 1 Mongo instance for the entire JVM.
 *
 * The following are equivalent, and all connect to the
 * local database running on the default port:
 *
 * <blockquote><pre>
 * Mongo mongo1 = new Mongo( "127.0.0.1" );
 * Mongo mongo2 = new Mongo( "127.0.0.1", 27017 );
 * Mongo mongo3 = new Mongo( new DBAddress( "127.0.0.1", 27017, "test" ) );
 * Mongo mongo4 = new Mongo( new ServerAddress( "127.0.0.1") );
 * </pre></blockquote>
 *
 * Mongo instances have connection pooling built in - see the requestStart
 * and requestDone methods for more information.
 * http://www.mongodb.org/display/DOCS/Java+Driver+Concurrency
 *
 * <h3>Connecting to a Replica Pair</h3>
 * <p>
 * You can connect to a
 * <a href="http://www.mongodb.org/display/DOCS/Replica+Pairs">replica pair</a>
 * using the Java driver by passing two DBAddresses to the Mongo constructor.
 * For example:
 * </p>
 * <blockquote><pre>
 * DBAddress left = new DBAddress("127.0.0.1:27017/test");
 * DBAddress right = new DBAddress("127.0.0.1:27018/test");
 *
 * Mongo mongo = new Mongo(left, right);
 * </pre></blockquote>
 *
 * <p>
 * If the master of a replica pair goes down, there will be a brief lag before
 * the slave becomes master.  Thus, your application should be prepared to catch
 * the exceptions that might be thrown in such a case: IllegalArgumentException,
 * MongoException, and MongoException.Network (depending on when the connection
 * drops).
 * </p>
 * <p>
 * Once the slave becomes master, the driver will begin using that connection
 * as the master connection and the exceptions will stop being thrown.
 * </p>
 *
 * <h3>Connecting to a Replica Set</h3>
 * <p>
 * You can connect to a
 * <a href="http://www.mongodb.org/display/DOCS/Replica+Sets">replica set</a>
 * using the Java driver by passing several a list if ServerAddress to the
 * Mongo constructor.
 * For example:
 * </p>
 * <blockquote><pre>
 * List<ServerAddress> addrs = new ArrayList<ServerAddress>();
 * addrs.add( new ServerAddress( "127.0.0.1" , 27017 ) );
 * addrs.add( new ServerAddress( "127.0.0.1" , 27018 ) );
 * addrs.add( new ServerAddress( "127.0.0.1" , 27019 ) );
 *
 * Mongo mongo = new Mongo( addrs );
 * </pre></blockquote>
 *
 * <p>
 * By default, all read and write operations will be made on the master.
 * But it's possible to read from the slave(s) by using slaveOk:
 * </p>
 * <blockquote><pre>
 * mongo.slaveOk();
 * </pre></blockquote>
 */
public class Mongo {

    /**
     *
     */
    public static final int MAJOR_VERSION = 2;
    /**
     *
     */
    public static final int MINOR_VERSION = 6;

    static int cleanerIntervalMS;
    static {
        cleanerIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.cleanerIntervalMS", "1000"));
    }

    /**
     * returns a database object
     * @param addr the database address
     * @return
     */
    public static DB connect( DBAddress addr ){
        return new Mongo( addr ).getDB( addr.getDBName() );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (localhost, default port)
     * @throws UnknownHostException
     * @throws MongoException
     */
    public Mongo()
        throws UnknownHostException , MongoException {
        this( new ServerAddress() );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     * @param host server to connect to
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public Mongo( String host )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host ) );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     * @param host server to connect to
     * @param options default query options
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public Mongo( String host , MongoOptions options )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host ) , options );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     * @param host the database's host address
     * @param port the port on which the database is running
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public Mongo( String host , int port )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host , port ) );
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     * @see com.mongodb.ServerAddress
     * @param addr the database address
     * @throws MongoException
     */
    public Mongo( ServerAddress addr )
        throws MongoException {
        this( addr , new MongoOptions() );
    }


    /**
     * Creates a Mongo instance based on a (single) mongo node using a given ServerAddress
     * @see com.mongodb.ServerAddress
     * @param addr the database address
     * @param options default query options
     * @throws MongoException
     */
    public Mongo( ServerAddress addr , MongoOptions options )
        throws MongoException {
        _addr = addr;
        _addrs = null;
        _options = options;
        _applyMongoOptions();
        _connector = new DBTCPConnector( this , _addr );
        _connector.start();
        _cleaner = new DBCleanerThread();
        _cleaner.start();
    }

    /**
     * <p>Creates a Mongo in paired mode. <br/> This will also work for
     * a replica set and will find all members (the master will be used by
     * default).</p>
     *
     * @see com.mongodb.ServerAddress
     * @param left left side of the pair
     * @param right right side of the pair
     * @throws MongoException
     */
    public Mongo( ServerAddress left , ServerAddress right )
        throws MongoException {
        this( left , right , new MongoOptions() );
    }

    /**
     * <p>Creates a Mongo connection in paired mode. <br/> This will also work for
     * a replica set and will find all members (the master will be used by
     * default).</p>
     *
     * @see com.mongodb.ServerAddress
     * @param left left side of the pair
     * @param right right side of the pair
     * @param options
     * @throws MongoException
     */
    public Mongo( ServerAddress left , ServerAddress right , MongoOptions options )
        throws MongoException {
        _addr = null;
        _addrs = Arrays.asList( left , right );
        _options = options;
        _applyMongoOptions();
        _connector = new DBTCPConnector( this , _addrs );
        _connector.start();

        _cleaner = new DBCleanerThread();
        _cleaner.start();
    }

    /**
     * <p>Creates a Mongo based on a replica set, or pair.
     * It will find all members (the master will be used by default).</p>
     * @see com.mongodb.ServerAddress
     * @param replicaSetSeeds Put as many servers as you can in the list and
     * the system will figure out the rest.
     * @throws MongoException
     */
    public Mongo( List<ServerAddress> replicaSetSeeds )
        throws MongoException {
        this( replicaSetSeeds , new MongoOptions() );
    }

    /**
     * <p>Creates a Mongo based on a replica set, or pair.
     * It will find all members (the master will be used by default).</p>
     * @see com.mongodb.ServerAddress
     * @param replicaSetSeeds put as many servers as you can in the list.
     *                       the system will figure the rest out
     * @param options default query options
     * @throws MongoException
     */
    public Mongo( List<ServerAddress> replicaSetSeeds , MongoOptions options )
        throws MongoException {

        _addr = null;
        _addrs = replicaSetSeeds;
        _options = options;
        _applyMongoOptions();
        _connector = new DBTCPConnector( this , _addrs );
        _connector.start();

        _cleaner = new DBCleanerThread();
        _cleaner.start();
    }

    /**
     * Creates a Mongo described by a URI.
     * If only one address is used it will only connect to that node, otherwise it will discover all nodes.
     * @param uri
     * @see MongoURI
     * <p>examples:
     *   <li>mongodb://127.0.0.1</li>
     *   <li>mongodb://fred:foobar@127.0.0.1/</li>
     *  </p>
     *  @throws MongoException
     * @throws UnknownHostException
     * @dochub connections
     */

    public Mongo( MongoURI uri )
        throws MongoException , UnknownHostException {

        _options = uri.getOptions();
        _applyMongoOptions();

        if ( uri.getHosts().size() == 1 ){
            _addr = new ServerAddress( uri.getHosts().get(0) );
            _addrs = null;
            _connector = new DBTCPConnector( this , _addr );
        }
        else {
            List<ServerAddress> replicaSetSeeds = new ArrayList<ServerAddress>( uri.getHosts().size() );
            for ( String host : uri.getHosts() )
                replicaSetSeeds.add( new ServerAddress( host ) );
            _addr = null;
            _addrs = replicaSetSeeds;
            _connector = new DBTCPConnector( this , replicaSetSeeds );
        }

        _connector.start();
        _cleaner = new DBCleanerThread();
        _cleaner.start();
    }

    /**
     * gets a database object
     * @param dbname the database name
     * @return
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
     * gets a collection of DBs used by the driver since this Mongo instance was created.
     * This may include DBs that exist in the client but not yet on the server.
     * @return
     */
    public Collection<DB> getUsedDatabases(){
        return _dbs.values();
    }

    /**
     * gets a list of all database names present on the server
     * @return
     * @throws MongoException
     */
    public List<String> getDatabaseNames()
        throws MongoException {

        BasicDBObject cmd = new BasicDBObject();
        cmd.put("listDatabases", 1);


        CommandResult res = getDB( "admin" ).command(cmd, getOptions());
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
     * @param dbName name of database to drop
     * @throws MongoException
     */
    public void dropDatabase(String dbName)
        throws MongoException {

        getDB( dbName ).dropDatabase();
    }

    /**
     * gets this driver version
     * @return
     */
    public String getVersion(){
        return MAJOR_VERSION + "." + MINOR_VERSION;
    }

    /**
     * returns a string representing the hosts used in this Mongo instance
     * @return
     */
    public String debugString(){
        return _connector.debugString();
    }

    /**
     * Gets the current master's hostname
     * @return
     */
    public String getConnectPoint(){
        return _connector.getConnectPoint();
    }

    /**
     * Gets the underlying TCP connector
     * @return
     */
    public DBTCPConnector getConnector() {
        return _connector;
    }

    /**
     * Gets the replica set status object
     * @return
     */
    public ReplicaSetStatus getReplicaSetStatus() {
        return _connector.getReplicaSetStatus();
    }

    /**
     * Gets the address of the current master
     * @return the address
     */
    public ServerAddress getAddress(){
        return _connector.getAddress();
    }

    /**
     * Gets a list of all server addresses used when this Mongo was created
     * @return
     */
    public List<ServerAddress> getAllAddress() {
        List<ServerAddress> result = _connector.getAllAddress();
        if (result == null) {
            return Arrays.asList(getAddress());
        }
        return result;
    }

    /**
     * Gets the list of server addresses currently seen by the connector.
     * This includes addresses auto-discovered from a replica set.
     * @return
     */
    public List<ServerAddress> getServerAddressList() {
        return _connector.getServerAddressList();
    }

    /**
     * closes the underlying connector, which in turn closes all open connections.
     * Once called, this Mongo instance can no longer be used.
     */
    public void close(){
        _connector.close();
        _cleaner.interrupt();
        try {
            _cleaner.join();
        } catch (InterruptedException e) {
            //end early
        }
    }

    /**
     * Sets the write concern for this database. Will be used as default for
     * writes to any collection in any database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    public void setWriteConcern( WriteConcern concern ){
        _concern = concern;
    }

    /**
     * Gets the default write concern
     * @return
     */
    public WriteConcern getWriteConcern(){
        return _concern;
    }

    /**
     * makes it possible to run read queries on slave nodes
     */
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * adds a default query option
     * @param option
     */
    public void addOption( int option ){
        _netOptions.add( option );
    }

    /**
     * sets the default query options
     * @param options
     */
    public void setOptions( int options ){
        _netOptions.set( options );
    }

    /**
     * reset the default query options
     */
    public void resetOptions(){
        _netOptions.reset();
    }

    /**
     * gets the default query options
     * @return
     */
    public int getOptions(){
        return _netOptions.get();
    }

    /**
     * Helper method for setting up MongoOptions at instantiation
     * so that any options which affect this connection can be set.
     */
    void _applyMongoOptions() {
        if (_options.slaveOk) slaveOk();
        setWriteConcern( _options.getWriteConcern() );
    }

    /**
     * Returns the mongo options.
     */
    public MongoOptions getMongoOptions() {
        return _options;
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * If the size is not known yet, a request may be sent to the master server
     * @return the maximum size
     */
    public int getMaxBsonObjectSize() {
        int maxsize = _connector.getMaxBsonObjectSize();
        if (maxsize == 0)
            maxsize = _connector.fetchMaxBsonObjectSize();
        return maxsize > 0 ? maxsize : Bytes.MAX_OBJECT_SIZE;
    }

    final ServerAddress _addr;
    final List<ServerAddress> _addrs;
    final MongoOptions _options;
    final DBTCPConnector _connector;
    final ConcurrentMap<String,DB> _dbs = new ConcurrentHashMap<String,DB>();
    private WriteConcern _concern = WriteConcern.NORMAL;
    final Bytes.OptionHolder _netOptions = new Bytes.OptionHolder( null );
    final DBCleanerThread _cleaner;

    org.bson.util.SimplePool<PoolOutputBuffer> _bufferPool =
        new org.bson.util.SimplePool<PoolOutputBuffer>( 1000 ){

        protected PoolOutputBuffer createNew(){
            return new PoolOutputBuffer();
        }

    };

    /**
     * Forces the master server to fsync the RAM data to disk
     * This is done automatically by the server at intervals, but can be forced for better reliability. 
     * @param async if true, the fsync will be done asynchronously on the server.
     * @return 
     */
    public CommandResult fsync(boolean async) {
        DBObject cmd = new BasicDBObject("fsync", 1);
        if (async) {
            cmd.put("async", 1);
        }
        return getDB("admin").command(cmd);
    }

    /**
     * Forces the master server to fsync the RAM data to disk, then lock all writes.
     * The database will be read-only after this command returns.
     * @return 
     */
    public CommandResult fsyncAndLock() {
        DBObject cmd = new BasicDBObject("fsync", 1);
        cmd.put("lock", 1);
        return getDB("admin").command(cmd);
    }

    /**
     * Unlocks the database, allowing the write operations to go through.
     * This command may be asynchronous on the server, which means there may be a small delay before the database becomes writable.
     * @return 
     */
    public DBObject unlock() {
        DB db = getDB("admin");
        DBCollection col = db.getCollection("$cmd.sys.unlock");
        return col.findOne();
    }

    /**
     * Returns true if the database is locked (read-only), false otherwise.
     * @return 
     */
    public boolean isLocked() {
        DB db = getDB("admin");
        DBCollection col = db.getCollection("$cmd.sys.inprog");
        BasicDBObject res = (BasicDBObject) col.findOne();
        if (res.containsField("fsyncLock")) {
            return res.getInt("fsyncLock") == 1;
        }
        return false;
    }

    // -------


    /**
     * Mongo.Holder can be used as a static place to hold several instances of Mongo.
     * Security is not enforced at this level, and needs to be done on the application side.
     */
    public static class Holder {

        /**
         * Attempts to find an existing Mongo instance matching that URI in the holder, and returns it if exists.
         * Otherwise creates a new Mongo instance based on this URI and adds it to the holder.
         * @param uri the Mongo URI
         * @return
         * @throws MongoException
         * @throws UnknownHostException
         */
        public Mongo connect( MongoURI uri )
            throws MongoException , UnknownHostException {

            String key = _toKey( uri );

            Mongo m = _mongos.get(key);
            if ( m != null )
                return m;

            m = new Mongo( uri );

            Mongo temp = _mongos.putIfAbsent( key , m );
            if ( temp == null ){
                // ours got in
                return m;
            }

            // there was a race and we lost
            // close ours and return the other one
            m.close();
            return temp;
        }

        String _toKey( MongoURI uri ){
            StringBuilder buf = new StringBuilder();
            for ( String h : uri.getHosts() )
                buf.append( h ).append( "," );
            buf.append( uri.getOptions() );
            buf.append( uri.getUsername() );
            return buf.toString();
        }
        
        public static Holder singleton() { return _default; }

        private static Holder _default = new Holder();
        private final ConcurrentMap<String,Mongo> _mongos = new ConcurrentHashMap<String,Mongo>();

    }

    class DBCleanerThread extends Thread {

        DBCleanerThread() {
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
        String str = "Mongo: ";
        List<ServerAddress> list = getServerAddressList();
        if (list == null || list.isEmpty())
            str += "null";
        else {
            for (ServerAddress addr : list) {
                str += addr.toString() + ",";
            }
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
}
