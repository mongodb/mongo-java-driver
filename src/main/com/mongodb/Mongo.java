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

import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.bson.io.*;

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
 * DBAddress left = new DBAddress("localhost:27017/test");
 * DBAddress right = new DBAddress("localhost:27018/test");
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
 * addrs.add( new ServerAddress( "localhost" , 27017 ) );
 * addrs.add( new ServerAddress( "localhost" , 27018 ) );
 * addrs.add( new ServerAddress( "localhost" , 27019 ) );
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

    public static final int MAJOR_VERSION = 2;
    public static final int MINOR_VERSION = 4;

    public static DB connect( DBAddress addr ){
        return new Mongo( addr ).getDB( addr.getDBName() );
    }

    public Mongo()
        throws UnknownHostException , MongoException {
        this( new ServerAddress() );
    }

    /**
     * Connects to a (single) mongodb node (default port)
     *
     *  @param host server to connect to
     *  @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host ) );
    }

    /**
     * Connects to a (single) mongodb node (default port)
     * @param host server to connect to
     * @param options options to use
     * @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host , MongoOptions options )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host ) , options );
    }

    /**
     * Connects to a (single) mongodb node
     * @param host the database's host address
     * @param port the port on which the database is running
     * @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host , int port )
        throws UnknownHostException , MongoException {
        this( new ServerAddress( host , port ) );
    }

    /**
     * Connects to a (single) mongodb node
     * @see com.mongodb.ServerAddress
     * @param addr the database address
     */
    public Mongo( ServerAddress addr )
        throws MongoException {
        this( addr , new MongoOptions() );
    }


    /**
     * Connects to a (single) mongo node using a given ServerAddress
     * @see com.mongodb.ServerAddress
     * @param addr the database address
     */
    public Mongo( ServerAddress addr , MongoOptions options )
        throws MongoException {
        _addr = addr;
        _addrs = null;
        _options = options;
        _connector = new DBTCPConnector( this , _addr );
        _connector.checkMaster( true , true );
        _connector.testMaster();
    }

    /**
     * <p>Creates a Mongo connection in paired mode. <br/> This will also work for
     * a replica set and will find all members (the master will be used by
     * default).</p>
     * 
     * @see com.mongodb.ServerAddress
     * @param left left side of the pair
     * @param right right side of the pair
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
     */
    public Mongo( ServerAddress left , ServerAddress right , MongoOptions options )
        throws MongoException {
        _addr = null;
        _addrs = Arrays.asList( left , right );
        _options = options;
        _connector = new DBTCPConnector( this , _addrs );
        _connector.checkMaster( true , false );
        _connector.testMaster();
    }

    /**
     * <p>Creates a Mongo connection. <br/> This will work for
     * a replica set, or pair, and will find all members (the master will be used by
     * default).</p>
     * 
     * @see com.mongodb.ServerAddress
     * @param replicaSetSeeds Put as many servers as you can in the list and
     * the system will figure out the rest.
     */
    public Mongo( List<ServerAddress> replicaSetSeeds )
        throws MongoException {
        this( replicaSetSeeds , new MongoOptions() );
    }

    /**
     * <p>Creates a Mongo connection. <br/> This will work for
     * a replica set, or pair, and will find all members (the master will be used by
     * default).</p>
     * 
     * @see com.mongodb.ServerAddress
     * @param replicaSetSeeds put as many servers as you can in the list. 
     *                       the system will figure the rest out
     */    
    public Mongo( List<ServerAddress> replicaSetSeeds , MongoOptions options )
        throws MongoException {
        
        _addr = null;
        _addrs = replicaSetSeeds;
        _options = options;
        _connector = new DBTCPConnector( this , _addrs );
        
        _connector.checkMaster( true , false );
    }
    /**
     * Creates a Mongo connection. If only one address is used it will only connect to that node, otherwise it will discover all nodes.
     * @see MongoURI
     * <p>examples:
     *   <li>mongodb://localhost</li>
     *   <li>mongodb://fred:foobar@localhost/</li>
     *  </p>
     *  @dochub connections
     */    

    public Mongo( MongoURI uri )
        throws MongoException , UnknownHostException {

        _options = uri.getOptions();
        
        if ( uri.getHosts().size() == 1 ){
            _addr = new ServerAddress( uri.getHosts().get(0) );
            _addrs = null;
            _connector = new DBTCPConnector( this , _addr );
            _connector.testMaster();
        }
        else {
            List<ServerAddress> replicaSetSeeds = new ArrayList<ServerAddress>( uri.getHosts().size() );
            for ( String host : uri.getHosts() )
                replicaSetSeeds.add( new ServerAddress( host ) );
            _addr = null;
            _addrs = replicaSetSeeds;
            _connector = new DBTCPConnector( this , replicaSetSeeds );
            _connector.checkMaster( true , true );
        }

    }

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
    
    public List<String> getDatabaseNames()
        throws MongoException {

        BasicDBObject cmd = new BasicDBObject();
        cmd.put("listDatabases", 1);
        

        BasicDBObject res = (BasicDBObject)getDB( "admin" ).command(cmd);

        if (res.getInt("ok" , 0 ) != 1 )
            throw new MongoException( "error listing databases : " + res );

        List l = (List)res.get("databases");

        List<String> list = new ArrayList<String>();

        for (Object o : l) {
            list.add(((BasicDBObject)o).getString("name"));
        }
        return list;
    }


    /**
     *  Drops the database if it exists.
     *
     * @param dbName name of database to drop
     */
    public void dropDatabase(String dbName)
        throws MongoException {
        
        getDB( dbName ).dropDatabase();
    }

    public String getVersion(){
        return MAJOR_VERSION + "." + MINOR_VERSION;
    }

    public String debugString(){
        return _connector.debugString();
    }

    public String getConnectPoint(){
        return _connector.getConnectPoint();
    }

    /** Gets the address of this database.
     * @return the address
     */
    public ServerAddress getAddress(){
        return _connector.getAddress();
    }

    public List<ServerAddress> getAllAddress() {
        List<ServerAddress> result = _connector.getAllAddress();
        if (result == null) {
            return Arrays.asList(getAddress());
        }
        return result;
    }

    /**
     * closes all open connections
     * this Mongo cannot be re-used
     */
    public void close(){
        _connector.close();
    }

    /**
     * Set the write concern for this database. Will be used for
     * writes to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    public void setWriteConcern( WriteConcern concern ){
        _concern = concern;
    }

    /**
     * Get the write concern for this database.
     */
    public WriteConcern getWriteConcern(){
        return _concern;
    }

    /**
     * makes this query ok to run on a slave node
     */
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    public void addOption( int option ){
        _netOptions.add( option );
    }

    public void setOptions( int options ){
        _netOptions.set( options );
    }

    public void resetOptions(){
        _netOptions.reset();
    }
   
    public int getOptions(){
        return _netOptions.get();
    }

    
    final ServerAddress _addr;
    final List<ServerAddress> _addrs;
    final MongoOptions _options;
    final DBTCPConnector _connector;
    final ConcurrentMap<String,DB> _dbs = new ConcurrentHashMap<String,DB>();
    private WriteConcern _concern = WriteConcern.NORMAL;
    final Bytes.OptionHolder _netOptions = new Bytes.OptionHolder( null );
    
    org.bson.util.SimplePool<PoolOutputBuffer> _bufferPool = 
        new org.bson.util.SimplePool<PoolOutputBuffer>( 1000 ){
        
        protected PoolOutputBuffer createNew(){
            return new PoolOutputBuffer();
        }
        
    };


    // -------   

    
    /**
     * Mongo.Holder is if you want to have a static place to hold instances of Mongo
     * security is not enforced at this level, so need to do on your side
     */
    public static class Holder {
        
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

        
        private static final ConcurrentMap<String,Mongo> _mongos = new ConcurrentHashMap<String,Mongo>();
        
    }

}
