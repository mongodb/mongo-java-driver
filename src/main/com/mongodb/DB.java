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

import java.io.*;
import java.util.*;

import com.mongodb.util.*;

/**
 * a logical database on a server
 * @dochub databases
 */
public abstract class DB {

    public DB( Mongo mongo , String name ){
        _mongo = mongo;
    	_name = name;
        _options = new Bytes.OptionHolder( _mongo._netOptions );
    }

    public abstract void requestStart();
    public abstract void requestDone();
    public abstract void requestEnsureConnection();
    
    /** Returns the collection represented by the string &lt;dbName&gt;.&lt;collectionName&gt;.
     * @param name the name of the collection
     * @return the collection
     */
    protected abstract DBCollection doGetCollection( String name );
    
    /** Gets a collection with a given name.
     * If the collection does not exist, a new collection is created.
     * @param name the name of the collection to return
     * @return the collection
     */
    public final DBCollection getCollection( String name ){
        DBCollection c = doGetCollection( name );
        if ( c != null ){
            _seenCollections.add( c );
	}
        return c;
    }

    /** Creates a collection with a given name and options.
     * If the collection does not exist, a new collection is created.
     * Possible options:
     * <dl>
     * <dt>capped</dt><dd><i>boolean</i>: if the collection is capped</dd>
     * <dt>size</dt><dd><i>int</i>: collection size</dd>
     * <dt>max</dt><dd><i>int</i>: max number of documents</dd>
     * </dl>
     * @param name the name of the collection to return
     * @param o options
     * @return the collection
     */
    public final DBCollection createCollection( String name, DBObject o ){
        if ( o != null ){
            DBObject createCmd = new BasicDBObject("create", name);
            createCmd.putAll(o);
            CommandResult result = command(createCmd);
            result.throwOnError();
        }
        return getCollection(name);
    }

    
    /** Returns a collection matching a given string.
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

    /** Execute a database command directly.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @return the result of the command from the database
     * @dochub commands
     */
    public CommandResult command( DBObject cmd )
        throws MongoException {
        return command( cmd , 0 );
    }

    /** Execute a database command directly.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
     * @return the result of the command from the database
     * @dochub commands
     */
    public CommandResult command( DBObject cmd , int options )
        throws MongoException {
        
        Iterator<DBObject> i = getCollection( "$cmd" ).__find( cmd , new BasicDBObject() , 0 , -1 , options );
        if ( i == null || ! i.hasNext() )
            return null;
        
        CommandResult res = (CommandResult)i.next();
        res._cmd = cmd;
        return res;
    }

    public CommandResult command( String cmd )
        throws MongoException {
        return command( new BasicDBObject( cmd , Boolean.TRUE ) );
    }

    public CommandResult doEval( String code , Object ... args )
        throws MongoException {

        return command( BasicDBObjectBuilder.start()
                        .add( "$eval" , code )
                        .add( "args" , args )
                        .get() );
    }

    public Object eval( String code , Object ... args )
        throws MongoException {
        
        CommandResult res = doEval( code , args );
        
        if ( res.ok() ){
            return res.get( "retval" );
        }
        
        throw new MongoException( "eval failed: " + res );
    }

    public CommandResult getStats() {
        return command("dbstats");
    }

    /** Returns the name of this database.
     * @return the name
     */
    public String getName(){
	return _name;
    }

    /** Makes this database read-only
     * @param b if the database should be read-only
     */
    public void setReadOnly( Boolean b ){
        _readOnly = b;
    }

    /** Returns a set of the names of collections in this database.
     * @return the names of collections in this database
     */
    public Set<String> getCollectionNames()
        throws MongoException {

        DBCollection namespaces = getCollection("system.namespaces");
        if (namespaces == null)
            throw new RuntimeException("this is impossible");

        Iterator<DBObject> i = namespaces.__find(new BasicDBObject(), null, 0, 0, getOptions());
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
     */
    public boolean collectionExists(String collectionName)
    {
        if (collectionName == null || "".equals(collectionName))
            return false;

        Set<String> collections = getCollectionNames();
        if (collections.size() == 0)
            return false;

        for (String collection : collections)
        {
            if (collectionName.equalsIgnoreCase(collection))
                return true;
        }

        return false;
    }


    /** Returns the name of this database.
     * @return the name
     */
    public String toString(){
        return _name;
    }

    /** Clears any indices that have not yet been applied to 
     * the collections in this database.
     */
    public void resetIndexCache(){
        for ( DBCollection c : _seenCollections )
            c.resetIndexCache();
    }

    /**
     *  Gets the the error (if there is one) from the previous operation.  The result of
     *  this command will look like
     *
     * <pre>
     * { "err" :  errorMessage  , "ok" : 1.0 }
     * </pre>
     *
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     *
     * Care must be taken to ensure that calls to getLastError go to the same connection as that
     * of the previous operation. See com.mongodb.Mongo.requestStart for more information.
     *
     *  @return DBObject with error and status information
     */
    public CommandResult getLastError()
        throws MongoException {
        return command(new BasicDBObject("getlasterror", 1));
    }

    public CommandResult getLastError( com.mongodb.WriteConcern concern )
        throws MongoException {
        return command( concern.getCommand() );
    }

    public CommandResult getLastError( int w , int wtimeout , boolean fsync )
        throws MongoException {
        return command( (new com.mongodb.WriteConcern( w, wtimeout , fsync )).getCommand() );
    }


    /**
     * Set the write concern for this database. Will be used for
     * writes to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    public void setWriteConcern( com.mongodb.WriteConcern concern ){
	if (concern == null) throw new IllegalArgumentException();
        _concern = concern;
    }

    /**
     * Get the write concern for this database.
     */
    public com.mongodb.WriteConcern getWriteConcern(){
        if ( _concern != null )
            return _concern;
        return _mongo.getWriteConcern();
    }

    /**
     *  Drops this database.  Removes all data on disk.  Use with caution.
     */
    public void dropDatabase()
        throws MongoException {

        CommandResult res = command(new BasicDBObject("dropDatabase", 1));
        res.throwOnError();

    }

    /**
     * Returns true iff this DB is authenticated
     *
     * @return true if authenticated, false otherwise
     * @dochub authenticate
     */
    public boolean isAuthenticated() {
	return ( _username != null );
    }

    /**
     *  Authenticates connection/db with given name and password
     *
     * @param username  name of user for this database
     * @param passwd password of user for this database
     * @return true if authenticated, false otherwise
     * @dochub authenticate
     */
    public boolean authenticate(String username, char[] passwd )
        throws MongoException {
        
        if ( username == null )
            throw new NullPointerException( "username can't be null" );
        
        if ( _username != null )
	    throw new IllegalStateException( "can't call authenticate twice on the same DBObject" );
        
        String hash = _hash( username , passwd );
        if ( ! _doauth( username , hash.getBytes() ) )
            return false;
        _username = username;
        _authhash = hash.getBytes();
        return true;
    }
    /*
    boolean reauth(){
        if ( _username == null || _authhash == null )
            throw new IllegalStateException( "no auth info!" );
        return _doauth( _username , _authhash );
    }
    */

    DBObject _authCommand( String nonce ){
        if ( _username == null || _authhash == null )
            throw new IllegalStateException( "no auth info!" );

        return _authCommand( nonce , _username , _authhash );
    }

    static DBObject _authCommand( String nonce , String username , byte[] hash ){
        String key = nonce + username + new String( hash );
        
        BasicDBObject cmd = new BasicDBObject();

        cmd.put("authenticate", 1);
        cmd.put("user", username);
        cmd.put("nonce", nonce);
        cmd.put("key", Util.hexMD5(key.getBytes()));
        
        return cmd;
    }

    private boolean _doauth( String username , byte[] hash ){
        CommandResult res = command(new BasicDBObject("getnonce", 1));

        if ( ! res.ok() ){
            throw new MongoException("Error - unable to get nonce value for authentication.");
        }

        DBObject cmd = _authCommand( res.getString( "nonce" ) , username , hash );

        res = command(cmd);
        
        return res.ok();
    }

    public void addUser( String username , char[] passwd ){
        DBCollection c = getCollection( "system.users" );
        DBObject o = c.findOne( new BasicDBObject( "user" , username ) );
        if ( o == null )
            o = new BasicDBObject( "user" , username );
        o.put( "pwd" , _hash( username , passwd ) );
        c.save( o );
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
     * The value for errormMessage will be null of no error has ocurred, or the message.  The value of
     * countOpsBack will be the number of operations since the error occurred.
     *
     * Care must be taken to ensure that calls to getPreviousError go to the same connection as that
     * of the previous operation. See com.mongodb.Mongo.requestStart for more information.
     *
     * @return DBObject with error and status information
     */
    public CommandResult getPreviousError()
        throws MongoException {
        return command(new BasicDBObject("getpreverror", 1));
    }

    /**
     *  Resets the error memory for this database.  Used to clear all errors such that getPreviousError()
     *  will return no error.
     */
    public void resetError()
        throws MongoException {
        command(new BasicDBObject("reseterror", 1));
    }

    /**
     *  For testing purposes only - this method forces an error to help test error handling
     */
    public void forceError()
        throws MongoException {
        command(new BasicDBObject("forceerror", 1));
    }

    public Mongo getMongo(){
        return _mongo;
    }

    public DB getSisterDB( String name ){
        return _mongo.getDB( name );
    }

    /**
     * makes this query ok to run on a slave node
     */
    public void slaveOk(){
        addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    public void addOption( int option ){
        _options.add( option );
    }

    public void setOptions( int options ){
        _options.set( options );
    }

    public void resetOptions(){
        _options.reset();
    }
   
    public int getOptions(){
        return _options.get();
    }

    final Mongo _mongo;
    final String _name;
    final Set<DBCollection> _seenCollections = Collections.synchronizedSet( new HashSet<DBCollection>() );

    protected boolean _readOnly = false;
    private com.mongodb.WriteConcern _concern;
    final Bytes.OptionHolder _options;

    String _username;
    byte[] _authhash = null;

}
