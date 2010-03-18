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

    public static enum WriteConcern { NONE, NORMAL, STRICT };

    public DB( String name ){
    	_name = name;
    }

    public abstract void requestStart();
    public abstract void requestDone();
    public abstract void requestEnsureConnection();
    
    /** Returns the collection represented by the string &lt;dbName&gt;.&lt;collectionName&gt;.
     * @param fullNameSpace the string
     * @return the collection
     */
    public abstract DBCollection getCollectionFromFull( String fullNameSpace );
    protected abstract DBCollection doGetCollection( String name );
    public abstract DB getSisterDB( String dbName );
    
    
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
            DBObject result = command(createCmd);
            if ( ((Number)(result.get( "ok" ))).intValue() != 1 ) {
                throw new MongoException( "getCollection failed: " + result.toString() );
            }
        }
        return getCollection(name);
    }

    
    /** Returns a collection matching a given string.
     * @param s the name of the collection
     * @return the collection
     */
    public DBCollection getCollectionFromString( String s ){
        DBCollection foo = null;
        
        while ( s.contains( "." ) ){
            int idx = s.indexOf( "." );
            String b = s.substring( 0 , idx );
            s = s.substring( idx + 1 );
            foo = getCollection( b );
        }

        if ( foo != null )
            return foo.getCollection( s );
        return getCollection( s );
    }

    /** Execute a database command directly.
     * @see <a href="http://mongodb.onconfluence.com/display/DOCS/Mongo+Commands">Mongo Commands</a>
     * @return the result of the command from the database
     * @dochub commands
     */
    public DBObject command( DBObject cmd )
        throws MongoException {
        return getCollection( "$cmd" ).findOne( cmd );
    }

    public DBObject command( String cmd )
        throws MongoException {
        return command( new BasicDBObject( cmd , Boolean.TRUE ) );
    }

    public DBObject doEval( String code , Object ... args )
        throws MongoException {

        return command( BasicDBObjectBuilder.start()
                        .add( "$eval" , code )
                        .add( "args" , args )
                        .get() );
    }

    public Object eval( String code , Object ... args )
        throws MongoException {
        
        DBObject res = doEval( code , args );
        
        if ( res.get( "ok" ) instanceof Number && 
             ((Number)res.get( "ok" ) ).intValue() == 1 ){
            return res.get( "retval" );
        }
        
        throw new MongoException( "eval failed: " + res );
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

        Iterator<DBObject> i = namespaces.find(new BasicDBObject(), null, 0, 0, 0);
        if (i == null)
            return new HashSet<String>();

        List<String> tables = new ArrayList<String>();

        for (; i.hasNext();) {
            DBObject o = i.next();
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

        return new OrderedSet<String>(tables);
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
     *  <pre>
     * { "err" :  errorMessage  , "ok" : 1.0 , "_ns" : "$cmd"}
     * </pre>
     *
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     *
     * Care must be taken to ensure that calls to getLastError go to the same connection as that
     * of the previous operation. See com.mongodb.Mongo.requestStart for more information.
     *
     *  @return DBObject with error and status information
     */
    public DBObject getLastError()
        throws MongoException {
        return command(new BasicDBObject("getlasterror", 1));
    }
    
    public void setWriteConcern( WriteConcern concern ){
        _concern = concern;
    }

    public WriteConcern getWriteConcern(){
        return _concern;
    }

    /**
     *  Drops this database.  Removes all data on disk.  Use with caution.
     */
    public void dropDatabase()
        throws MongoException {

        BasicDBObject res = (BasicDBObject) command(new BasicDBObject("dropDatabase", 1));

        if (res.getInt("ok") != 1) {
            throw new RuntimeException("Error - unable to drop database : " + res.toString());
        }
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

    boolean reauth(){
        if ( _username == null || _authhash == null )
            throw new IllegalStateException( "no auth info!" );
        return _doauth( _username , _authhash );
    }

    private boolean _doauth( String username , byte[] hash ){
        BasicDBObject res = (BasicDBObject) command(new BasicDBObject("getnonce", 1));

        if (res.getInt("ok") != 1) {
            throw new MongoException("Error - unable to get nonce value for authentication.");
        }

        String nonce = res.getString("nonce");
        String key = nonce + username + new String( hash );
        
        BasicDBObject cmd = new BasicDBObject();

        cmd.put("authenticate", 1);
        cmd.put("user", username);
        cmd.put("nonce", nonce);
        cmd.put("key", Util.hexMD5(key.getBytes()));

        res = (BasicDBObject)command(cmd);
        
        return res.getInt("ok") == 1;
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
    public DBObject getPreviousError()
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

    final String _name;
    final Set<DBCollection> _seenCollections = new HashSet<DBCollection>();

    protected boolean _readOnly = false;
    private WriteConcern _concern = WriteConcern.NORMAL;

    String _username;
    byte[] _authhash = null;

}
