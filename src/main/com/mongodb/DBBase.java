// DBBase.java

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

import java.util.*;

public abstract class DBBase {

    static enum WriteConcern { NONE, NORMAL, STRICT };

    public DBBase( String name ){
    	_name = name;
    }

    public void requestStart(){}
    public void requestDone(){}
    public void requestEnsureConnection(){}
    
    /** Returns the collection represented by the string &lt;dbName&gt;.&lt;collectionName&gt;.
     * @param fullNameSpace the string
     * @return the collection
     */
    public abstract DBCollection getCollectionFromFull( String fullNameSpace );
    protected abstract DBCollection doGetCollection( String name );
    public abstract Set<String> getCollectionNames() throws MongoException ;
    
    /** Gets the address of this database.
     * @return the address
     */
    public abstract DBAddress getAddress();
    public abstract String getConnectPoint();
    
    /** Gets a collection with a given name.
     * If the collection does not exist, a new collection is created.
     * @param name the name of the collection to return
     * @return the collection
     */
    public final DBCollection getCollection( String name ){
        return getCollection(name, null);
    }

    /** Gets a collection with a given name and options.
     * If the collection does not exist, a new collection is created.
     * Possible options:
     * <dl>
     * <dt>capped</dt><dd><i>boolean</i>: if the collection is capped</dd>
     * <dt>size</dt><dd><i>int</i>: collection size</dd>
     * <dt>max</dt><dd><i>int</i>: max number of documents</dd>
     * </dl>
     * @param name the name of the collection to return
     * @param collection options
     * @return the collection
     */
    public final DBCollection getCollection( String name, DBObject o ){
        if ( o != null ){
            DBObject createCmd = new BasicDBObject("create", name);
            createCmd.putAll(o);
            DBObject result = command(createCmd);
            if ( ((Number)(result.get( "ok" ))).intValue() != 1 ) {
                throw new MongoException( "getCollection failed: " + result.toString() );
            }
        }

        DBCollection c = doGetCollection( name );
        if ( c != null ){
            _seenCollections.add( c );
	}
        return c;
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
     */
    public DBObject command( DBObject cmd )
        throws MongoException {
        return getCollection( "$cmd" ).findOne( cmd );
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
    
    public void setWriteConcern( DBBase.WriteConcern concern ){
        _concern = concern;
    }

    public DBBase.WriteConcern getWriteConcern(){
        return _concern;
    }

    
    final String _name;
    final Set<DBCollection> _seenCollections = new HashSet<DBCollection>();

    protected boolean _readOnly = false;
    private DBBase.WriteConcern _concern = DBBase.WriteConcern.NORMAL;

}
