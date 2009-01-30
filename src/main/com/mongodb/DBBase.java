// DBBase.java

package com.mongodb;

import java.util.*;

public abstract class DBBase {

    public DBBase( String name ){
    	_name = name;
    }

    public void requestStart(){}
    public void requestDone(){}
    public void requestEnsureConnection(){}
    
    public abstract DBCollection getCollectionFromFull( String fullNameSpace );
    protected abstract DBCollection doGetCollection( String name );
    public abstract Set<String> getCollectionNames();
    
    public abstract DBAddress getAddress();
    public abstract String getConnectPoint();
    
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
    public DBObject command( DBObject cmd ){
        return getCollection( "$cmd" ).findOne( cmd );
    }

    /** Returns the name of this database.
     * @return the name
     */
    public String getName(){
	return _name;
    }

    /** Returns a set of the names of collections in this database.
     * @return the names of collections in this database
     */
    public Set<String> keySet( boolean includePrototype ){
        return getCollectionNames();
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

    
    final String _name;
    final Set<DBCollection> _seenCollections = new HashSet<DBCollection>();

    protected boolean _readOnly = false;

}
