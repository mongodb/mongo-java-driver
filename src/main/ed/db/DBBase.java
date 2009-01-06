// DBBase.java

package ed.db;

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
    
    public final DBCollection getCollection( String name ){
        DBCollection c = doGetCollection( name );
        if ( c != null ){
            _seenCollections.add( c );
	}
        return c;
    }
    
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

    public DBObject command( DBObject cmd ){
        return getCollection( "$cmd" ).findOne( cmd );
    }

    public String getName(){
	return _name;
    }

    public Set<String> keySet( boolean includePrototype ){
        return getCollectionNames();
    }

    public void setReadOnly( Boolean b ){
        _readOnly = b;
    }

    public String toString(){
        return _name;
    }

    public void resetIndexCache(){
        for ( DBCollection c : _seenCollections )
            c.resetIndexCache();
    }

    
    final String _name;
    final Set<DBCollection> _seenCollections = new HashSet<DBCollection>();

    protected boolean _readOnly = false;

}
