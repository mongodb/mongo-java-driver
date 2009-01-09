// BasicDBList.java

package com.mongodb;

import com.mongodb.util.OrderedSet;

import java.util.*;

public class BasicDBList extends ArrayList<Object> implements DBObject {
    
    public BasicDBList(){
    }
    
    public Object put( String key , Object v ){
        int i = _getInt( key );
        while ( i >= size() )
            add( null );
        set( i , v );
        return v;
    }
    
    public Object get( String key ){
        int i = _getInt( key );
        if ( i < 0 )
            return null;
        if ( i >= size() )
            return null;
        return get( i );
    }

    public Object removeField( String key ){
        int i = _getInt( key );
        if ( i < 0 )
            return null;
        if ( i >= size() )
            return null;
        return remove( i );        
    }

    public boolean containsKey( String key ){
        int i = _getInt( key );
        return i >= 0 && i < size();
    }

    public Set<String> keySet(){
        Set<String> s = new OrderedSet<String>();
        for ( int i=0; i<size(); i++ )
            s.add( String.valueOf( i ) );
        return s;
    }

    int _getInt( String s ){
        try {
            return Integer.parseInt( s );
        }
        catch ( Exception e ){
            return -1;
        }
    }

    public boolean isPartialObject(){
        return _isPartialObject;
    }

    public void markAsPartialObject(){
        _isPartialObject = true;
    }

    private boolean _isPartialObject;
}
