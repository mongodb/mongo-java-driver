// BasicDBList.java

package ed.db;

import java.util.*;

public class BasicDBList extends ArrayList<Object> implements DBObject {
    
    public BasicDBList(){
    }
    
    public Object put( String key , Object v ){
        int i = Integer.parseInt( key );
        while ( i >= size() )
            add( null );
        set( i , v );
        return v;
    }
    
    public Object get( String key ){
        int i = Integer.parseInt( key );
        if ( i >= size() )
            return null;
        return get( i );
    }

    public Object removeField( String key ){
        int i = Integer.parseInt( key );
        if ( i >= size() )
            return null;
        return remove( i );        
    }

    public boolean containsKey( String key ){
        int i = Integer.parseInt( key );
        return i >= 0 && i < size();
    }

    public Set<String> keySet(){
        Set<String> s = new HashSet<String>();
        for ( int i=0; i<size(); i++ )
            s.add( String.valueOf( i ) );
        return s;
    }

    public boolean isPartialObject(){
        return _isPartialObject;
    }

    public void markAsPartialObject(){
        _isPartialObject = true;
    }

    private boolean _isPartialObject;
}
