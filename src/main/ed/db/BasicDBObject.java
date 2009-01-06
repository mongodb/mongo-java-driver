// BasicDBObject.java

package ed.db;

import java.util.*;

public class BasicDBObject extends HashMap<String,Object> implements DBObject {
    
    public BasicDBObject(){
        this( false );
    }
    
    public BasicDBObject( boolean isPartialObject ){
        _isPartialObject = isPartialObject;
    }

    public Object removeField( String key ){
        return remove( key );
    }

    public boolean isPartialObject(){
        return _isPartialObject;
    }

    public boolean containsKey( String key ){
        return super.containsKey( (Object)key );
    }

    public Object get( String key ){
        return super.get( (Object)key );
    }

    final boolean _isPartialObject;
}
