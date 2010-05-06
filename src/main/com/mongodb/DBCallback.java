// DBCallback.java

package com.mongodb;

import java.util.*;
import java.util.logging.*;

import org.bson.*;
import org.bson.types.*;

public class DBCallback extends BasicBSONCallback {

    DBCallback( DBCollection coll ){
        _collection = coll;
    }
    
    public BSONObject create(){
        return _create( null );
    }

    public BSONObject create( boolean array , List<String> path ){
        if ( array )
            return new BasicDBList();
        return _create( path );
    }

    private DBObject _create( List<String> path ){
        
        Class c = null;

        if ( _collection != null && _collection._objectClass != null){
            if ( path == null || path.size() == 0 ){
                c = _collection._objectClass;
            }
            else {
                StringBuilder buf = new StringBuilder();
                for ( int i=0; i<path.size(); i++ ){
                    if ( i > 0 )
                        buf.append(".");
                    buf.append( path.get(i) );
                }
                c = _collection.getInternalClass( buf.toString() );
            }
            
        }
        
        if ( c != null ){
            try {
                return (DBObject)c.newInstance();
            }
            catch ( InstantiationException ie ){
                LOGGER.log( Level.FINE , "can't create a: " + c , ie );
                throw new MongoInternalException( "can't instantiate a : " + c , ie );
            }
            catch ( IllegalAccessException iae ){
                LOGGER.log( Level.FINE , "can't create a: " + c , iae );
                throw new MongoInternalException( "can't instantiate a : " + c , iae );
            }
        }
        return new BasicDBObject();
    }

    final DBCollection _collection;
    static final Logger LOGGER = Logger.getLogger( "com.mongo.DECODING" );
}
