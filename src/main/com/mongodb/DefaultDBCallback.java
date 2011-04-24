// DBCallback.java

package com.mongodb;

// Bson
import org.bson.*;
import org.bson.types.*;

// Java
import java.util.*;
import java.util.logging.*;

/**
 * This class overrides BasicBSONCallback to implement some extra features specific to the Database.
 * For example DBRef type.
 * @author antoine
 */
public class DefaultDBCallback extends BasicBSONCallback implements DBCallback {

    static class DefaultFactory implements DBCallbackFactory {
        @Override
        public DBCallback create( DBCollection collection ){
            return new DefaultDBCallback( collection );
        }
    }

    public static DBCallbackFactory FACTORY = new DefaultFactory();

    public DefaultDBCallback( DBCollection coll ){
        _collection = coll;
        _db = _collection == null ? null : _collection.getDB();
    }

    @Override
    @SuppressWarnings("deprecation")
    public void gotDBRef( String name , String ns , ObjectId id ){
        if ( id.equals( Bytes.COLLECTION_REF_ID ) )
            cur().put( name , _collection );
        else
            cur().put( name , new DBPointer( (DBObject)cur() , name , _db , ns , id ) );
    }

    @Override
    public void objectStart(boolean array, String name){
        _lastName = name;
        super.objectStart( array , name );
    }

    @Override
    public Object objectDone(){
        BSONObject o = (BSONObject)super.objectDone();
        if ( ! ( o instanceof List ) &&
             o.containsField( "$ref" ) &&
             o.containsField( "$id" ) ){
            return cur().put( _lastName , new DBRef( _db, o ) );
        }

        return o;
    }

    @Override
    public BSONObject create(){
        return _create( null );
    }

    @Override
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

        if ( _collection != null && _collection._name.equals( "$cmd" ) )
            return new CommandResult();
        return new BasicDBObject();
    }

    DBObject dbget(){
        return (DBObject)get();
    }

    @Override
    public void reset(){
        _lastName = null;
        super.reset();
    }

    private String _lastName;
    final DBCollection _collection;
    final DB _db;
    static final Logger LOGGER = Logger.getLogger( "com.mongo.DECODING" );
}
