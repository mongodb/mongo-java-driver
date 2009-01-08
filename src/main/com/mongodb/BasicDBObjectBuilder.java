// BasicDBObjectBuilder.java

package com.mongodb;

public class BasicDBObjectBuilder {
    
    public static BasicDBObjectBuilder start(){
        return new BasicDBObjectBuilder();
    }

    public BasicDBObjectBuilder add( String key , Object val ){
        _it.put( key , val );
        return this;
    }
    
    public DBObject get(){
        return _it;
    }

    DBObject _it = new BasicDBObject();

}
