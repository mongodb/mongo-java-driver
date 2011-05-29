// BSONCallback.java

package org.bson;

import org.bson.types.ObjectId;

public interface BSONCallback {
    
    void objectStart();
    void objectStart(String name);
    void objectStart(boolean array);
    Object objectDone();

    void reset();
    Object get();
    BSONCallback createBSONCallback();

    void arrayStart();
    void arrayStart(String name);
    Object arrayDone();
    
    void gotNull( String name );
    void gotUndefined( String name );
    void gotMinKey( String name );
    void gotMaxKey( String name );
    
    void gotBoolean( String name , boolean v );
    void gotDouble( String name , double v );
    void gotInt( String name , int v );
    void gotLong( String name , long v );
    
    void gotDate( String name , long millis );
    void gotString( String name , String v );
    void gotSymbol( String name , String v );
    void gotRegex( String name , String pattern , String flags );

    void gotTimestamp( String name , int time , int inc );
    void gotObjectId( String name , ObjectId id );
    void gotDBRef( String name , String ns , ObjectId id );
    
    /**
     * 
     */
    @Deprecated
    void gotBinaryArray( String name , byte[] data );
    void gotBinary( String name , byte type , byte[] data );
    /**
     * subtype 3
     */
    void gotUUID( String name , long part1, long part2);

    void gotCode( String name , String code );
    void gotCodeWScope( String name , String code , Object scope );
}
