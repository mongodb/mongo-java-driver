// BSONCallback.java

package org.bson;

import java.io.*;

import org.bson.types.*;

public interface BSONCallback {
    
    void objectStart();
    void objectStart(String name);
    Object objectDone();

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
     * subtype 2
     */
    void gotBinaryArray( String name , byte[] b );
    void gotBinary( String name , byte type , byte[] data );
}
