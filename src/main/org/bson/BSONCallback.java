// BSONCallback.java

package org.bson;

import java.io.*;

public interface BSONCallback {
    
    void objectStart();
    void objectStart(String name);
    void objectDone();

    void arrayStart(String name);
    void arrayDone();

    
    void gotNull( String name );
    void gotUndefined( String name );
    void gotMinKey( String name );
    void gotMaxKey( String name );
    
    void gotBoolean( String name , boolean v );
    void gotDouble( String name , double v );
    void gotInt( String name , int v );
    void gotLong( String name , long v );
    
    void gotString( String name , String v );
    void gotSymbol( String name , String v );

    void gotTimestamp( String name , int time , int inc );
}
