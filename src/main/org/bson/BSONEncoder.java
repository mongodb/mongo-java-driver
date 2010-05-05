// BSONEncoder.java

package org.bson;

import org.bson.io.*;

/**
 * this is meant to be pooled or cached
 * there is some per instance memory for string conversion, etc...
 */
public class BSONEncoder {
    
    public BSONEncoder(){

    }

    public void set( OutputBuffer out ){
        if ( _out != null )
            throw new IllegalStateException( "in the middle of something" );
        
        _out = out;
    }
    
    private OutputBuffer _out;
}
