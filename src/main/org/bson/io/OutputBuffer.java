// OutputBuffer.java

package org.bson.io;

import java.io.*;

public abstract class OutputBuffer extends OutputStream {
    
    public abstract int getPosition();
    public abstract void setPosition( int position );
    
    public abstract void seekEnd();
    public abstract void seekStart();
    
    /**
     * @return size of data so far
     */
    public abstract int size();
    
    /**
     * @return bytes written
     */
    public abstract int pipe( OutputStream out )
        throws IOException;

    /**
     * mostly for testing
     */
    public String asString(){
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream( size() );
            pipe( bout );
            return bout.toString();
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }

    public String toString(){
        return getClass().getName() + " size: " + size() + " pos: " + getPosition() ;
    }
}
