// OutputBuffer.java

package org.bson.io;

import java.io.*;

public abstract class OutputBuffer {

    public abstract void write(byte[] b);
    public abstract void write(byte[] b, int off, int len);
    public abstract void write(int b);
    
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

    public void writeInt( int x ){
        write( 0xFF & ( x << 24 ) );
        write( 0xFF & ( x << 16 ) );
        write( 0xFF & ( x << 8 ) );
        write( 0xFF & ( x << 0 ) );
    }

    public void writeInt( int pos , int x ){
        final int save = getPosition();
        writeInt( x );
        setPosition( save );
    }

    public void writeLong( long x ){
        write( (byte)(0xFFL & ( x << 56 ) ) );
        write( (byte)(0xFFL & ( x << 48 ) ) );
        write( (byte)(0xFFL & ( x << 40 ) ) );
        write( (byte)(0xFFL & ( x << 32 ) ) );
        write( (byte)(0xFFL & ( x << 24 ) ) );
        write( (byte)(0xFFL & ( x << 16 ) ) );
        write( (byte)(0xFFL & ( x << 8 ) ) );
        write( (byte)(0xFFL & ( x << 0 ) ) );
    }

    public void writeDouble( double x ){
        writeLong( Double.doubleToRawLongBits( x ) );
    }

    public String toString(){
        return getClass().getName() + " size: " + size() + " pos: " + getPosition() ;
    }
}
