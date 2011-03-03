// OutputBuffer.java

package org.bson.io;

import java.io.*;
import java.security.*;

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
    public byte[] toByteArray(){
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream( size() );
            pipe( bout );
            return bout.toByteArray();
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }

    public String asString(){
        return new String( toByteArray() );
    }

    public String asString( String encoding )
        throws UnsupportedEncodingException {
        return new String( toByteArray() , encoding );
    }


    public String hex(){
        final StringBuilder buf = new StringBuilder();
        try {
            pipe( new OutputStream(){
                    public void write( int b ){
                        String s = Integer.toHexString(0xff & b);
                        
                        if (s.length() < 2) 
                            buf.append("0");
                        buf.append(s);
                    }
                } 
                );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "impossible" );
        }
        return buf.toString();
    }

    public String md5(){
        final MessageDigest md5 ;
        try {
            md5 = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error - this implementation of Java doesn't support MD5.");
        }        
        md5.reset();

        try {
            pipe( new OutputStream(){
                    public void write( byte[] b , int off , int len ){
                        md5.update( b , off , len );
                    }

                    public void write( int b ){
                        md5.update( (byte)(b&0xFF) );
                    }
                } 
                );
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "impossible" );
        }
        
        return com.mongodb.util.Util.toHex( md5.digest() );
    }
    
    public void writeInt( int x ){
        write( x >> 0 );
        write( x >> 8 );
        write( x >> 16 );
        write( x >> 24 );
    }

    public void writeIntBE( int x ){
        write( x >> 24 );
        write( x >> 16 );
        write( x >> 8 );
        write( x );
    }

    public void writeInt( int pos , int x ){
        final int save = getPosition();
        setPosition( pos );
        writeInt( x );
        setPosition( save );
    }

    public void writeLong( long x ){
        write( (byte)(0xFFL & ( x >> 0 ) ) );
        write( (byte)(0xFFL & ( x >> 8 ) ) );
        write( (byte)(0xFFL & ( x >> 16 ) ) );
        write( (byte)(0xFFL & ( x >> 24 ) ) );
        write( (byte)(0xFFL & ( x >> 32 ) ) );
        write( (byte)(0xFFL & ( x >> 40 ) ) );
        write( (byte)(0xFFL & ( x >> 48 ) ) );
        write( (byte)(0xFFL & ( x >> 56 ) ) );
    }

    public void writeDouble( double x ){
        writeLong( Double.doubleToRawLongBits( x ) );
    }

    public String toString(){
        return getClass().getName() + " size: " + size() + " pos: " + getPosition() ;
    }
}
