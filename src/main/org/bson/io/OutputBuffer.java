// OutputBuffer.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.bson.io;

import org.bson.BSONException;

import java.io.*;
import java.security.*;

public abstract class OutputBuffer extends OutputStream {

    public abstract void write(byte[] b);
    public abstract void write(byte[] b, int off, int len);
    public abstract void write(int b);
    
    public abstract int getPosition();

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public abstract void setPosition( int position );

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public abstract void seekEnd();

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
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
    public byte [] toByteArray(){
        try {
            final ByteArrayOutputStream bout = new ByteArrayOutputStream( size() );
            pipe( bout );
            return bout.toByteArray();
        }
        catch ( IOException ioe ){
            throw new RuntimeException( "should be impossible" , ioe );
        }
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public String asString(){
        return new String( toByteArray() );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public String asString( String encoding )
        throws UnsupportedEncodingException {
        return new String( toByteArray() , encoding );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
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


    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
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

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public void writeIntBE( int x ){
        write( x >> 24 );
        write( x >> 16 );
        write( x >> 8 );
        write( x );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
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

    /**
     * Writes C string (null-terminated string) to underlying buffer.
     *
     * @param str the string
     * @return number of bytes written
     */
    public int writeCString(final String str) {

        final int len = str.length();
        int total = 0;

        for (int i = 0; i < len;/*i gets incremented*/) {
            final int c = Character.codePointAt(str, i);

            if (c == 0x0) {
                throw new BSONException(
                        String.format("BSON cstring '%s' is not valid because it contains a null character at index %d", str, i));
            }
            if (c < 0x80) {
                write((byte) c);
                total += 1;
            } else if (c < 0x800) {
                write((byte) (0xc0 + (c >> 6)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 2;
            } else if (c < 0x10000) {
                write((byte) (0xe0 + (c >> 12)));
                write((byte) (0x80 + ((c >> 6) & 0x3f)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 3;
            } else {
                write((byte) (0xf0 + (c >> 18)));
                write((byte) (0x80 + ((c >> 12) & 0x3f)));
                write((byte) (0x80 + ((c >> 6) & 0x3f)));
                write((byte) (0x80 + (c & 0x3f)));
                total += 4;
            }

            i += Character.charCount(c);
        }

        write((byte) 0);
        total++;
        return total;
    }

    public String toString(){
        return getClass().getName() + " size: " + size() + " pos: " + getPosition() ;
    }
}
