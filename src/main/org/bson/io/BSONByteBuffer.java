/**
 *      Copyright (C) 2008-2011 10gen Inc.
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

import org.bson.*;

import java.io.*;
import java.nio.*;
import java.util.logging.*;

/**
 * Pseudo byte buffer, delegates as it is too hard to properly override / extend the ByteBuffer API
 *
 * @author brendan
 */
public class BSONByteBuffer {

    protected ByteBuffer buf;

    private BSONByteBuffer( ByteBuffer buf ){
        this.buf = buf;
        this._size = getInt( 0 );
        buf.order( ByteOrder.LITTLE_ENDIAN );
    }

    public static BSONByteBuffer wrap( byte[] bytes, int offset, int length ){
        return new BSONByteBuffer( ByteBuffer.wrap( bytes, offset, length ) );
    }

    public static BSONByteBuffer wrap( byte[] bytes ){
        return new BSONByteBuffer( ByteBuffer.wrap( bytes ) );
    }

    public ByteBuffer slice(){
        return buf.slice();
    }

    public ByteBuffer duplicate(){
        return buf.duplicate();
    }

    public byte get( int i ){
        return buf.get( i );
    }

    public ByteBuffer get( byte[] bytes, int offset, int length ){
        return buf.get( bytes, offset, length );
    }

    public ByteBuffer get( byte[] bytes ){
        return buf.get( bytes );
    }

    public boolean hasArray(){
        return buf.hasArray();
    }

    public byte[] array(){
        return buf.array();
    }

    public int arrayOffset(){
        return buf.arrayOffset();
    }

    public String toString(){
        return buf.toString();
    }

    public int hashCode(){
        return buf.hashCode();
    }

    public boolean equals( Object o ){
        return buf.equals( o );
    }

    public int compareTo( ByteBuffer byteBuffer ){
        return buf.compareTo( byteBuffer );
    }

    public ByteOrder order(){
        return buf.order();
    }

    public char getChar( int i ){
        return buf.getChar( i );
    }

    public short getShort( int i ){
        return buf.getShort( i );
    }


    /**
     * Gets a Little Endian Integer
     *
     * @param i Index to read from
     *
     * @return
     */
    public int getInt( int i ){
        return getInt( i, true );
    }

    public int getInt( int i, boolean littleEndian ){
        return littleEndian ? getIntLE( i ) : getIntBE( i );
    }

    public int getIntLE( int i ){
        int x = 0;
        x |= ( 0xFF & buf.get( i + 0 ) ) << 0;
        x |= ( 0xFF & buf.get( i + 1 ) ) << 8;
        x |= ( 0xFF & buf.get( i + 2 ) ) << 16;
        x |= ( 0xFF & buf.get( i + 3 ) ) << 24;
        return x;
    }

    public int getIntBE( int i ){
        int x = 0;
        x |= ( 0xFF & buf.get( i + 0 ) ) << 24;
        x |= ( 0xFF & buf.get( i + 1 ) ) << 16;
        x |= ( 0xFF & buf.get( i + 2 ) ) << 8;
        x |= ( 0xFF & buf.get( i + 3 ) ) << 0;
        return x;
    }

    public long getLong( int i ){
        return buf.getLong( i );
    }


    public float getFloat( int i ){
        return buf.getFloat( i );
    }


    public double getDouble( int i ){
        return buf.getDouble( i );
    }


    public String getCString( int i ){
        boolean isAscii = true;
        // Short circuit 1 byte strings
        _random[0] = get( i++ );
        if ( _random[0] == 0 ){
            return "";
        }

        _random[1] = get( i++ );
        if ( _random[1] == 0 ){
            String out = ONE_BYTE_STRINGS[_random[0]];
            if ( out != null ){
                return out;
            }
            try {
                return new String( _random, 0, 1, "UTF-8" );
            }
            catch ( UnsupportedEncodingException e ) {
                throw new BSONException( "Cannot decode string as UTF-8" );
            }
        }

        // TODO - is this thread safe?
        _stringBuffer.reset();
        _stringBuffer.write( _random[0] );
        _stringBuffer.write( _random[1] );

        isAscii = _isAscii( _random[0] ) && _isAscii( _random[1] );

        while ( true ){
            byte b = get( i++ );
            if ( b == 0 )
                break;
            _stringBuffer.write( b );
            isAscii = isAscii && _isAscii( b );
        }

        String out = null;
        if ( isAscii ){
            out = _stringBuffer.asAscii();
        }
        else{
            try {
                out = _stringBuffer.asString( "UTF-8" );
            }
            catch ( UnsupportedEncodingException e ) {
                throw new BSONException( "Cannot decode string as UTF-8" );
            }
        }
        _stringBuffer.reset();
        return out.intern();
    }

    public String getUTF8String( int i ){
        int size = getInt( i );
        i += 4;
        /**
         * Protect against corruption to avoid huge strings
         */
        if ( size <= 0 || size > ( 32 * 1024 * 1024 ) )
            throw new BSONException( "Bad String Size: " + size );

        if ( size == 1 ){
            get( i );
            return "";
        }

        byte[] b = size < _random.length ? _random : new byte[size];

        for ( int n = 0; n < size; n++ ){
            b[n] = get( i + n );
        }

        try {
            return new String( b, 0, size - 1, "UTF-8" ).intern();
        }
        catch ( UnsupportedEncodingException e ) {
            throw new BSONException( "Cannot decode string as UTF-8." );
        }
    }

    public Buffer position( int i ){
        return buf.position( i );
    }

    public Buffer mark(){
        return buf.mark();
    }

    public Buffer reset(){
        return buf.reset();
    }


    public Buffer rewind(){
        return buf.rewind();
    }

    public int remaining(){
        return buf.remaining();
    }

    public boolean hasRemaining(){
        return buf.hasRemaining();
    }


    protected boolean _isAscii( byte b ){
        return b >= 0 && b <= 127;
    }

    public int size(){
        return _size;
    }

    private int _size;
    private byte[] _random = new byte[1024]; // has to be used within a single function
    private PoolOutputBuffer _stringBuffer = new PoolOutputBuffer();

    static final String[] ONE_BYTE_STRINGS = new String[128];

    static void _fillRange( byte min, byte max ){
        while ( min < max ){
            String s = "";
            s += (char) min;
            ONE_BYTE_STRINGS[(int) min] = s;
            min++;
        }
    }

    static{
        _fillRange( (byte) '0', (byte) '9' );
        _fillRange( (byte) 'a', (byte) 'z' );
        _fillRange( (byte) 'A', (byte) 'Z' );
    }

    private static final Logger log = Logger.getLogger( "org.bson.io.BSONByteBuffer" );


}
