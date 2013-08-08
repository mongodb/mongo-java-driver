// PoolOutputBuffer.java

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

import org.bson.*;
import org.bson.io.*;
import org.bson.util.*;

import java.io.*;
import java.util.*;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class PoolOutputBuffer extends OutputBuffer {

    public static final int BUF_SIZE = 1024 * 16;

    public PoolOutputBuffer(){
        reset();
    }

    public void reset(){
        _cur.reset();
        _end.reset();

        for ( int i=0; i<_fromPool.size(); i++ )
            _extra.done( _fromPool.get(i) );
        _fromPool.clear();
    }

    public int getPosition(){
        return _cur.pos();
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public void setPosition( int position ){
        _cur.reset( position );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public void seekEnd(){
        _cur.reset( _end );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public void seekStart(){
        _cur.reset();
    }


    public int size(){
        return _end.pos();
    }

    public void write(byte[] b){
        write( b , 0 , b.length );
    }

    public void write(byte[] b, int off, int len){
        while ( len > 0 ){
            byte[] bs = _cur();
            int space = Math.min( bs.length - _cur.y , len );
            System.arraycopy( b , off , bs , _cur.y , space );
            _cur.inc( space );
            len -= space;
            off += space;
            _afterWrite();
        }
    }

    public void write(int b){
        byte[] bs = _cur();
        bs[_cur.getAndInc()] = (byte)(b&0xFF);
        _afterWrite();
    }

    void _afterWrite(){

        if ( _cur.pos() < _end.pos() ){
            // we're in the middle of the total space
            // just need to make sure we're not at the end of a buffer
            if ( _cur.y == BUF_SIZE )
                _cur.nextBuffer();
            return;
        }

        _end.reset( _cur );

        if ( _end.y < BUF_SIZE )
            return;

        _fromPool.add( _extra.get() );
        _end.nextBuffer();
        _cur.reset( _end );
    }

    byte[] _cur(){
        return _get( _cur.x );
    }

    byte[] _get( int z ){
        if ( z < 0 )
            return _mine;
        return _fromPool.get(z);
    }

    public int pipe( final OutputStream out )
        throws IOException {

        if ( out == null )
            throw new NullPointerException( "out is null" );

        int total = 0;

        for ( int i=-1; i<_fromPool.size(); i++ ){
            final byte[] b = _get( i );
            final int amt = _end.len( i );
            out.write( b , 0 , amt );
            total += amt;
        }

        return total;
    }

    static class Position {
        Position(){
            reset();
        }

        void reset(){
            x = -1;
            y = 0;
        }

        void reset( Position other ){
            x = other.x;
            y = other.y;
        }

        void reset( int pos ){
            x = ( pos / BUF_SIZE ) - 1;
            y = pos % BUF_SIZE;
        }

        int pos(){
            return ( ( x + 1 ) * BUF_SIZE ) + y;
        }

        int getAndInc(){
            return y++;
        }

        void inc( int amt ){
            y += amt;
            if ( y > BUF_SIZE )
                throw new IllegalArgumentException( "something is wrong" );
        }

        void nextBuffer(){
            if ( y != BUF_SIZE )
                throw new IllegalArgumentException( "broken" );
            x++;
            y = 0;
        }

        int len( int which ){
            if ( which < x )
                return BUF_SIZE;
            return y;
        }

        public String toString(){
            return x + "," + y;
        }

        int x; // which buffer -1 == _mine
        int y; // position in buffer
    }

    public String asAscii(){
        if ( _fromPool.size() > 0 )
            return super.asString();

        final int m = size();
        final char c[] = m < _chars.length ? _chars : new char[m];

        for ( int i=0; i<m; i++ )
            c[i] = (char)_mine[i];

        return new String( c , 0  , m );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public String asString( String encoding )
        throws UnsupportedEncodingException {


        if ( _fromPool.size() > 0 )
            return super.asString( encoding );

        if ( encoding.equals( DEFAULT_ENCODING_1 ) || encoding.equals( DEFAULT_ENCODING_2) ){
            try {
                return _encoding.decode( _mine , 0 , size() );
            }
            catch ( IOException ioe ){
                // we failed, fall back
            }
        }
        return new String( _mine , 0 , size() , encoding );
    }


    final byte[] _mine = new byte[BUF_SIZE];
    final char[] _chars = new char[BUF_SIZE];
    final List<byte[]> _fromPool = new ArrayList<byte[]>();
    final UTF8Encoding _encoding = new UTF8Encoding();

    private static final String DEFAULT_ENCODING_1 = "UTF-8";
    private static final String DEFAULT_ENCODING_2 = "UTF8";

    private final Position _cur = new Position();
    private final Position _end = new Position();

    private static org.bson.util.SimplePool<byte[]> _extra =
        new org.bson.util.SimplePool<byte[]>( ( 1024 * 1024 * 10 ) / BUF_SIZE ){

        protected byte[] createNew(){
            return new byte[BUF_SIZE];
        }

    };
}
