// ByteBufferHolder.java

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

package com.mongodb.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class ByteBufferHolder {

    public ByteBufferHolder(){
	this( 1024 * 1024 * 1024 ); // 1gb
    }

    public ByteBufferHolder( int max ){
	_max = max;
    }
    
    public byte get( int i ){
        if ( i >= _pos )
            throw new RuntimeException( "out of bounds" );
        
        final int num = i / _bufSize;
        final int pos = i % _bufSize;

        return _buffers.get( num ).get( pos );
    }

    public void get( int pos , byte b[] ){
        for ( int i=0; i<b.length; i++ )
            b[i] = get( i + pos );
    }

    public void put( int i , byte val ){
        if ( i >= _pos )
            throw new RuntimeException( "out of bounds" );
        
        final int num = i / _bufSize;
        final int pos = i % _bufSize;

        _buffers.get( num ).put( pos , val );
    }
    
    public int position(){
        return _pos;
    }

    public void position( int p ){
        _pos = p;
        int num = _pos / _bufSize;
        int pos = _pos % _bufSize;
        
        while ( _buffers.size() <= num )
            _addBucket();

        ByteBuffer bb = _buffers.get( num );
        bb.position( pos );
        for ( int i=num+1; i<_buffers.size(); i++ )
            _buffers.get( i ).position( 0 );
    }

    public int remaining(){
        return Integer.MAX_VALUE;
    }

    public void put( ByteBuffer in ){
        while ( in.hasRemaining() ){
            int num = _pos / _bufSize;
            if ( num >= _buffers.size() )
                _addBucket();

            ByteBuffer bb = _buffers.get( num );
            
            final int canRead = Math.min( bb.remaining() , in.remaining() );
            
            final int oldLimit = in.limit();
            in.limit( in.position() + canRead );
            
            bb.put( in );
            
            in.limit( oldLimit );
            
            _pos += canRead;
        }

    }

    private void _addBucket(){
	if ( capacity() + _bufSize > _max )
	    throw new RuntimeException( "too big current:" + capacity() );
        _buffers.add( ByteBuffer.allocateDirect( _bufSize ) );
    }
    
    public int capacity(){
	return _buffers.size() * _bufSize;
    }

    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append( "{ ByteBufferHolder pos:" + _pos + " " );
        for ( ByteBuffer bb : _buffers )
            buf.append( bb ).append( " " );
        return buf.append( "}" ).toString();
    }

    List<ByteBuffer> _buffers = new ArrayList<ByteBuffer>();
    int _pos = 0;
    final int _max;

    static final int _bufSize = 4096;
}
