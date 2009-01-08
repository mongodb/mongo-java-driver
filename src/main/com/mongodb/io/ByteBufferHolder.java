// ByteBufferHolder.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package com.mongodb.io;

import java.util.*;
import java.nio.*;
import java.nio.channels.*;

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
