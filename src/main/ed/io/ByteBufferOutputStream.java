// ByteBufferOutputStream.java

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

package ed.io;

import java.io.*;
import java.nio.*;
import java.util.*;

public class ByteBufferOutputStream extends OutputStream  {

    public ByteBufferOutputStream(){
	this( _defaultFactory );
    }
    
    public ByteBufferOutputStream( int size ){
	this( new ByteBufferFactory.SimpleHeapByteBufferFactory( size ) );
    }

    public ByteBufferOutputStream( ByteBufferFactory factory ){
	_factory = factory;
    }

    public void close(){
    }
    
    public void flush(){
    }
    
    public void write(byte[] b){
	write( b , 0 , b.length );
    }

    public void write(byte[] b, int off, int len){
	ByteBuffer cur = _need( 1 );

	int toWrite = Math.min( len , cur.remaining() );
	cur.put( b , off , toWrite );
	
	if ( toWrite == len )
	    return;

	write( b , off + toWrite , len - toWrite );
    }

    public void write(int b){
	_need(1).put((byte)b);
    }

    public List<ByteBuffer> getBuffers(){
	return _lst;
    }

    public List<ByteBuffer> getBuffers( boolean flip ){
	if ( flip )
	    for ( ByteBuffer buf : _lst )
		buf.flip();
	return _lst;
    }
    
    private ByteBuffer _need( int space ){
	if ( _lst.size() == 0 ){
	    _lst.add( _factory.get() );
	    return _lst.get( 0 );
	}

	ByteBuffer cur = _lst.get( _lst.size() - 1 );
	if ( space <= cur.remaining() )
	    return cur;

	_lst.add( _factory.get() );
	return _lst.get( _lst.size() - 1 );
    }

    final List<ByteBuffer> _lst = new ArrayList<ByteBuffer>();
    final ByteBufferFactory _factory;
    
    static final ByteBufferFactory _defaultFactory = new ByteBufferFactory.SimpleHeapByteBufferFactory( 1024 * 4 );
}
