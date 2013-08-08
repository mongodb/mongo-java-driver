// ByteBufferOutputStream.java

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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
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
