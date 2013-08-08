// ByteBufferInputStream.java

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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class ByteBufferInputStream extends InputStream {

    public ByteBufferInputStream( List<ByteBuffer> lst ){
	this( lst , false );
    }

    public ByteBufferInputStream( List<ByteBuffer> lst , boolean flip ){
	_lst = lst;
	if ( flip )
	    for ( ByteBuffer buf : _lst )
		buf.flip();
    }
    
    public int available(){
	int avail = 0;
	for ( int i=_pos; i<_lst.size(); i++ )
	    avail += _lst.get( i ).remaining();
	return avail;
    }

    public void close(){}
    
    public void mark(int readlimit){
	throw new RuntimeException( "mark not supported" );
    }

    public void reset(){
	throw new RuntimeException( "mark not supported" );
    }
    
    public boolean markSupported(){
	return false;
    }
    
    public int read(){
	if ( _pos >= _lst.size() )
	    return -1;
	
	ByteBuffer buf = _lst.get( _pos );
	if ( buf.remaining() > 0 )
	    return buf.get() & 0xff;
	
	_pos++;
	return read();
    }
    
    public int read(byte[] b){
	return read( b , 0 , b.length );
    }
    
    public int read(byte[] b, int off, int len){
	if ( _pos >= _lst.size() )
	    return -1;
	
	ByteBuffer buf = _lst.get( _pos );

	if ( buf.remaining() == 0 ){
	    _pos++;
	    return read( b , off , len );
	}
	
	int toRead = Math.min( len , buf.remaining() );
	buf.get( b , off , toRead );

	if ( toRead == len || _pos + 1 >= _lst.size() )
	    return toRead;

	_pos++;
	return toRead + read( b , off + toRead , len - toRead );
    }

    
    public long skip(long n){
	long skipped = 0;

	while ( n >= 0 && _pos < _lst.size() ){
	    ByteBuffer b = _lst.get( _pos );
	    if ( b.remaining() < n ){
		skipped += b.remaining();
		n -= b.remaining();
		b.position( b.limit() );
		_pos++;
		continue;
	    }
	    
	    skipped += n;
	    b.position( (int)(b.position() + n) );
	    return skipped;
	}
	
	return skipped;
    }
    
    final List<ByteBuffer> _lst;
    private int _pos = 0;
}
