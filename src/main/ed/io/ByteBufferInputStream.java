// ByteBufferInputStream.java

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
