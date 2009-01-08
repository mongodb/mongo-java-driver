// ByteBufferFactory.java

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

import java.nio.*;

public interface ByteBufferFactory {
    public ByteBuffer get();

    public static class SimpleHeapByteBufferFactory implements ByteBufferFactory {
	public SimpleHeapByteBufferFactory( int size ){
	    _size = size;
	}
	
	public ByteBuffer get(){
	    return ByteBuffer.wrap( new byte[_size] );
	}

	final int _size;
    }
}
