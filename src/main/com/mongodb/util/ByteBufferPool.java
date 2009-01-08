// ByteBufferPool.java

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

package com.mongodb.util;

import java.nio.*;

import com.mongodb.io.*;

public class ByteBufferPool extends SimplePool<ByteBuffer> implements ByteBufferFactory {

    /** Initializes this pool with a given number of byte buffers to keep and size.
     * @param maxToKeep The number of byte buffers allowed
     * @param size The size for buffers this pool creates, in bytes
     */
    public ByteBufferPool( String name , int maxToKeep , int size ){
        this( name , maxToKeep , size , null );
    }

    /** Initializes this pool with a given number of byte buffers, size, and ordering.
     * @param maxToKeep The number of byte buffers allowed
     * @param size The size for buffers this pool creates, in bytes
     * @param order The ordering of the buffers (big or little endian)
     */
    public ByteBufferPool( String name , int maxToKeep , int size , ByteOrder order ){
        super( "ByteBufferPool-" + name , maxToKeep , -1  );
        _size = size;
        _order = order;
    }

    /** Creates a new buffer with this pool's standard size and order.
     * @return The new buffer.
     */
    public ByteBuffer createNew(){
        ByteBuffer bb = ByteBuffer.allocateDirect( _size );
        if ( _order != null )
            bb.order( _order );
        return bb;
    }

    /** Called when the given buffer is added to the pool.  Sets the size to its capacity and resets its mark's position to 0.
     * @param buf Buffer to be added.
     * @return true
     */
    public boolean ok( ByteBuffer buf ){
        buf.position( 0 );
        buf.limit( buf.capacity() );
        return true;
    }

    protected long memSize( ByteBuffer buf ){
        return _size;
    }

    final int _size;
    final ByteOrder _order;
}
