// ByteBufferPool.java

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
