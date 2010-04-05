// DBMessage.java

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

package com.mongodb;

import java.nio.*;
import java.util.concurrent.atomic.*;

/** Creates a message to send to the database.  
 * Database messages are of the form:
 * <blockquote>
 * &lt;length&gt;&lt;id&gt;&lt;respondingTo&gt;&lt;operation&gt;&lt;data&gt;
 * </blockquote>
 * The first four variables are the header and the last is the content.
 * <table border="1"><tr>
 * <th>Variable</th><th>Type</th><th>Description</th></tr>
 * <tr><td>length</td><td><code>int</code></td><td>The length of the entire message</td></tr>
 * <tr><td>id</td><td><code>int</code></td><td>A unique id generated automatically for the message</td></tr>
 * <tr><td>respondingTo</td><td><code>int</code></td><td>The id of the message this is in response to, or 0 if it isn't a reponse</td></tr>
 * <tr><td>operation</td><td><code>int</code></td><td>The database operation desired</td></tr>
 * <tr><td>data</td><td><code>byte[]</code></td><td>The message body</td></tr>
 * </table>
 */
public class DBMessage {
    
    static AtomicInteger ID = new AtomicInteger(1);
    static int HEADER_LENGTH = 16;

    DBMessage( int operation , ByteBuffer data ){
        _id = ID.getAndIncrement();
        _responseTo = 0;
        _operation = operation;
        _data = data;
        
        if ( _data.position() > 0 )
            _data.flip();
        
        _len = HEADER_LENGTH + data.limit();
    }
    
    DBMessage( ByteBuffer buf , ByteBuffer dataBuffer ){
        _len = buf.getInt();
        _id = buf.getInt();
        _responseTo = buf.getInt();
        _operation = buf.getInt();

        _data = dataBuffer;
    }

    void putHeader( ByteBuffer buf ){
        buf.putInt( _len );
        buf.putInt( _id ) ;
        buf.putInt( _responseTo );
        buf.putInt( _operation );
    }

    ByteBuffer getData(){
        return _data;
    }

    int dataLen(){
        return _len - HEADER_LENGTH;
    }

    public String toString(){
        return "DBMessage len: " + _len + " id: " + _id +
            " responseTo: " + _responseTo + " operation: "  + _operation;
    }

    final int _len;    
    final int _id;
    final int _responseTo;
    final int _operation;

    
    final ByteBuffer _data;
}
