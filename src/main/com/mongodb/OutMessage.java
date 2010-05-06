// OutMessage.java

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

import java.util.*;
import java.util.regex.*;
import java.util.concurrent.atomic.*;
import java.io.*;

import com.mongodb.util.*;

import org.bson.*;
import org.bson.io.*;
import org.bson.types.*;
import static org.bson.BSON.*;

class OutMessage extends BSONEncoder {

    enum State { READY , BUILDING , PREPARED }
    
    static AtomicInteger ID = new AtomicInteger(1);
    
    private static ThreadLocal<OutMessage> TL = new ThreadLocal<OutMessage>(){
        protected OutMessage initialValue(){
            return new OutMessage();
        }
    };

    static OutMessage get( int op ){
        OutMessage m = TL.get();
        m.reset( op );
        return m;
    }
    
    
    private OutMessage(){
        _state = State.READY;
    }
    
    private void reset( int op ){
        if ( _state == State.BUILDING )
            throw new IllegalStateException( "something is wrong state is:" + _state );
        
        done();
        _buffer.reset();
        set( _buffer );
        
        _id = ID.getAndIncrement();
        _state = State.BUILDING;

        writeInt( 0 ); // will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( op );
    }

    void prepare(){
        if ( _state == State.PREPARED )
            return; // this should be ok
            
        if ( _state == State.READY )
            throw new IllegalStateException( "something is wrong, preparing a READY buffer" );
        
        _buffer.writeInt( 0 , _buffer.size() );
        _state = State.PREPARED;
    }
    
    protected boolean handleSpecialObjects( String name , BSONObject o ){
        
        if ( o == null )
            return false;

        if ( o instanceof DBCollection ){
            DBCollection c = (DBCollection)o;
            putDBPointer( name , c.getName() , Bytes.COLLECTION_REF_ID );
            return true;
        }
        
        if ( name != null && o instanceof DBPointer ){
            DBPointer r = (DBPointer)o;
            putDBPointer( name , r._ns , (ObjectId)r._id );
            return true;
        }
        
        return false;
    }

    protected boolean putSpecial( String name , Object val ){
        if ( val instanceof DBPointer ){
            DBPointer r = (DBPointer)val;
            putDBPointer( name , r._ns , (ObjectId)r._id );
            return true;
        }
        
        if ( val instanceof DBRefBase ){
            putDBRef( name, (DBRefBase)val );
            return true;
        }
        
        return false;
    }

    protected void putDBPointer( String name , String ns , ObjectId oid ){
        _put( REF , name );
        
        _putValueString( ns );
        _buf.writeInt( oid._time() );
        _buf.writeInt( oid._machine() );
        _buf.writeInt( oid._inc() );
    }

    protected void putDBRef( String name, DBRefBase ref ){
        _put( OBJECT , name );
        final int sizePos = _buf.getPosition();
        _buf.writeInt( 0 );
        
        _putObjectField( "$ref" , ref.getRef() );
        _putObjectField( "$id" , ref.getId() );

        _buf.write( EOO );
        _buf.writeInt( sizePos , _buf.getPosition() - sizePos );
    }

    void pipe( OutputStream out )
        throws IOException {
        _buffer.pipe( out );
    }

    private PoolOutputBuffer _buffer = new PoolOutputBuffer();
    private int _id;
    private State _state;
}
