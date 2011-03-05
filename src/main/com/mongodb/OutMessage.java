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

    static AtomicInteger ID = new AtomicInteger(1);
    
    static OutMessage query( Mongo m , int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        OutMessage out = new OutMessage( m , 2004 );
        out._appendQuery( options , ns , numToSkip , batchSize , query , fields );
        return out;
    }

    OutMessage( Mongo m ){
        _mongo = m;
        _buffer = _mongo == null ? new PoolOutputBuffer() : _mongo._bufferPool.get();
        set( _buffer );
    }

    OutMessage( Mongo m , int op ){
        this( m );
        reset( op );
    }
    
    private void _appendQuery( int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        _queryOptions = options;
        writeInt( options );
        writeCString( ns );

        writeInt( numToSkip );
        writeInt( batchSize );
        
        putObject( query );
        if ( fields != null )
            putObject( fields );

    }

    private void reset( int op ){
        done();
        _buffer.reset();
        set( _buffer );
        
        _id = ID.getAndIncrement();

        writeInt( 0 ); // length: will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( op );
    }

    void prepare(){
        _buffer.writeInt( 0 , _buffer.size() );
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
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

    void append( String db , WriteConcern c ){

        _id = ID.getAndIncrement();

        int loc = size();

        writeInt( 0 ); // will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( 2004 );
        _appendQuery( 0 , db + ".$cmd" , 0 , -1 , c.getCommand() , null );
        _buf.writeInt( loc , size() - loc );
    }

    void pipe( OutputStream out )
        throws IOException {
        _buffer.pipe( out );
    }

    int size(){
        return _buffer.size();
    }

    byte[] toByteArray(){
        return _buffer.toByteArray();
    }

    void doneWithMessage(){
        if ( _buffer != null && _mongo != null )
            _mongo._bufferPool.done( _buffer );
        
        _buffer = null;
        _mongo = null;
    }

    boolean hasOption( int option ){
        return ( _queryOptions & option ) != 0;
    }

    int getId(){ 
        return _id;
    }

    @Override
    public int putObject(BSONObject o) {
        // check max size
        int sz = super.putObject(o);
        if (_mongo != null) {
            int maxsize = _mongo.getConnector().getMaxBsonObjectSize();
            maxsize = Math.max(maxsize, Bytes.MAX_OBJECT_SIZE);
            if (sz > maxsize) {
                throw new MongoInternalException("DBObject of size " + sz + " is over Max BSON size " + _mongo.getMaxBsonObjectSize());
            }
        }
        return sz;
    }

    private Mongo _mongo;
    private PoolOutputBuffer _buffer;
    private int _id;
    private int _queryOptions = 0;

}
