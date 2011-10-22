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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.io.PoolOutputBuffer;

class OutMessage extends BasicBSONEncoder {

    static AtomicInteger ID = new AtomicInteger(1);

    static OutMessage query( Mongo m , int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        return query( m, options, ns, numToSkip, batchSize, query, fields, ReadPreference.PRIMARY );
    }

    static OutMessage query( Mongo m , int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref ){
        return query( m, options, ns, numToSkip, batchSize, query, fields, readPref, DefaultDBEncoder.FACTORY.create());
    }

    static OutMessage query( Mongo m , int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref, DBEncoder enc ){
        OutMessage out = new OutMessage( m , 2004, enc );
        out._appendQuery( options , ns , numToSkip , batchSize , query , fields, readPref);
        return out;
    }

    OutMessage( Mongo m ){
        this( m , DefaultDBEncoder.FACTORY.create() );
    }

    OutMessage( Mongo m , int op ){
        this( m );
        reset( op );
    }

    OutMessage( Mongo m , DBEncoder encoder ) {
        _encoder = encoder;
        _mongo = m;
        _buffer = _mongo == null ? new PoolOutputBuffer() : _mongo._bufferPool.get();
        set( _buffer );
    }

    OutMessage( Mongo m , int op , DBEncoder enc ) {
        this( m , enc );
        reset( op );
    }
    private void _appendQuery( int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref){
        _queryOptions = options;
        _readPref = readPref;

        //If the readPrefs are non-null and non-primary, set slaveOk query option
        if (!(_readPref instanceof ReadPreference.PrimaryReadPreference))
		_queryOptions |= Bytes.QUERYOPTION_SLAVEOK;

        writeInt( _queryOptions );
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
        int sz = _encoder.writeObject(_buf, o);
        if (_mongo != null) {
            int maxsize = _mongo.getConnector().getMaxBsonObjectSize();
            maxsize = Math.max(maxsize, Bytes.MAX_OBJECT_SIZE);
            if (sz > maxsize) {
                throw new MongoInternalException("DBObject of size " + sz + " is over Max BSON size " + _mongo.getMaxBsonObjectSize());
            }
        }
        return sz;
    }


    public ReadPreference getReadPreference(){
        return _readPref;
    }

    private Mongo _mongo;
    private PoolOutputBuffer _buffer;
    private int _id;
    private int _queryOptions = 0;
    private ReadPreference _readPref = ReadPreference.PRIMARY;
    private DBEncoder _encoder;

}
