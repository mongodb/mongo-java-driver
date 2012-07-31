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

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.io.PoolOutputBuffer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

class OutMessage extends BasicBSONEncoder {

    enum OpCode {
        OP_UPDATE(2001),
        OP_INSERT(2002),
        OP_QUERY(2004),
        OP_GETMORE(2005),
        OP_DELETE(2006),
        OP_KILL_CURSORS(2007);

        OpCode(int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }

    static AtomicInteger REQUEST_ID = new AtomicInteger(1);

    public static OutMessage insert(final DBCollection collection, final DBEncoder encoder, WriteConcern concern) {
        OutMessage om = new OutMessage(collection, OpCode.OP_INSERT, encoder);

        int flags = 0;
        if (concern.getContinueOnErrorForInsert()) {
            flags |= 1;
        }
        om.writeInt( flags );
        om.writeCString( collection.getFullName() );

        return om;
    }

    public static OutMessage update(final DBCollection collection, final DBEncoder encoder,
                              final boolean upsert, final boolean multi, final DBObject query, final DBObject o) {
        OutMessage om = new OutMessage(collection, OpCode.OP_UPDATE, encoder);

        om.writeInt( 0 ); // reserved
        om.writeCString(collection.getFullName());

        int flags = 0;
        if ( upsert ) flags |= 1;
        if ( multi ) flags |= 2;
        om.writeInt( flags );

        om.putObject( query );
        om.putObject( o );

        om._query = query;

        return om;
    }

    public static OutMessage remove(final DBCollection collection, final DBEncoder encoder, final DBObject query) {
        OutMessage om = new OutMessage(collection, OpCode.OP_DELETE, encoder);

        om.writeInt( 0 ); // reserved
        om.writeCString( collection.getFullName() );

        Collection<String> keys = query.keySet();

        if ( keys.size() == 1 && keys.iterator().next().equals( "_id" ) && query.get( keys.iterator().next() ) instanceof ObjectId)
            om.writeInt( 1 );
        else
            om.writeInt( 0 );

        om.putObject( query );

        om._query = query;

        return om;
    }


    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        return query( collection, options, numToSkip, batchSize, query, fields, ReadPreference.primary() );
    }

    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref ){
        return query( collection, options, numToSkip, batchSize, query, fields, readPref, DefaultDBEncoder.FACTORY.create());
    }

    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref, DBEncoder enc ){
        OutMessage out = new OutMessage(collection, OpCode.OP_QUERY, enc);

        out._appendQuery(options, collection, numToSkip, batchSize, query, fields, readPref);

        return out;
    }

    static OutMessage getMore(DBCollection collection, long cursorId, int batchSize) {
        OutMessage om = new OutMessage(collection, OpCode.OP_GETMORE);

        om.writeInt(0);
        om.writeCString(collection.getFullName());
        om.writeInt(batchSize);
        om.writeLong(cursorId);

        return om;
    }

    static OutMessage killCursors(Mongo mongo, int numCursors) {
        OutMessage om = new OutMessage(mongo , OpCode.OP_KILL_CURSORS);

        om.writeInt(0); // reserved
        om.writeInt(numCursors);


        return om;
    }

    OutMessage(final DBCollection collection, final OpCode opQuery, final DBEncoder enc) {
        this(collection.getDB().getMongo(), opQuery, enc);
        this._collection = collection;
    }

    OutMessage(final DBCollection collection, final OpCode opQuery) {
        this(collection.getDB().getMongo(), opQuery);
        this._collection = collection;
    }


    OutMessage( Mongo m , OpCode opCode ){
        this( m, DefaultDBEncoder.FACTORY.create() );
        reset(opCode);
    }

    OutMessage( Mongo m , OpCode opCode , DBEncoder enc ) {
        this( m , enc );
        reset( opCode );
    }

    private OutMessage( Mongo m , DBEncoder encoder ) {
        _encoder = encoder;
        _mongo = m;
        _buffer = _mongo == null ? new PoolOutputBuffer() : _mongo._bufferPool.get();
        _buffer.reset();

        set( _buffer );
    }

    private void _appendQuery( int options , DBCollection collection , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref){
        _queryOptions = options;
        _readPref = readPref;

        //If the readPrefs are non-null and non-primary, set slaveOk query option
        if (_readPref != null && _readPref.isSlaveOk()) {
            _queryOptions |= Bytes.QUERYOPTION_SLAVEOK;
        }

        writeInt( _queryOptions );
        writeCString( collection.getFullName() );

        writeInt( numToSkip );
        writeInt( batchSize );

        putObject( query );
        if ( fields != null )
            putObject( fields );

        this._query = query;

    }

    private void reset( OpCode opCode ){
        done();
        _buffer.reset();
        set( _buffer );

        _id = REQUEST_ID.getAndIncrement();
        _opCode = opCode;

        writeInt( 0 ); // length: will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( opCode.getValue() );
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
        if ( _buffer != null && _mongo != null ) {
            _buffer.reset();
            _mongo._bufferPool.done( _buffer );
        }

        _buffer = null;
        _mongo = null;
    }

    boolean hasOption( int option ){
        return ( _queryOptions & option ) != 0;
    }

    int getId(){
        return _id;
    }

    OpCode getOpCode() {
        return _opCode;
    }

    DBObject getQuery() {
        return _query;
    }

    String getNamespace() {
        return _collection != null ? _collection.getFullName() : null;
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
    private OpCode _opCode;
    private DBCollection _collection;
    private int _queryOptions = 0;
    private DBObject _query;
    private ReadPreference _readPref = ReadPreference.primary();
    private DBEncoder _encoder;

}
