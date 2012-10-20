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
        om.writeInsertPrologue(concern);

        return om;
    }

    public static OutMessage update(final DBCollection collection, final DBEncoder encoder,
                              final boolean upsert, final boolean multi, final DBObject query, final DBObject o) {
        OutMessage om = new OutMessage(collection, OpCode.OP_UPDATE, encoder, query);
        om.writeUpdate(upsert, multi, query, o);

        return om;
    }

    public static OutMessage remove(final DBCollection collection, final DBEncoder encoder, final DBObject query) {
        OutMessage om = new OutMessage(collection, OpCode.OP_DELETE, encoder, query);
        om.writeRemove();

        return om;
    }

    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        return query( collection, options, numToSkip, batchSize, query, fields, ReadPreference.primary() );
    }

    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref ){
        return query( collection, options, numToSkip, batchSize, query, fields, readPref, DefaultDBEncoder.FACTORY.create());
    }

    static OutMessage query( DBCollection collection , int options , int numToSkip , int batchSize , DBObject query , DBObject fields, ReadPreference readPref, DBEncoder enc ){
        OutMessage om =  new OutMessage(collection, enc, query, options, readPref);
        om.writeQuery(fields, numToSkip, batchSize);

        return om;
    }

    static OutMessage getMore(DBCollection collection, long cursorId, int batchSize) {
        OutMessage om = new OutMessage(collection, OpCode.OP_GETMORE);
        om.writeGetMore(cursorId, batchSize);

        return om;
    }

    static OutMessage killCursors(Mongo mongo, int numCursors) {
        OutMessage om = new OutMessage(mongo , OpCode.OP_KILL_CURSORS);
        om.writeKillCursorsPrologue(numCursors);

        return om;
    }

    private OutMessage( Mongo m , OpCode opCode ){
        this(null, m, opCode, null);
    }

    private OutMessage(final DBCollection collection, final OpCode opCode) {
        this(collection, opCode, null);
    }

    private OutMessage(final DBCollection collection, final OpCode opCode, final DBEncoder enc) {
        this(collection, collection.getDB().getMongo(), opCode, enc);
    }

    private OutMessage(final DBCollection collection, final Mongo m, final OpCode opCode, final DBEncoder enc) {
        this(collection, m, opCode, enc, null, -1, null);
    }

    private OutMessage(final DBCollection collection, final OpCode opCode, final DBEncoder enc, final DBObject query) {
        this(collection, collection.getDB().getMongo(), opCode, enc, query, 0, null);
    }

    private OutMessage(final DBCollection collection, final DBEncoder enc, final DBObject query, final int options, final ReadPreference readPref) {
        this(collection, collection.getDB().getMongo(), OpCode.OP_QUERY, enc, query, options, readPref);
    }

    private OutMessage(final DBCollection collection, final Mongo m, OpCode opCode, final DBEncoder enc, final DBObject query, final int options, final ReadPreference readPref) {
        _collection = collection;
        _mongo = m;
        _encoder = enc;

        _buffer = _mongo._bufferPool.get();
        _buffer.reset();
        set(_buffer);

        _id = REQUEST_ID.getAndIncrement();
        _opCode = opCode;

        writeMessagePrologue(opCode);

        if (query == null) {
            _query = null;
            _queryOptions = 0;
        } else {
            _query = query;

            int allOptions = options;
            if (readPref != null && readPref.isSlaveOk()) {
                allOptions |= Bytes.QUERYOPTION_SLAVEOK;
            }

            _queryOptions = allOptions;
        }
    }

    private void writeInsertPrologue(final WriteConcern concern) {
        int flags = 0;
        if (concern.getContinueOnErrorForInsert()) {
            flags |= 1;
        }
        writeInt(flags);
        writeCString(_collection.getFullName());
    }

    private void writeUpdate(final boolean upsert, final boolean multi, final DBObject query, final DBObject o) {
        writeInt(0); // reserved
        writeCString(_collection.getFullName());

        int flags = 0;
        if ( upsert ) flags |= 1;
        if ( multi ) flags |= 2;
        writeInt(flags);

        putObject(query);
        putObject(o);
    }

    private void writeRemove() {
        writeInt(0); // reserved
        writeCString(_collection.getFullName());

        Collection<String> keys = _query.keySet();

        if ( keys.size() == 1 && keys.iterator().next().equals( "_id" ) && _query.get( keys.iterator().next() ) instanceof ObjectId)
            writeInt( 1 );
        else
            writeInt( 0 );

        putObject(_query);
    }

    private void writeGetMore(final long cursorId, final int batchSize) {
        writeInt(0);
        writeCString(_collection.getFullName());
        writeInt(batchSize);
        writeLong(cursorId);
    }

    private void writeKillCursorsPrologue(final int numCursors) {
        writeInt(0); // reserved
        writeInt(numCursors);
    }

    private void writeQuery(final DBObject fields, final int numToSkip, final int batchSize) {
        writeInt(_queryOptions);
        writeCString(_collection.getFullName());

        writeInt(numToSkip);
        writeInt(batchSize);

        putObject(_query);
        if (fields != null)
            putObject(fields);
    }

    private void writeMessagePrologue(final OpCode opCode) {
        writeInt( 0 ); // length: will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( opCode.getValue() );
    }

    void prepare(){
        if (_buffer == null) {
            throw new IllegalStateException("Already closed");
        }

        _buffer.writeInt( 0 , _buffer.size() );
    }

    void pipe( OutputStream out ) throws IOException {
        if (_buffer == null) {
            throw new IllegalStateException("Already closed");
        }

        _buffer.pipe( out );
    }

    int size() {
        if (_buffer == null) {
            throw new IllegalStateException("Already closed");
        }

        return _buffer.size();
    }

    void doneWithMessage() {
        if (_buffer == null) {
            throw new IllegalStateException("Only call this once per instance");
        }

        _buffer.reset();
        _mongo._bufferPool.done(_buffer);
        _buffer = null;
        done();
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

    int getNumDocuments() {
        return _numDocuments;
    }

    @Override
    public int putObject(BSONObject o) {
        if (_buffer == null) {
            throw new IllegalStateException("Already closed");
        }

        // check max size
        int objectSize = _encoder.writeObject(_buf, o);
        if (objectSize > Math.max(_mongo.getConnector().getMaxBsonObjectSize(), Bytes.MAX_OBJECT_SIZE)) {
            throw new MongoInternalException("DBObject of size " + objectSize + " is over Max BSON size " + _mongo.getMaxBsonObjectSize());
        }
        _numDocuments++;
        return objectSize;
    }

    private final Mongo _mongo;
    private final DBCollection _collection;
    private PoolOutputBuffer _buffer;
    private final int _id;
    private final OpCode _opCode;
    private final int _queryOptions;
    private final DBObject _query;
    private final DBEncoder _encoder;
    private volatile int _numDocuments; // only one thread will modify this field, so volatile is sufficient synchronization
}
