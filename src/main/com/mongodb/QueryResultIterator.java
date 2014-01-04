/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;

class QueryResultIterator implements Iterator<DBObject> {

    private DBApiLayer db;

    QueryResultIterator(final DBApiLayer db, DBCollectionImpl coll, Response res, int batchSize, int limit,
                        int options, DBDecoder decoder){
        this.db = db;
        _collection = coll;
        _batchSize = batchSize;
        _limit = limit;
        _options = options;
        _host = res._host;
        _decoder = decoder;
        // Only enable finalizer if cursor finalization is enabled and there is actually a cursor that needs killing
        _optionalFinalizer = coll.getDB().getMongo().getMongoOptions().isCursorFinalizerEnabled() && res.cursor() != 0 ?
                new OptionalFinalizer() : null;
        init( res );
    }

    public DBObject next() {
        while (true) {
            if (!(hasNext() && !_cur.hasNext())) {
                break;
            }
        }

        if (!hasNext()) {
            throw new NoSuchElementException("no more documents in query result iterator");
        }

        return _cur.next();
    }

    public boolean hasNext(){
        if (_cur.hasNext()) {
            return true;
        }

        if (!hasGetMore()) {
            return false;
        }

        _advance();

        return _cur.hasNext() || awaitResultsFromTailable();
    }

    private void _advance(){
        if (_cursorId == 0) {
            throw new MongoInternalException("can't advance when there is no cursor id");
        }

        OutMessage m = OutMessage.getMore(_collection, _cursorId, DBApiLayer.chooseBatchSize(_batchSize, _limit, _numFetched));

        Response res = db._connector.call(_collection.getDB(), _collection, m, _host, _decoder);
        _numGetMores++;
        init( res );
    }

    public void remove(){
        throw new UnsupportedOperationException("can't remove a document via a query result iterator");
    }

    public void setBatchSize(int size){
        _batchSize = size;
    }

    public long getCursorId(){
        return _cursorId;
    }

    int numGetMores(){
        return _numGetMores;
    }

    List<Integer> getSizes(){
        return Collections.unmodifiableList(_sizes);
    }

    void close(){
        if (!closed) {
            closed = true;
            killCursor();
        }
    }

    private void init( Response res ){
        DBApiLayer.throwOnQueryFailure(res, _cursorId);

        _cursorId = res.cursor();
        _cur = res.iterator();
        _curSize = res.size();
        _sizes.add( res.size() );
        _numFetched += res.size();

        if (res._cursor != 0 && _limit > 0 && _limit - _numFetched <= 0) {
            // fetched all docs within limit, close cursor server-side
            killCursor();
        }
    }

    private boolean hasGetMore() {
        if (_cursorId == 0) {
            return false;
        }

        if (_curSize > 0) {
            return true;
        }

        if (!isTailable()) {
            return false;
        }

        // have a tailable cursor, it is always possible to call get more
        return true;
    }

    private boolean isTailable() {
        return (_options & Bytes.QUERYOPTION_TAILABLE) != 0;
    }

    private boolean awaitResultsFromTailable() {
        return isTailable() && (_options & Bytes.QUERYOPTION_AWAITDATA) != 0;
    }

    void killCursor() {
        if (_cursorId == 0)
            return;

        try {
            db.killCursors(_host, asList(_cursorId));
            _cursorId = 0;
        } catch (MongoException e) {
            db.addDeadCursor(new DBApiLayer.DeadCursor(_cursorId, _host));
        }
    }

    public ServerAddress getServerAddress() {
        return _host;
    }

    boolean hasFinalizer() {
        return _optionalFinalizer != null;
    }

    private final DBDecoder _decoder;
    private final DBCollectionImpl _collection;
    private final int _options;
    private final ServerAddress _host;
    private final int _limit;

    private long _cursorId;
    private Iterator<DBObject> _cur;
    private int _curSize;
    private int _batchSize;

    private boolean closed;

    private final List<Integer> _sizes = new ArrayList<Integer>();
    private int _numGetMores = 0;
    private int _numFetched = 0;

    // This allows us to easily enable/disable finalizer for cleaning up un-closed cursors
    private final OptionalFinalizer _optionalFinalizer;

    private class OptionalFinalizer {
        @Override
        protected void finalize() throws Throwable {
            if (!closed && _cursorId != 0) {
                db.addDeadCursor(new DBApiLayer.DeadCursor(_cursorId, _host));
            }
            super.finalize();
        }
    }

}
