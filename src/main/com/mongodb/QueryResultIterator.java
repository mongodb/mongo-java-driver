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

import org.bson.BSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.mongodb.DBApiLayer.DeadCursor;
import static java.util.Arrays.asList;

class QueryResultIterator implements MongoCursor {

    private final DBApiLayer _db;
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

    // Constructor to use for normal queries
    QueryResultIterator(DBApiLayer db, DBCollectionImpl collection, Response res, int batchSize, int limit,
                        int options, DBDecoder decoder){
        this._db = db;
        _collection = collection;
        _batchSize = batchSize;
        _limit = limit;
        _options = options;
        _host = res._host;
        _decoder = decoder;
        init( res );
        _optionalFinalizer = getOptionalFinalizer(collection);
    }

    // Constructor to use for aggregate queries
    QueryResultIterator(CommandResult res, DBApiLayer db, DBCollectionImpl collection, int batchSize, DBDecoder decoder) {
        this._db = db;
        _collection = collection;
        _batchSize = batchSize;
        _host = res.getServerUsed();
        _limit = 0;
        _options = 0;
        _decoder = decoder;
        initFromAggregateResult(res);
        _optionalFinalizer = getOptionalFinalizer(collection);
    }

    static int chooseBatchSize(int batchSize, int limit, int fetched) {
        int bs = Math.abs(batchSize);
        int remaining = limit > 0 ? limit - fetched : 0;
        int res;
        if (bs == 0 && remaining > 0)
            res = remaining;
        else if (bs > 0 && remaining == 0)
            res = bs;
        else
            res = Math.min(bs, remaining);

        if (batchSize < 0) {
            // force close
            res = -res;
        }

        if (res == 1) {
            // optimization: use negative batch size to close cursor
            res = -1;
        }
        return res;
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

        OutMessage m = OutMessage.getMore(_collection, _cursorId, chooseBatchSize(_batchSize, _limit, _numFetched));

        Response res = _db._connector.call(_collection.getDB(), _collection, m, _host, _decoder);
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

    public void close(){
        if (!closed) {
            closed = true;
            killCursor();
        }
    }

    private void init(final Response response) {
        init(response._flags, response.cursor(), response.size(), response.iterator());
    }

    @SuppressWarnings("unchecked")
    private void initFromAggregateResult(final CommandResult res) {
        Map cursor = (Map) res.get("cursor");
        if (cursor != null) {
            long cursorId = (Long) cursor.get("id");
            List<DBObject> firstBatch = (List<DBObject>) cursor.get("firstBatch");
            init(0, cursorId, firstBatch.size(), firstBatch.iterator());
        } else {
            List<DBObject> result = (List<DBObject>) res.get("result");
            init(0, 0, result.size(), result.iterator());
        }
    }

    private void init(int flags, long cursorId, int size, Iterator<DBObject> iterator){
        _curSize = size;
        _cur = iterator;
        _sizes.add(size);
        _numFetched += size;

        throwOnQueryFailure(_cursorId, flags);
        _cursorId = cursorId;

        if (cursorId != 0 && _limit > 0 && _limit - _numFetched <= 0) {
            // fetched all docs within limit, close cursor server-side
            killCursor();
        }
    }

    private void throwOnQueryFailure(final long cursorId, int flags) {
        if ((flags & Bytes.RESULTFLAG_ERRSET) > 0) {
            BSONObject errorDocument = _cur.next();
            if (ServerError.getCode(errorDocument) == 50) {
                throw new MongoExecutionTimeoutException(ServerError.getCode(errorDocument),
                                                         ServerError.getMsg(errorDocument, null));
            } else {
                throw new MongoException(ServerError.getCode(errorDocument), ServerError.getMsg(errorDocument, null));
            }
        }
        else if ((flags & Bytes.RESULTFLAG_CURSORNOTFOUND) > 0) {
            throw new MongoException.CursorNotFound(cursorId, _host);
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
            _db.killCursors(_host, asList(_cursorId));
            _cursorId = 0;
        } catch (MongoException e) {
            _db.addDeadCursor(new DeadCursor(_cursorId, _host));
        }
    }

    public ServerAddress getServerAddress() {
        return _host;
    }

    boolean hasFinalizer() {
        return _optionalFinalizer != null;
    }

    private OptionalFinalizer getOptionalFinalizer(final DBCollectionImpl coll) {
        return coll.getDB().getMongo().getMongoOptions().isCursorFinalizerEnabled() && _cursorId != 0 ?
               new OptionalFinalizer() : null;
    }

    private class OptionalFinalizer {
        @Override
        protected void finalize() throws Throwable {
            if (!closed && _cursorId != 0) {
                _db.addDeadCursor(new DeadCursor(_cursorId, _host));
            }
            super.finalize();
        }
    }

}
