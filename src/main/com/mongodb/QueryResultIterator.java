/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

class QueryResultIterator implements Cursor {

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
    private OptionalFinalizer _optionalFinalizer;
    private boolean batchSizeTrackingDisabled;

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
        initFromQueryResponse(res);
    }

    // Constructor to use for aggregate queries
    QueryResultIterator(DBObject cursorDocument, DBApiLayer db, DBCollectionImpl collection, int batchSize, DBDecoder decoder,
                        final ServerAddress serverAddress) {
        this._db = db;
        _collection = collection;
        _batchSize = batchSize;
        _host = serverAddress;
        _limit = 0;
        _options = 0;
        _decoder = decoder;
        initFromCursorDocument(cursorDocument);
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
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return _cur.next();
    }

    public boolean tryHasNext() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (_cur.hasNext()) {
            return true;
        }

        if (_cursorId != 0) {
            getMore();
        }
        return _curSize > 0;

    }

    public boolean hasNext() {
        if (closed) {
           throw new IllegalStateException("Iterator has been closed");
        }

        if (_cur.hasNext()) {
            return true;
        }

        while (_cursorId != 0) {
            getMore();
            if (_curSize > 0) {
                return true;
            }
        }

        return false;
    }

    private void getMore(){
        Response res = _db._connector.call(_collection.getDB(), _collection,
                                           OutMessage.getMore(_collection, _cursorId, getGetMoreBatchSize()),
                                           _host, _decoder);
        _numGetMores++;
        initFromQueryResponse(res);
    }

    private int getGetMoreBatchSize() {
        return chooseBatchSize(_batchSize, _limit, _numFetched);
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

    private void initFromQueryResponse(final Response response) {
        init(response._flags, response.cursor(), response.size(), response.iterator());
    }

    @SuppressWarnings("unchecked")
    private void initFromCursorDocument(final DBObject cursorDocument) {
        Map cursor = (Map) cursorDocument.get("cursor");
        if (cursor != null) {
            long cursorId = (Long) cursor.get("id");
            List<DBObject> firstBatch = (List<DBObject>) cursor.get("firstBatch");
            init(0, cursorId, firstBatch.size(), firstBatch.iterator());
        } else {
            List<DBObject> result = (List<DBObject>) cursorDocument.get("result");
            init(0, 0, result.size(), result.iterator());
        }
    }

    private void init(int flags, long cursorId, int size, Iterator<DBObject> iterator){
        _curSize = size;
        _cur = iterator;
        if (!batchSizeTrackingDisabled) {
            _sizes.add(size);
        }
        _numFetched += size;

        if (_optionalFinalizer == null) {
            _optionalFinalizer = createFinalizerIfNeeded(cursorId);
        }

        setCursorIdOnFinalizer(cursorId);
        throwOnQueryFailure(_cursorId, flags);
        _cursorId = cursorId;


        if (cursorId != 0 && _limit > 0 && _limit - _numFetched <= 0) {
            // fetched all docs within limit, close cursor server-side
            killCursor();
        }
    }

    private void setCursorIdOnFinalizer(final long cursorId) {
        if (_optionalFinalizer != null) {
            _optionalFinalizer.setCursorId(cursorId);
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


    void killCursor() {
        setCursorIdOnFinalizer(0);

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

    void disableBatchSizeTracking() {
        batchSizeTrackingDisabled = true;
        _sizes.clear();
    }

    boolean hasFinalizer() {
        return _optionalFinalizer != null;
    }

    private OptionalFinalizer createFinalizerIfNeeded(final long cursorId) {
        return _collection.getDB().getMongo().getMongoOptions().isCursorFinalizerEnabled() && cursorId != 0 ?
               new OptionalFinalizer(_db, _host) : null;
    }

    private static class OptionalFinalizer {
        private final DBApiLayer db;
        private final ServerAddress host;
        private volatile long cursorId;

        private OptionalFinalizer(final DBApiLayer db, final ServerAddress host) {
            this.db = db;
            this.host = host;
        }

        public void setCursorId(final long cursorId) {
            this.cursorId = cursorId;
        }

        @Override
        protected void finalize() throws Throwable {
            if (cursorId != 0) {
                db.addDeadCursor(new DeadCursor(cursorId, host));
            }
        }
    }

}
