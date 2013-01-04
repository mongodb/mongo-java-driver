/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;

import java.util.Iterator;
import java.util.NoSuchElementException;

@NotThreadSafe
class MongoCollectionCursor<T> implements MongoCursor<T> {
    private final MongoCollection<T> collection;
    private final MongoFind find;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private boolean closed;

    public MongoCollectionCursor(final MongoCollection<T> collection, final MongoFind find) {
        this.collection = collection;
        this.find = find;
        currentResult = collection.getClient().getOperations().query(collection.getNamespace(), find,
                                                                     collection.getOptions().getDocumentSerializer(),
                                                                     collection.getSerializer());
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void close() {
        closed = true;
        if (currentResult != null && currentResult.getCursor() != null) {
            collection.getClient().getOperations().killCursors(new MongoKillCursor(currentResult.getCursor()));
        }
        currentResult = null;
        currentIterator = null;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (currentIterator.hasNext()) {
            return true;
        }

        if ( find.getLimit() > 0 && nextCount >= find.getLimit() )
            return false;

        if (currentResult.getCursor() == null) {
            return false;
        }

        getMore();
        return currentIterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        nextCount++;

        return currentIterator.next();
    }

    /**
     * Gets the cursor id.
     * @return the cursor id
     */
    @Override
    public ServerCursor getServerCursor() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        return currentResult.getCursor();
    }

    private void getMore() {
        currentResult = collection.getClient().
                getOperations().getMore(collection.getNamespace(),
                                        new GetMore(currentResult.getCursor(), find.getBatchSize()),
                                        collection.getSerializer());
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }

    @Override
    public String toString() {
        return "MongoCollectionCursor{" +
                "collection=" + collection +
                ", find=" + find +
                ", cursor=" + currentResult.getCursor() +
                '}';
    }
}
