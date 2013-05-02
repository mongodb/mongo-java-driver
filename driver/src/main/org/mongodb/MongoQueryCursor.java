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

package org.mongodb;

import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.QueryOption;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @param <T> the document type of the cursor
 * @since 3.0
 */
@NotThreadSafe
public class MongoQueryCursor<T> implements MongoCursor<T> {
    private final MongoFind find;
    private final MongoSession connector;
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private final List<Integer> sizes = new ArrayList<Integer>();
    private boolean closed;

    public MongoQueryCursor(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                            final Decoder<T> decoder, final MongoSession connector) {
        this.namespace = namespace;
        this.decoder = decoder;
        this.find = find;
        this.connector = connector;
        currentResult = connector.query(namespace, find, queryEncoder, decoder);
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
        killCursorIfLimitReached();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (currentResult.getCursor() != null && !limitReached()) {
            connector.killCursors(new MongoKillCursor(currentResult.getCursor()));
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

        if (find.getLimit() > 0 && nextCount >= find.getLimit()) {
            return false;
        }

        if (currentResult.getCursor() == null) {
            return false;
        }

        if (isTailableAwait()) {
            return true;
        }

        getMore();

        return currentIterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        while (!currentIterator.hasNext() && isTailableAwait()) {
            getMore();
        }

        nextCount++;
        return currentIterator.next();
    }

    /**
     * Gets the cursor id.
     *
     * @return the cursor id
     */
    @Override
    public ServerCursor getServerCursor() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        return currentResult.getCursor();
    }

    public ServerAddress getServerAddress() {
        return currentResult.getAddress();
    }

    /**
     * Gets the number of OP_GET_MORE operations sent to the server so far.
     *
     * @return number of get more operations
     */
    public int getNumGetMores() {
        return sizes.size() - 1;
    }

    /**
     * Gets a list containing the number of items received in each batch
     *
     * @return the sizes of each batch
     */
    public List<Integer> getSizes() {
        return Collections.unmodifiableList(sizes);
    }

    /**
     * Gets the criteria for this query
     *
     * @return the criteria
     */
    public MongoFind getCriteria() {
        return new MongoFind(find);
    }

    private void getMore() {
        currentResult = connector.getMore(namespace,
                new MongoGetMore(currentResult.getCursor(),  find.getLimit(), find.getBatchSize(), nextCount), decoder);
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
        killCursorIfLimitReached();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }

    @Override
    public String toString() {
        return "MongoQueryCursor{namespace=" + namespace + ", find=" + find + ", cursor=" + currentResult.getCursor() + '}';
    }

    private void killCursorIfLimitReached() {
        if (limitReached()) {
            connector.killCursors(new MongoKillCursor(currentResult.getCursor()));
        }
    }

    private boolean limitReached() {
        return currentResult.getCursor() != null && find.getLimit() > 0
                && find.getLimit() - (nextCount + currentResult.getResults().size()) <= 0;
    }

    private boolean isTailableAwait() {
        return find.getOptions().containsAll(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData));
    }
}
