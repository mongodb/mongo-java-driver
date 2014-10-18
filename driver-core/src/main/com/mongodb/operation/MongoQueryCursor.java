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

package com.mongodb.operation;

import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryResult;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;

@NotThreadSafe
class MongoQueryCursor<T> implements MongoCursor<T> {
    private final Connection exhaustConnection;
    private final MongoNamespace namespace;
    private final int limit;
    private final int batchSize;
    private final Decoder<T> decoder;
    private final ConnectionSource source;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private final List<Integer> sizes = new ArrayList<Integer>();
    private boolean closed;

    // For normal queries
    MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                     final Decoder<T> decoder, final ConnectionSource source) {
        this(namespace, firstBatch, limit, batchSize, decoder, source, null);
    }

    // For exhaust queries
    MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                     final Decoder<T> decoder, final Connection exhaustConnection) {
        this(namespace, firstBatch, limit, batchSize, decoder, null, exhaustConnection);
    }

    private MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch,
                             final int limit, final int batchSize, final Decoder<T> decoder, final ConnectionSource source,
                             final Connection exhaustConnection) {
        this.namespace = namespace;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.source = source;
        if (this.source != null) {
            this.source.retain();
        }
        this.exhaustConnection = exhaustConnection;
        if (this.exhaustConnection != null) {
            this.exhaustConnection.retain();
        }
        this.currentResult = firstBatch;
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
        if (limitReached()) {
            killCursor();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (isExhaust()) {
            discardRemainingGetMoreResponses();
        } else if (!limitReached()) {
            try {
                killCursor();
            } finally {
                source.release();
            }
        }
        closed = true;
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

        if (limit > 0 && nextCount >= limit) {
            return false;
        }

        while (currentResult.getCursor() != null) {
            getMore();
            if (currentIterator.hasNext()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public T next() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        nextCount++;
        return currentIterator.next();
    }

    /**
     * A special {@code next()} case that returns the next element in the iteration if available or null.
     *
     * <p>Note: Tailable cursors that don't have the {@see com.mongodb.Bytes#QUERYOPTION_AWAITDATA} query flag set either via
     * {@see com.mongodb.client.model.FindOptions#awaitData} or via {@see com.mongodb.DBCursor#addOption} should
     * use this method rather than iterating via {@code next()} as that may throw a {@code NoSuchElementException} even though in the
     * future there may be more results available.</p>
     *
     * @return the next element in the iteration if available or null.
     */
    @Override
    public T tryNext() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (!tryHasNext()) {
            return null;
        }
        return next();
    }

    boolean tryHasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (currentIterator.hasNext()) {
            return true;
        }

        if (limit > 0 && nextCount >= limit) {
            return false;
        }

        if (currentResult.getCursor() != null) {
            getMore();
        }

        return currentIterator.hasNext();
    }

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

    private void getMore() {
        if (isExhaust()) {
            currentResult = exhaustConnection.getMoreReceive(decoder, currentResult.getRequestId());
        } else {
            Connection connection = source.getConnection();
            try {
                currentResult = connection.getMore(namespace, new GetMore(currentResult.getCursor(), limit, batchSize, nextCount), decoder);
                if (limitReached()) {
                    killCursor(connection);
                }
            } finally {
                connection.release();
            }
        }
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }

    private void killCursor() {
        if (currentResult.getCursor() != null) {
            Connection connection = source.getConnection();
            try {
                killCursor(connection);
            } finally {
                connection.release();
            }
        }
    }

    private void killCursor(final Connection connection) {
        if (currentResult.getCursor() != null) {
            connection.killCursor(asList(currentResult.getCursor()));
        }
    }

    private boolean limitReached() {
        return currentResult.getCursor() != null
               && limit > 0
               && limit - (nextCount + currentResult.getResults().size()) <= 0;
    }

    private void discardRemainingGetMoreResponses() {
        try {
            if (currentResult.getCursor() != null) {
                exhaustConnection.getMoreDiscard(currentResult.getCursor().getId(), currentResult.getRequestId());
            }
        } finally {
            exhaustConnection.release();
        }
    }

    private boolean isExhaust() {
        return exhaustConnection != null;
    }
}
