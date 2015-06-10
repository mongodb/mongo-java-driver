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

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static java.util.Collections.singletonList;

class QueryBatchCursor<T> implements BatchCursor<T> {
    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final ConnectionSource connectionSource;
    private int batchSize;
    private ServerCursor serverCursor;
    private List<T> nextBatch;
    private int count;
    private boolean closed;

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final Decoder<T> decoder) {
        this(firstQueryResult, limit, batchSize, decoder, (ConnectionSource) null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize,
                     final Decoder<T> decoder, final ConnectionSource connectionSource) {
        this.namespace = firstQueryResult.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = notNull("decoder", decoder);
        if (firstQueryResult.getCursor() != null) {
            notNull("connectionSource", connectionSource);
        }
        if (connectionSource != null) {
            this.connectionSource = connectionSource.retain();
        } else {
            this.connectionSource = null;
        }

        initFromQueryResult(firstQueryResult);
        if (limitReached()) {
            killCursor();
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        while (serverCursor != null) {
            getMore();
            if (nextBatch != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<T> next() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        return retVal;
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            killCursor();
        } finally {
            if (connectionSource != null) {
                connectionSource.release();
            }
        }

        closed = true;
    }

    @Override
    public List<T> tryNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (!tryHasNext()) {
            return null;
        }
        return next();
    }

    boolean tryHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        if (serverCursor != null) {
            getMore();
        }

        return nextBatch != null;
    }

    @Override
    public ServerCursor getServerCursor() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        return serverCursor;
    }

    @Override
    public ServerAddress getServerAddress() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        return connectionSource.getServerDescription().getAddress();
    }

    private void getMore() {
        Connection connection = connectionSource.getConnection();
        try {
            QueryResult<T> nextQueryResult = connection.getMore(namespace, serverCursor.getId(),
                                                                getNumberToReturn(limit, batchSize, count),
                                                                decoder);
            initFromQueryResult(nextQueryResult);
            if (limitReached()) {
                killCursor(connection);
            }
        } finally {
            connection.release();
        }
    }

    private void initFromQueryResult(final QueryResult<T> queryResult) {
        serverCursor = queryResult.getCursor();
        nextBatch = queryResult.getResults().isEmpty() ? null : queryResult.getResults();
        count += queryResult.getResults().size();
    }

    private boolean limitReached() {
        return limit != 0 && count >= limit;
    }

    private void killCursor() {
        if (serverCursor != null) {
            Connection connection = connectionSource.getConnection();
            try {
                killCursor(connection);
            } finally {
                connection.release();
            }
        }
    }

    private void killCursor(final Connection connection) {
        if (serverCursor != null) {
            connection.killCursor(singletonList(serverCursor.getId()));
            serverCursor = null;
        }
    }
}
