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

package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.protocol.GetMoreDiscardProtocol;
import org.mongodb.protocol.GetMoreProtocol;
import org.mongodb.protocol.GetMoreReceiveProtocol;
import org.mongodb.protocol.KillCursor;
import org.mongodb.protocol.KillCursorProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@NotThreadSafe
class MongoQueryCursor<T> implements MongoCursor<T> {
    private final Connection exhaustConnection;
    private final MongoNamespace namespace;
    private final int limit;
    private final int batchSize;
    private final EnumSet<QueryFlag> queryFlags;
    private final Decoder<T> decoder;
    private final ServerConnectionProvider provider;
    private final ServerDescription serverDescription;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private final List<Integer> sizes = new ArrayList<Integer>();
    private boolean closed;

    // For normal queries
    MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                            final int limit, final int batchSize, final Decoder<T> decoder, final ServerConnectionProvider provider) {
        this(namespace, firstBatch, queryFlags, limit, batchSize, decoder, provider, null, null);
    }

    // For exhaust queries
    MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                            final int limit, final int batchSize, final Decoder<T> decoder, final Connection exhaustConnection,
                            final ServerDescription serverDescription) {
        this(namespace, firstBatch, queryFlags, limit, batchSize, decoder, null, exhaustConnection, serverDescription);
    }

    private MongoQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                            final int limit, final int batchSize, final Decoder<T> decoder, final ServerConnectionProvider provider,
                            final Connection exhaustConnection, final ServerDescription serverDescription) {
        this.namespace = namespace;
        this.limit = limit;
        this.batchSize = batchSize;
        this.queryFlags = queryFlags;
        this.decoder = decoder;
        this.provider = provider;
        this.exhaustConnection = exhaustConnection;
        this.currentResult = firstBatch;
        this.serverDescription = serverDescription != null ? serverDescription : provider.getServerDescription();
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
        if (isExhaust()) {
            discardRemainingGetMoreResponses();
        } else if (currentResult.getCursor() != null && !limitReached()) {
            Connection connection = getConnection();
            try {
                new KillCursorProtocol(new KillCursor(currentResult.getCursor())).execute(connection, provider.getServerDescription());
            } finally {
                if (!isExhaust()) {
                    connection.close();
                }
            }
        }
        if (exhaustConnection != null) {
            exhaustConnection.close();
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

    private void getMore() {
        if (isExhaust()) {
            currentResult = new GetMoreReceiveProtocol<T>(decoder, currentResult.getRequestId())
                            .execute(getConnection(), serverDescription);
        } else {
            Connection connection = getConnection();
            try {
                currentResult = new GetMoreProtocol<T>(namespace, new GetMore(currentResult.getCursor(), limit, batchSize, nextCount),
                                                       decoder)
                                .execute(connection, serverDescription);
            } finally {
                connection.close();
            }
        }
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
        killCursorIfLimitReached();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }

    ServerConnectionProvider getServerConnectionProvider() {
        return provider;
    }

    private void killCursorIfLimitReached() {
        if (limitReached()) {
            Connection connection = getConnection();
            try {
                new KillCursorProtocol(new KillCursor(currentResult.getCursor())).execute(connection, serverDescription);
            } finally {
                if (!isExhaust()) {
                    connection.close();
                }
            }
        }
    }

    private boolean limitReached() {
        return currentResult.getCursor() != null
               && limit > 0
               && limit - (nextCount + currentResult.getResults().size()) <= 0;
    }

    private void discardRemainingGetMoreResponses() {
        if (currentResult.getCursor() != null) {
            new GetMoreDiscardProtocol(currentResult.getCursor().getId(), currentResult.getRequestId())
            .execute(getConnection(), serverDescription);
        }
    }

    private boolean isExhaust() {
        return queryFlags.contains(QueryFlag.Exhaust);
    }

    private Connection getConnection() {
        if (isExhaust()) {
            return exhaustConnection;
        } else {
            return provider.getConnection();
        }
    }
}
