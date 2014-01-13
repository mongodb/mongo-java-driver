/*
 * Copyright (c) 2008 MongoDB, Inc.
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
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.GetMore;
import org.mongodb.protocol.GetMoreProtocol;
import org.mongodb.protocol.KillCursor;
import org.mongodb.protocol.KillCursorProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * @since 3.0
 */
@NotThreadSafe
public class AggregationCursor<T> implements MongoCursor<T> {
    private final Session session;
    private final boolean closeSession;
    private final AggregationOptions aggregationOptions;
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final BufferProvider bufferProvider;
    private final ServerConnectionProvider provider;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private boolean closed;

    public AggregationCursor(final AggregationOptions aggregationOptions, final MongoNamespace namespace, final Decoder<T> decoder,
                             final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                             final ServerConnectionProvider provider,
                             final QueryResult<T> batch) {
        this.aggregationOptions = aggregationOptions;
        this.namespace = namespace;
        this.decoder = decoder;
        this.bufferProvider = bufferProvider;
        this.session = session;
        this.closeSession = closeSession;
        this.provider = provider;
        currentResult = batch;
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (currentResult != null) {
            new KillCursorProtocol(new KillCursor(currentResult.getCursor()), bufferProvider, provider.getServerDescription(),
                                   getConnection(), false).execute();
        }
        if (closeSession) {
            session.close();
        }
        currentResult = null;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (currentIterator.hasNext()) {
            return true;
        }

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

        while (!currentIterator.hasNext()) {
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

    private void getMore() {
        currentResult = new GetMoreProtocol<T>(namespace, new AggregationGetMore(), decoder, bufferProvider,
                                               provider.getServerDescription(), getConnection(), true).execute();
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }

    @Override
    public String toString() {
        return "AggregationCursor{namespace=" + namespace + ", cursor=" + currentResult.getCursor() + '}';
    }

    private Connection getConnection() {
        return provider.getConnection();
    }

    private class AggregationGetMore extends GetMore {
        public AggregationGetMore() {
            super(currentResult.getCursor(), aggregationOptions.getBatchSize(),
                  aggregationOptions.getBatchSize(), nextCount);
        }

        @Override
        public int getNumberToReturn() {
            return getBatchSize();
        }
    }

    @Override
    public ServerAddress getServerAddress() {
        return provider.getServerDescription().getAddress();
    }
}
