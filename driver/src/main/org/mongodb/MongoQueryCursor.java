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
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.Find;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.KillCursor;
import org.mongodb.session.ServerSelectingSession;
import org.mongodb.session.Session;
import org.mongodb.operation.GetMoreOperation;
import org.mongodb.operation.KillCursorOperation;
import org.mongodb.operation.QueryOperation;
import org.mongodb.operation.QueryOption;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.ServerCursor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mongodb.session.SessionBindingType.Connection;
import static org.mongodb.session.SessionBindingType.Server;

/**
 * @param <T> the document type of the cursor
 * @since 3.0
 */
@NotThreadSafe
public class MongoQueryCursor<T> implements MongoCursor<T> {
    private final Find find;
    private final Session session;
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final BufferPool<ByteBuffer> bufferPool;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;
    private long nextCount;
    private final List<Integer> sizes = new ArrayList<Integer>();
    private boolean closed;

    public MongoQueryCursor(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                            final Decoder<T> decoder, final ServerSelectingSession initialSession,
                            final BufferPool<ByteBuffer> bufferPool) {
        this.namespace = namespace;
        this.decoder = decoder;
        this.find = find;
        this.bufferPool = bufferPool;
        this.session = initialSession.getBoundSession(new ReadPreferenceServerSelector(find.getReadPreference()),
                find.getOptions().contains(QueryOption.Exhaust) ? Connection : Server);
        currentResult = new QueryOperation<T>(namespace, find, queryEncoder, decoder, this.bufferPool).execute(session);
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
        if (find.getOptions().contains(QueryOption.Exhaust)) {
            discardRemainingGetMoreResponses();
        }
        else if (currentResult.getCursor() != null && !limitReached()) {
            new KillCursorOperation(new KillCursor(currentResult.getCursor()), bufferPool).execute(session);
        }
        session.close();
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
    public Find getCriteria() {
        return new Find(find);
    }

    private void getMore() {
        final GetMoreOperation<T> getMoreOperation = new GetMoreOperation<T>(namespace,
                new GetMore(currentResult.getCursor(), find.getLimit(), find.getBatchSize(), nextCount), decoder, bufferPool);
        if (find.getOptions().contains(QueryOption.Exhaust)) {
            currentResult = getMoreOperation.executeReceive(session, currentResult.getRequestId());
        }
        else {
            currentResult = getMoreOperation.execute(session);
        }
        currentIterator = currentResult.getResults().iterator();
        sizes.add(currentResult.getResults().size());
        killCursorIfLimitReached();
    }

    private void discardRemainingGetMoreResponses() {
        new GetMoreOperation<T>(namespace, new GetMore(currentResult.getCursor(), find.getLimit(), find.getBatchSize(), nextCount),
                decoder, bufferPool).executeDiscard(session);
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
            new KillCursorOperation(new KillCursor(currentResult.getCursor()), bufferPool).execute(session);
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
