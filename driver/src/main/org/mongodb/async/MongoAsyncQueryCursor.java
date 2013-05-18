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

package org.mongodb.async;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.AsyncSession;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.Server;
import org.mongodb.connection.SingleConnectionAsyncSession;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.connection.SingleServerAsyncSession;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.QueryOption;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.ServerCursor;
import org.mongodb.operation.async.AsyncGetMoreOperation;
import org.mongodb.operation.async.AsyncQueryOperation;

import java.nio.ByteBuffer;

// TODO: kill cursor on early breakout
// TODO: Report errors in callback
public class MongoAsyncQueryCursor<T> {
    private final MongoNamespace namespace;
    private final MongoFind find;
    private Encoder<Document> queryEncoder;
    private final Decoder<T> decoder;
    private final AsyncBlock<? super T> block;
    private final AsyncSession session;
    private long numFetchedSoFar;
    private ServerCursor cursor;

    public MongoAsyncQueryCursor(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                                 final Decoder<T> decoder, final AsyncSession initialSession, final AsyncBlock<? super T> block) {
        this.namespace = namespace;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.decoder = decoder;
        this.block = block;
        final AsyncConnection connection = initialSession.getConnection(new ReadPreferenceServerSelector(find.getReadPreference()));
        final Server server = initialSession.getCluster().getServer(connection.getServerAddress());

        if (find.getOptions().contains(QueryOption.Exhaust)) {
            this.session = new SingleConnectionAsyncSession(connection, initialSession.getCluster());
        }
        else {
            this.session = new SingleServerAsyncSession(server, initialSession.getCluster());
        }
    }

    public void start() {
        new AsyncQueryOperation<T>(namespace, find, queryEncoder, decoder, getBufferPool()).execute(session)
                .register(new QueryResultSingleResultCallback());

    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return session.getCluster().getBufferPool();
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                close();
                block.done();
            }
            else {
                cursor = result.getCursor();
            }

            boolean breakEarly = false;

            for (final T cur : result.getResults()) {
                numFetchedSoFar++;
                if (find.getLimit() > 0 && numFetchedSoFar > find.getLimit()) {
                    breakEarly = true;
                    break;
                }

                if (!block.run(cur)) {
                    breakEarly = true;
                    break;
                }
            }

            if (result.getCursor() == null || breakEarly) {
                close();
                block.done();
            }
            else {
                // get more results
                AsyncGetMoreOperation<T> getMoreOperation = new AsyncGetMoreOperation<T>(namespace,
                        new MongoGetMore(result.getCursor(), find.getLimit(), find.getBatchSize(), numFetchedSoFar), decoder,
                        getBufferPool());
                if (find.getOptions().contains(QueryOption.Exhaust)) {
                    getMoreOperation.executeReceive(session, result.getRequestId()).register(this);
                }
                else {
                    getMoreOperation.execute(session).register(this);
                }
            }
        }

        private void close() {
            if (find.getOptions().contains(QueryOption.Exhaust)) {
                handleExhaustCleanup();
            }
            else {
                session.close();
            }
        }

        private void handleExhaustCleanup() {
            MongoFuture<Void> future = new AsyncGetMoreOperation<T>(namespace, new MongoGetMore(cursor, find.getLimit(),
                    find.getBatchSize(),
                    numFetchedSoFar), null, getBufferPool()).executeDiscard(session);
            future.register(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    session.close();
                }
            });
        }
    }
}
