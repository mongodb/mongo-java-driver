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
import org.mongodb.operation.Find;
import org.mongodb.operation.GetMore;
import org.mongodb.session.AsyncServerSelectingSession;
import org.mongodb.session.AsyncSession;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.QueryOption;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.ServerCursor;
import org.mongodb.operation.AsyncGetMoreOperation;
import org.mongodb.operation.AsyncQueryOperation;

import java.nio.ByteBuffer;

import static org.mongodb.session.SessionBindingType.Connection;
import static org.mongodb.session.SessionBindingType.Server;

// TODO: kill cursor on early breakout
// TODO: Report errors in callback
public class MongoAsyncQueryCursor<T> {
    private final MongoNamespace namespace;
    private final Find find;
    private Encoder<Document> queryEncoder;
    private final Decoder<T> decoder;
    private final BufferPool<ByteBuffer> bufferPool;
    private final AsyncSession session;
    private final AsyncBlock<? super T> block;
    private long numFetchedSoFar;
    private ServerCursor cursor;

    public MongoAsyncQueryCursor(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                                 final Decoder<T> decoder, final BufferPool<ByteBuffer> bufferPool,
                                 final AsyncServerSelectingSession initialSession, final AsyncBlock<? super T> block) {
        this.namespace = namespace;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.decoder = decoder;
        this.bufferPool = bufferPool;
        this.block = block;
        this.session = initialSession.getBoundSession(new ReadPreferenceServerSelector(find.getReadPreference()),
                find.getOptions().contains(QueryOption.Exhaust) ? Connection : Server);
    }

    public void start() {
        new AsyncQueryOperation<T>(namespace, find, queryEncoder, decoder, bufferPool).execute(session)
                .register(new QueryResultSingleResultCallback());
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                close(0);
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
                close(result.getRequestId());
            }
            else {
                // get more results
                AsyncGetMoreOperation<T> getMoreOperation = new AsyncGetMoreOperation<T>(namespace,
                        new GetMore(result.getCursor(), find.getLimit(), find.getBatchSize(), numFetchedSoFar), decoder, bufferPool);
                if (find.getOptions().contains(QueryOption.Exhaust)) {
                    getMoreOperation.executeReceive(session, result.getRequestId()).register(this);
                }
                else {
                    getMoreOperation.execute(session).register(this);
                }
            }
        }

        private void close(final int responseTo) {
            if (find.getOptions().contains(QueryOption.Exhaust)) {
                handleExhaustCleanup(responseTo);
            }
            else {
                session.close();
                block.done();
            }
        }

        private void handleExhaustCleanup(final int responseTo) {
            MongoFuture<Void> future = new AsyncGetMoreOperation<T>(namespace, new GetMore(cursor, find.getLimit(),
                    find.getBatchSize(),
                    numFetchedSoFar), null, bufferPool).executeDiscard(session, responseTo);
            future.register(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    session.close();
                    block.done();
                }
            });
        }
    }
}
