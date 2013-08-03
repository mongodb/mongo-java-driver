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
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AsyncGetMoreDiscardOperation;
import org.mongodb.operation.AsyncGetMoreOperation;
import org.mongodb.operation.AsyncGetMoreReceiveOperation;
import org.mongodb.operation.AsyncQueryOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.protocol.QueryResult;
import org.mongodb.session.AsyncServerSelectingSession;
import org.mongodb.session.AsyncSession;

import static org.mongodb.session.SessionBindingType.Connection;
import static org.mongodb.session.SessionBindingType.Server;

// TODO: kill cursor on early breakout
// TODO: Report errors in callback
public class MongoAsyncQueryCursor<T> {
    private final MongoNamespace namespace;
    private final Find find;
    private final Decoder<T> decoder;
    private final BufferProvider bufferProvider;
    private final MongoFuture<AsyncSession> sessionFuture;
    private final AsyncBlock<? super T> block;
    private long numFetchedSoFar;
    private ServerCursor cursor;
    private final AsyncQueryOperation<T> operation;

    public MongoAsyncQueryCursor(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                                 final Decoder<T> decoder, final BufferProvider bufferProvider,
                                 final AsyncServerSelectingSession initialSession, final AsyncBlock<? super T> block) {
        this.namespace = namespace;
        this.find = find;
        this.decoder = decoder;
        this.bufferProvider = bufferProvider;
        this.block = block;
        this.operation = new AsyncQueryOperation<T>(namespace, find, queryEncoder, decoder, bufferProvider);
        this.sessionFuture = initialSession.getBoundSession(operation,
                find.getOptions().getFlags().contains(QueryFlag.Exhaust) ? Connection : Server);
    }

    public void start() {
        sessionFuture.register(new SingleResultCallback<AsyncSession>() {
            @Override
            public void onResult(final AsyncSession result, final MongoException e) {
                if (e != null) {
                   // TODO: report error to block
                }
                else {
                    result.execute(operation).register(new QueryResultSingleResultCallback(result));
                }
            }
        });
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private AsyncSession session;

        public QueryResultSingleResultCallback(final AsyncSession session) {
            this.session = session;
        }

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
                if (find.getOptions().getFlags().contains(QueryFlag.Exhaust)) {
                    session.execute(new AsyncGetMoreReceiveOperation<T>(decoder, result.getRequestId())).register(this);
                }
                else {
                    session.execute(
                            new AsyncGetMoreOperation<T>(namespace, new GetMore(result.getCursor(), find.getLimit(), find.getBatchSize(),
                                    numFetchedSoFar), decoder, bufferProvider))
                            .register(this);
                }
            }
        }

        private void close(final int responseTo) {
            if (find.getOptions().getFlags().contains(QueryFlag.Exhaust)) {
                handleExhaustCleanup(responseTo);
            }
            else {
                session.close();
                block.done();
            }
        }

        private void handleExhaustCleanup(final int responseTo) {
            MongoFuture<Void> future = session.execute(new AsyncGetMoreDiscardOperation(cursor != null ? cursor.getId() : 0, responseTo));
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
