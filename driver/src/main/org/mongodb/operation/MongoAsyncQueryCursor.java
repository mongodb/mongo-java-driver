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

package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.async.AsyncBlock;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Channel;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.GetMoreDiscardProtocol;
import org.mongodb.protocol.GetMoreProtocol;
import org.mongodb.protocol.GetMoreReceiveProtocol;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerChannelProvider;
import org.mongodb.session.ServerChannelProviderOptions;
import org.mongodb.session.Session;

// TODO: kill cursor on early breakout
// TODO: Report errors in callback
class MongoAsyncQueryCursor<T> implements MongoAsyncCursor<T> {
    private final MongoNamespace namespace;
    private final Find find;
    private Encoder<Document> queryEncoder;
    private final Decoder<T> decoder;
    private final BufferProvider bufferProvider;
    private final Session session;
    private ServerChannelProvider serverChannelProvider;
    private final boolean closeSession;
    private Channel exhaustChannel;
    private long numFetchedSoFar;
    private ServerCursor cursor;
    private AsyncBlock<? super T> block;

    public MongoAsyncQueryCursor(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                                 final Decoder<T> decoder, final BufferProvider bufferProvider,
                                 final Session session, final boolean closeSession) {
        this.namespace = namespace;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.decoder = decoder;
        this.bufferProvider = bufferProvider;
        this.session = session;
        this.closeSession = closeSession;
    }

    @Override
    public void start(final AsyncBlock<? super T> aBlock) {
        this.block = aBlock;
        session.createServerChannelProviderAsync(new ServerChannelProviderOptions(true,
                new ReadPreferenceServerSelector(find.getReadPreference()))).register(new SingleResultCallback<ServerChannelProvider>() {
            @Override
            public void onResult(final ServerChannelProvider provider, final MongoException e) {
                if (e != null) {
                    close(0);
                }
                else {
                    serverChannelProvider = provider;
                    provider.getChannelAsync().register(new SingleResultCallback<Channel>() {
                        @Override
                        public void onResult(final Channel channel, final MongoException e) {
                            if (e != null) {
                                close(0);
                            }
                            else {
                                if (isExhaust()) {
                                    exhaustChannel = channel;
                                }
                                new QueryProtocol<T>(namespace, find, queryEncoder, decoder, bufferProvider,
                                        provider.getServerDescription(), channel, !isExhaust()).executeAsync()
                                        .register(new QueryResultSingleResultCallback());
                            }
                        }
                    });
                }

            }
        });
    }

    private void close(final int responseTo) {
        if (find.getOptions().getFlags().contains(QueryFlag.Exhaust)) {
            handleExhaustCleanup(responseTo);
        }
        else {
            if (closeSession) {
                session.close();
            }
            block.done();
        }
    }

    private boolean isExhaust() {
        return find.getOptions().getFlags().contains(QueryFlag.Exhaust);
    }

    private void handleExhaustCleanup(final int responseTo) {
        new GetMoreDiscardProtocol(cursor != null ? cursor.getId() : 0, responseTo, exhaustChannel).executeAsync()
                .register(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final MongoException e) {
                        if (closeSession) {
                            session.close();
                        }
                        block.done();
                    }
                });
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
                if (find.getOptions().getFlags().contains(QueryFlag.Exhaust)) {
                    new GetMoreReceiveProtocol<T>(decoder, result.getRequestId(), exhaustChannel).executeAsync().register(this);
                }
                else {
                    serverChannelProvider.getChannelAsync().register(new SingleResultCallback<Channel>() {
                        @Override
                        public void onResult(final Channel channel, final MongoException e) {
                            if (e != null) {
                                close(0);
                            }
                            else {
                                new GetMoreProtocol<T>(namespace, new GetMore(result.getCursor(), find.getLimit(), find.getBatchSize(),
                                        numFetchedSoFar), decoder, bufferProvider, serverChannelProvider.getServerDescription(), channel,
                                        true).executeAsync().register(QueryResultSingleResultCallback.this);
                            }
                        }
                    });
                }
            }
        }
    }
}
