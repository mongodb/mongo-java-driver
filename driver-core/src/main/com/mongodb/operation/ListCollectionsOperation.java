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

import com.mongodb.Function;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.operation.OperationHelper.createEmptyBatchCursor;
import static com.mongodb.operation.OperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.operation.OperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotEight;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that provides a cursor allowing iteration through the metadata of all the collections in a database.  This operation
 * ensures that the value of the {@code name} field of each returned document is the simple name of the collection rather than the full
 * namespace.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class ListCollectionsOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final String databaseName;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param decoder the decoder to use for the results
     */
    public ListCollectionsOperation(final String databaseName, final Decoder<T> decoder) {
        this.databaseName = notNull("databaseName", databaseName);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    try {
                        return executeWrappedCommandProtocol(databaseName, getCommand(), createCommandDecoder(), binding,
                                                             commandTransformer(source));
                    } catch (MongoCommandException e) {
                        return rethrowIfNotNamespaceError(e, createEmptyBatchCursor(createNamespace(), decoder,
                                                                                    source.getServerDescription().getAddress()));
                    }
                } else {
                    return new FilteringBatchCursor(new QueryBatchCursor<BsonDocument>(connection.query(getNamespace(), new BsonDocument(),
                                                                                                         null, 0, 0,
                                                                                                         binding.getReadPreference()
                                                                                                                .isSlaveOk(),
                                                                                                         false, false, false, false, false,
                                                                                                         new BsonDocumentCodec()),
                                                                                        0, 0, new BsonDocumentCodec(), source));
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Connection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errorHandlingCallback(callback),
                                                                                                        source, connection);
                    if (serverIsAtLeastVersionTwoDotEight(connection)) {
                        executeWrappedCommandProtocolAsync(databaseName, getCommand(), createCommandDecoder(), connection,
                                                           binding.getReadPreference(), asyncTransformer(source),
                                                           new SingleResultCallback<AsyncBatchCursor<T>>() {
                                                               @Override
                                                               public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                                                                   if (t != null && !isNamespaceError(t)) {
                                                                       wrappedCallback.onResult(null, t);
                                                                   } else {
                                                                       wrappedCallback.onResult(result != null
                                                                                                ? result : emptyAsyncCursor(source),
                                                                                                null);
                                                                   }
                                                               }
                                                           });
                    } else {
                        connection.queryAsync(getNamespace(), new BsonDocument(), null, 0, 0,
                                              binding.getReadPreference().isSlaveOk(), false,
                                              false, false, false, false,
                                              new BsonDocumentCodec(), new SingleResultCallback<QueryResult<BsonDocument>>() {
                            @Override
                            public void onResult(final QueryResult<BsonDocument> result, final Throwable t) {
                                if (t != null) {
                                    wrappedCallback.onResult(null, t);
                                } else {
                                    AsyncBatchCursor<T> cursor =
                                    new FilteringAsyncBatchCursor(new AsyncQueryBatchCursor<BsonDocument>(result, 0, 0,
                                                                                                           new BsonDocumentCodec(),
                                                                                                           source));
                                    wrappedCallback.onResult(cursor, null);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    private AsyncBatchCursor<T> emptyAsyncCursor(final AsyncConnectionSource source) {
        return createEmptyAsyncBatchCursor(createNamespace(), decoder, source.getServerDescription().getAddress());
    }

    private MongoNamespace createNamespace() {
        return new MongoNamespace(databaseName, "$cmd.listCollections");
    }

    private Function<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnectionSource source) {
        return new Function<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result) {
                return cursorDocumentToAsyncBatchCursor(result.getDocument("cursor"), decoder, source);
            }
        };
    }

    private Function<BsonDocument, BatchCursor<T>> commandTransformer(final ConnectionSource source) {
        return new Function<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result) {
                return cursorDocumentToBatchCursor(result.getDocument("cursor"), decoder, source);
            }
        };
    }


    private MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "system.namespaces");
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listCollections", new BsonInt32(1)).append("cursor", new BsonDocument());
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }

    private final class FilteringBatchCursor implements BatchCursor<T> {

        private final BatchCursor<BsonDocument> delegate;

        private FilteringBatchCursor(final BatchCursor<BsonDocument> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public List<T> next() {
            return fixAndFilterDocumentsFromSystemNamespace(delegate.next());
        }

        @Override
        public void setBatchSize(final int batchSize) {
            delegate.setBatchSize(batchSize);
        }

        @Override
        public int getBatchSize() {
            return delegate.getBatchSize();
        }

        @Override
        public List<T> tryNext() {
            return fixAndFilterDocumentsFromSystemNamespace(delegate.tryNext());
        }

        @Override
        public ServerCursor getServerCursor() {
            return delegate.getServerCursor();
        }

        @Override
        public ServerAddress getServerAddress() {
            return delegate.getServerAddress();
        }

    }

    private final class FilteringAsyncBatchCursor implements AsyncBatchCursor<T> {

        private final AsyncBatchCursor<BsonDocument> delegate;

        private FilteringAsyncBatchCursor(final AsyncBatchCursor<BsonDocument> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void next(final SingleResultCallback<List<T>> callback) {
            delegate.next(new SingleResultCallback<List<BsonDocument>>() {
                @Override
                public void onResult(final List<BsonDocument> result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(fixAndFilterDocumentsFromSystemNamespace(result), null);
                    }
                }
            });
        }

        @Override
        public void setBatchSize(final int batchSize) {
            delegate.setBatchSize(batchSize);
        }

        @Override
        public int getBatchSize() {
            return delegate.getBatchSize();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    // Skip documents whose collection name contains a '$'
    // For all others, remove database prefix from the value of the name field
    private List<T> fixAndFilterDocumentsFromSystemNamespace(final List<BsonDocument> unstripped) {
        if (unstripped == null) {
            return null;
        }
        List<T> stripped = new ArrayList<T>(unstripped.size());
        String prefix = databaseName + ".";
        for (BsonDocument cur : unstripped) {
            String name = cur.getString("name").getValue();
            if (name.startsWith(prefix)) {
                String collectionName = name.substring(prefix.length());
                if (!collectionName.contains("$")) {
                    cur.put("name", new BsonString(collectionName));
                    stripped.add(decoder.decode(new BsonDocumentReader(cur), DecoderContext.builder().build()));
                }
            }
        }
        return stripped;
    }
}
