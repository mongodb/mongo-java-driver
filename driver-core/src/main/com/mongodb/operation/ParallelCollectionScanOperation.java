/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.session.SessionContext;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.validateReadConcern;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationReadConcernHelper.appendReadConcernToCommand;

/**
 * Return a list of cursors over the collection that can be used to scan it in parallel.
 *
 * <p> Note: As of MongoDB 2.6, this operation will work against a mongod, but not a mongos. </p>
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual reference/command/parallelCollectionScan/ parallelCollectionScan
 * @mongodb.server.release 2.6
 * @since 3.0
 */
public class
ParallelCollectionScanOperation<T> implements AsyncReadOperation<List<AsyncBatchCursor<T>>>,
                                                           ReadOperation<List<BatchCursor<T>>> {
    private final MongoNamespace namespace;
    private final int numCursors;
    private int batchSize = 0;
    private final Decoder<T> decoder;
    private ReadConcern readConcern = ReadConcern.DEFAULT;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param numCursors The maximum number of cursors to return. Must be between 1 and 10000, inclusive.
     * @param decoder the decoder for the result documents.

     */
    public ParallelCollectionScanOperation(final MongoNamespace namespace, final int numCursors, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        isTrue("numCursors >= 1", numCursors >= 1);
        this.numCursors = numCursors;
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the number of cursors requested.
     *
     * @return number of cursors requested.
     */
    public int getNumCursors() {
        return numCursors;
    }

    /**
     * Gets the batch size to use for each cursor.  The default value is 0, which tells the server to use its own default batch size.
     *
     * @return batch size
     * @mongodb.driver.manual core/cursors/#cursor-batches BatchSize
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * The batch size to use for each cursor.
     *
     * @param batchSize the batch size, which must be greater than or equal to  0
     * @return this
     * @mongodb.driver.manual core/cursors/#cursor-batches BatchSize
     */
    public ParallelCollectionScanOperation<T> batchSize(final int batchSize) {
        isTrue("batchSize >= 0", batchSize >= 0);
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @since 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     * @param readConcern the read concern
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ParallelCollectionScanOperation<T> readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
        return this;
    }

    @Override
    public List<BatchCursor<T>> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<List<BatchCursor<T>>>() {
            @Override
            public List<BatchCursor<T>> call(final ConnectionSource source, final Connection connection) {
                validateReadConcern(connection, readConcern);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(binding.getSessionContext()),
                                                     CommandResultDocumentCodec.create(decoder, "firstBatch"), connection,
                                                     transformer(source));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<List<AsyncBatchCursor<T>>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<List<AsyncBatchCursor<T>>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<List<AsyncBatchCursor<T>>> wrappedCallback = releasingCallback(
                            errHandlingCallback, source, connection);
                    validateReadConcern(source, connection, readConcern, new AsyncCallableWithConnectionAndSource() {
                        @Override
                        public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                        getCommand(binding.getSessionContext()),
                                        CommandResultDocumentCodec.create(decoder, "firstBatch"), connection,
                                        asyncTransformer(source, connection), wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    private CommandTransformer<BsonDocument, List<BatchCursor<T>>> transformer(final ConnectionSource source) {
        return new CommandTransformer<BsonDocument, List<BatchCursor<T>>>() {
            @Override
            public List<BatchCursor<T>> apply(final BsonDocument result, final ServerAddress serverAddress) {
                List<BatchCursor<T>> cursors = new ArrayList<BatchCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(result)) {
                    cursors.add(new QueryBatchCursor<T>(createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                     source.getServerDescription().getAddress()),
                                                        0, getBatchSize(), decoder, source));
                }
                return cursors;
            }
        };
    }

    private CommandTransformer<BsonDocument, List<AsyncBatchCursor<T>>> asyncTransformer(final AsyncConnectionSource source,
                                                                               final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, List<AsyncBatchCursor<T>>>() {
            @Override
            public List<AsyncBatchCursor<T>> apply(final BsonDocument result, final ServerAddress serverAddress) {
                List<AsyncBatchCursor<T>> cursors = new ArrayList<AsyncBatchCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(result)) {
                    cursors.add(new AsyncQueryBatchCursor<T>(createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                          source.getServerDescription().getAddress()),
                                                             0, getBatchSize(), 0, decoder, source, connection));
                }
                return cursors;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private BsonArray getCursorDocuments(final BsonDocument result) {
        return result.getArray("cursors");
    }

    private BsonDocument getCursorDocument(final BsonDocument cursorDocument) {
        return cursorDocument.getDocument("cursor");
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress);
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("parallelCollectionScan", new BsonString(namespace.getCollectionName()))
               .append("numCursors", new BsonInt32(getNumCursors()));
        appendReadConcernToCommand(sessionContext, document);
        return document;
    }
}
