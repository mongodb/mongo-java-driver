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
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.session.SessionContext;
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
import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.validateReadConcern;
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
@Deprecated
public class
ParallelCollectionScanOperation<T> implements AsyncReadOperation<List<AsyncBatchCursor<T>>>,
                                                           ReadOperation<List<BatchCursor<T>>> {
    private final MongoNamespace namespace;
    private final int numCursors;
    private boolean retryReads;
    private int batchSize = 0;
    private final Decoder<T> decoder;

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
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ParallelCollectionScanOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    @Override
    public List<BatchCursor<T>> execute(final ReadBinding binding) {
        return executeCommand(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                CommandResultDocumentCodec.create(decoder, "firstBatch"),
                transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<List<AsyncBatchCursor<T>>> callback) {
        executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                CommandResultDocumentCodec.create(decoder, "firstBatch"),
                asyncTransformer(), retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandReadTransformer<BsonDocument, List<BatchCursor<T>>> transformer() {
        return new CommandReadTransformer<BsonDocument, List<BatchCursor<T>>>() {
            @Override
            public List<BatchCursor<T>> apply(final BsonDocument result, final ConnectionSource source, final Connection connection) {
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

    private CommandReadTransformerAsync<BsonDocument, List<AsyncBatchCursor<T>>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, List<AsyncBatchCursor<T>>>() {
            @Override
            public List<AsyncBatchCursor<T>> apply(final BsonDocument result, final AsyncConnectionSource source,
                                                   final AsyncConnection connection) {
                List<AsyncBatchCursor<T>> cursors = new ArrayList<AsyncBatchCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(result)) {
                    cursors.add(new AsyncQueryBatchCursor<T>(createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                          source.getServerDescription().getAddress()),
                                                             0, getBatchSize(), 0, decoder, source, connection, result));
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

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                validateReadConcern(connectionDescription, sessionContext.getReadConcern());
                return getCommand(sessionContext);
            }
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("parallelCollectionScan", new BsonString(namespace.getCollectionName()))
               .append("numCursors", new BsonInt32(getNumCursors()));
        appendReadConcernToCommand(sessionContext, document);
        return document;
    }
}
