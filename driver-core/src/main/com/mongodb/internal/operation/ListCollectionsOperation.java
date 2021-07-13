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

package com.mongodb.internal.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.createReadCommandAndExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.createReadCommandAndExecuteAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.decorateReadWithRetries;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CursorHelper.getCursorDocumentFromBatchSize;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.createEmptyBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.OperationHelper.withSourceAndConnection;

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
    private boolean retryReads;
    private BsonDocument filter;
    private int batchSize;
    private long maxTimeMS;
    private boolean nameOnly;
    private BsonValue comment;

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

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Gets whether only the collection names should be returned.
     *
     * @return true if only the collection names should be returned
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public boolean isNameOnly() {
        return nameOnly;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public ListCollectionsOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Sets the query filter to apply to the query.
     * <p>
     *     Note: this is advisory only, and should be considered an optimization.  Server versions prior to MongoDB 4.0 will ignore
     *     this request.
     * </p>
     *
     * @param nameOnly true if only the collection names should be requested from the server
     * @return this
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public ListCollectionsOperation<T> nameOnly(final boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.
     *
     * @return the batch size
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ListCollectionsOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public ListCollectionsOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ListCollectionsOperation<T> retryReads(final boolean retryReads) {
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

    /**
     * @return the comment for this operation. A null value means no comment is set.
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public ListCollectionsOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        RetryState retryState = initialRetryState(retryReads);
        Supplier<BatchCursor<T>> read = decorateReadWithRetries(retryState, () -> {
            logRetryExecute(retryState);
            return withSourceAndConnection(binding::getReadConnectionSource, false, (source, connection) -> {
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), connection.getDescription(),
                        binding.getSessionContext()));
                try {
                    return createReadCommandAndExecute(retryState, binding, source, databaseName, getCommandCreator(),
                            createCommandDecoder(), commandTransformer(), connection);
                } catch (MongoCommandException e) {
                    return rethrowIfNotNamespaceError(e, createEmptyBatchCursor(createNamespace(), decoder,
                            source.getServerDescription().getAddress(), batchSize));
                }
            });
        });
        return read.get();
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        RetryState retryState = initialRetryState(retryReads);
        binding.retain();
        AsyncCallbackSupplier<AsyncBatchCursor<T>> asyncRead = CommandOperationHelper.<AsyncBatchCursor<T>>decorateReadWithRetries(
                retryState, funcCallback -> {
                    logRetryExecute(retryState);
                    withAsyncSourceAndConnection(binding::getReadConnectionSource, false, funcCallback,
                            (source, connection, releasingCallback) -> {
                                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), connection.getDescription(),
                                        binding.getSessionContext()), releasingCallback)) {
                                    return;
                                }
                                createReadCommandAndExecuteAsync(retryState, binding, source, databaseName, getCommandCreator(), createCommandDecoder(),
                                        asyncTransformer(), connection, (result, t) -> {
                                            if (t != null && !isNamespaceError(t)) {
                                                releasingCallback.onResult(null, t);
                                            } else {
                                                releasingCallback.onResult(result != null ? result : emptyAsyncCursor(source), null);
                                            }
                                        });
                            });
                }).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, LOGGER));
    }

    private AsyncBatchCursor<T> emptyAsyncCursor(final AsyncConnectionSource source) {
        return createEmptyAsyncBatchCursor(createNamespace(), source.getServerDescription().getAddress());
    }

    private MongoNamespace createNamespace() {
        return new MongoNamespace(databaseName, "$cmd.listCollections");
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final AsyncConnectionSource source,
                                             final AsyncConnection connection) {
                return cursorDocumentToAsyncBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
            }
        };
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> commandTransformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ConnectionSource source, final Connection connection) {
                return cursorDocumentToBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
            }
        };
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand();
            }
        };
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("listCollections", new BsonInt32(1))
                .append("cursor", getCursorDocumentFromBatchSize(batchSize == 0 ? null : batchSize));
        if (filter != null) {
            command.append("filter", filter);
        }
        if (nameOnly) {
            command.append("nameOnly", BsonBoolean.TRUE);
        }
        if (maxTimeMS > 0) {
            command.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        putIfNotNull(command, "comment", comment);
        return command;
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }
}
