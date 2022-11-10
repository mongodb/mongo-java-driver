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
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
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
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    public ListCollectionsOperation(final String databaseName, final Decoder<T> decoder) {
        this.databaseName = notNull("databaseName", databaseName);
        this.decoder = notNull("decoder", decoder);
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public boolean isNameOnly() {
        return nameOnly;
    }

    public ListCollectionsOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public ListCollectionsOperation<T> nameOnly(final boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public ListCollectionsOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public ListCollectionsOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListCollectionsOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    @Nullable
    public BsonValue getComment() {
        return comment;
    }

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
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), binding.getSessionContext()));
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
                                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(),
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
        return (result, source, connection) -> cursorDocumentToAsyncBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> commandTransformer() {
        return (result, source, connection) -> cursorDocumentToBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
    }

    private CommandCreator getCommandCreator() {
        return (serverDescription, connectionDescription) -> getCommand();
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
