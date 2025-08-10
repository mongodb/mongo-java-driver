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
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.createReadCommandAndExecuteAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.decorateReadWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.AsyncSingleBatchCursor.createEmptyAsyncSingleBatchCursor;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CursorHelper.getCursorDocumentFromBatchSize;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.applyTimeoutModeToOperationContext;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.SingleBatchCursor.createEmptySingleBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.createReadCommandAndExecute;
import static com.mongodb.internal.operation.SyncOperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateReadWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.withSourceAndConnection;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ListIndexesOperation<T> implements ReadOperationCursor<T> {
    private static final String COMMAND_NAME = "listIndexes";
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private int batchSize;
    private BsonValue comment;
    private TimeoutMode timeoutMode = TimeoutMode.CURSOR_LIFETIME;

    public ListIndexesOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public ListIndexesOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public ListIndexesOperation<T> retryReads(final boolean retryReads) {
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

    public ListIndexesOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    public TimeoutMode getTimeoutMode() {
        return timeoutMode;
    }

    public ListIndexesOperation<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        if (timeoutMode != null) {
            this.timeoutMode = timeoutMode;
        }
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding, final OperationContext operationContext) {
        OperationContext listIndexesOperationContext = applyTimeoutModeToOperationContext(timeoutMode, operationContext);

        RetryState retryState = initialRetryState(retryReads, listIndexesOperationContext.getTimeoutContext());
        Supplier<BatchCursor<T>> read = decorateReadWithRetries(retryState, listIndexesOperationContext, () ->
            withSourceAndConnection(binding::getReadConnectionSource, false, (source, connection, operationContextWithMinRTT) -> {
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), operationContextWithMinRTT));
                try {
                    return createReadCommandAndExecute(retryState, operationContextWithMinRTT, source, namespace.getDatabaseName(),
                                                       getCommandCreator(), createCommandDecoder(), transformer(), connection);
                } catch (MongoCommandException e) {
                    return rethrowIfNotNamespaceError(e,
                            createEmptySingleBatchCursor(source.getServerDescription().getAddress(), batchSize));
                }
            }, listIndexesOperationContext)
        );
        return read.get();
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        OperationContext listIndexesOperationContext = applyTimeoutModeToOperationContext(timeoutMode, operationContext);

        RetryState retryState = initialRetryState(retryReads, operationContext.getTimeoutContext());
        binding.retain();
        AsyncCallbackSupplier<AsyncBatchCursor<T>> asyncRead = decorateReadWithRetriesAsync(
                retryState, listIndexesOperationContext, (AsyncCallbackSupplier<AsyncBatchCursor<T>>) funcCallback ->
                    withAsyncSourceAndConnection(binding::getReadConnectionSource, false, listIndexesOperationContext, funcCallback,
                            (source, connection, operationContextWithMinRtt, releasingCallback) -> {
                                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(),
                                        operationContextWithMinRtt), releasingCallback)) {
                                    return;
                                }
                                createReadCommandAndExecuteAsync(retryState, operationContextWithMinRtt, source,
                                        namespace.getDatabaseName(), getCommandCreator(), createCommandDecoder(),
                                        asyncTransformer(), connection,
                                        (result, t) -> {
                                            if (t != null && !isNamespaceError(t)) {
                                                releasingCallback.onResult(null, t);
                                            } else {
                                                releasingCallback.onResult(result != null
                                                        ? result : createEmptyAsyncSingleBatchCursor(getBatchSize()), null);
                                            }
                                        });
                            })
                ).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, LOGGER));
    }


    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument commandDocument = new BsonDocument(getCommandName(), new BsonString(namespace.getCollectionName()))
                    .append("cursor", getCursorDocumentFromBatchSize(batchSize == 0 ? null : batchSize));
            putIfNotNull(commandDocument, "comment", comment);
            return commandDocument;
        };
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return (result, source, connection, operationContext) ->
                cursorDocumentToBatchCursor(timeoutMode, result, batchSize, decoder, comment, source, connection, operationContext);
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection, operationContext) ->
                cursorDocumentToAsyncBatchCursor(timeoutMode, result, batchSize, decoder, comment, source, connection, operationContext);
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }
}
