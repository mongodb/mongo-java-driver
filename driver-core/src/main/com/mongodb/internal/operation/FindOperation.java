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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.createReadCommandAndExecuteAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.decorateReadWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNullOrEmpty;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.addMaxTimeMSToNonTailableCursor;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.MIN_WIRE_VERSION;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.createReadCommandAndExecute;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateReadWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.withSourceAndConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class FindOperation<T> implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private static final String FIRST_BATCH = "firstBatch";

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private int batchSize;
    private int limit;
    private BsonDocument projection;
    private int skip;
    private BsonDocument sort;
    private CursorType cursorType = CursorType.NonTailable;
    private boolean noCursorTimeout;
    private boolean partial;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private BsonDocument variables;
    private BsonDocument max;
    private BsonDocument min;
    private boolean returnKey;
    private boolean showRecordId;
    private Boolean allowDiskUse;
    private TimeoutMode timeoutMode;

    public FindOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public Decoder<T> getDecoder() {
        return decoder;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public FindOperation<T> filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public FindOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public FindOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public BsonDocument getProjection() {
        return projection;
    }

    public FindOperation<T> projection(@Nullable final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    public int getSkip() {
        return skip;
    }

    public FindOperation<T> skip(final int skip) {
        this.skip = skip;
        return this;
    }

    public BsonDocument getSort() {
        return sort;
    }

    public FindOperation<T> sort(@Nullable final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    public CursorType getCursorType() {
        return cursorType;
    }

    public FindOperation<T> cursorType(final CursorType cursorType) {
        this.cursorType = notNull("cursorType", cursorType);
        return this;
    }

    public FindOperation<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        if (timeoutMode != null) {
            this.timeoutMode = timeoutMode;
        }
        return this;
    }

    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    public FindOperation<T> noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    public boolean isPartial() {
        return partial;
    }

    public FindOperation<T> partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public FindOperation<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public FindOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    public BsonValue getHint() {
        return hint;
    }

    public FindOperation<T> hint(@Nullable final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    public BsonDocument getLet() {
        return variables;
    }

    public FindOperation<T> let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    public BsonDocument getMax() {
        return max;
    }

    public FindOperation<T> max(@Nullable final BsonDocument max) {
        this.max = max;
        return this;
    }

    public BsonDocument getMin() {
        return min;
    }

    public FindOperation<T> min(@Nullable final BsonDocument min) {
        this.min = min;
        return this;
    }

    public boolean isReturnKey() {
        return returnKey;
    }

    public FindOperation<T> returnKey(final boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    public boolean isShowRecordId() {
        return showRecordId;
    }

    public FindOperation<T> showRecordId(final boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    public FindOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    public Boolean isAllowDiskUse() {
        return allowDiskUse;
    }

    public FindOperation<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        IllegalStateException invalidTimeoutModeException = invalidTimeoutModeException();
        if (invalidTimeoutModeException != null) {
            throw invalidTimeoutModeException;
        }

        RetryState retryState = initialRetryState(retryReads,  binding.getOperationContext().getTimeoutContext());
        Supplier<BatchCursor<T>> read = decorateReadWithRetries(retryState, binding.getOperationContext(), () ->
            withSourceAndConnection(binding::getReadConnectionSource, false, (source, connection) -> {
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), binding.getOperationContext()));
                try {
                    return createReadCommandAndExecute(retryState, binding.getOperationContext(), source, namespace.getDatabaseName(),
                                                       getCommandCreator(), CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                                       transformer(), connection);
                } catch (MongoCommandException e) {
                    throw new MongoQueryException(e.getResponse(), e.getServerAddress());
                }
            })
        );
        return read.get();
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        IllegalStateException invalidTimeoutModeException = invalidTimeoutModeException();
        if (invalidTimeoutModeException != null) {
            callback.onResult(null, invalidTimeoutModeException);
            return;
        }

        RetryState retryState = initialRetryState(retryReads,  binding.getOperationContext().getTimeoutContext());
        binding.retain();
        AsyncCallbackSupplier<AsyncBatchCursor<T>> asyncRead = decorateReadWithRetriesAsync(
                retryState, binding.getOperationContext(), (AsyncCallbackSupplier<AsyncBatchCursor<T>>) funcCallback ->
                    withAsyncSourceAndConnection(binding::getReadConnectionSource, false, funcCallback,
                            (source, connection, releasingCallback) -> {
                                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(),
                                        binding.getOperationContext()), releasingCallback)) {
                                    return;
                                }
                                SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = exceptionTransformingCallback(releasingCallback);
                                createReadCommandAndExecuteAsync(retryState, binding.getOperationContext(), source,
                                        namespace.getDatabaseName(), getCommandCreator(),
                                        CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                        asyncTransformer(), connection, wrappedCallback);
                            })
                ).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, LOGGER));
    }

    private static <T> SingleResultCallback<T> exceptionTransformingCallback(final SingleResultCallback<T> callback) {
        return (result, t) -> {
            if (t != null) {
                if (t instanceof MongoCommandException) {
                    MongoCommandException commandException = (MongoCommandException) t;
                    callback.onResult(result,
                            new MongoQueryException(commandException.getResponse(), commandException.getServerAddress()));
                } else {
                    callback.onResult(result, t);
                }
            } else {
                callback.onResult(result, null);
            }
        };
    }

    @Override
    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                       final Decoder<R> resultDecoder) {
        return createExplainableOperation(verbosity, resultDecoder);
    }

    @Override
    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return createExplainableOperation(verbosity, resultDecoder);
    }

    <R> CommandReadOperation<R> createExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new CommandReadOperation<>(getNamespace().getDatabaseName(),
                (operationContext, serverDescription, connectionDescription) ->
                        asExplainCommand(getCommand(operationContext, MIN_WIRE_VERSION), verbosity), resultDecoder);
    }

    private BsonDocument getCommand(final OperationContext operationContext, final int maxWireVersion) {
        BsonDocument commandDocument = new BsonDocument("find", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(operationContext.getSessionContext(), maxWireVersion, commandDocument);

        putIfNotNull(commandDocument, "filter", filter);
        putIfNotNullOrEmpty(commandDocument, "sort", sort);
        putIfNotNullOrEmpty(commandDocument, "projection", projection);
        if (skip > 0) {
            commandDocument.put("skip", new BsonInt32(skip));
        }
        if (limit != 0) {
            commandDocument.put("limit", new BsonInt32(Math.abs(limit)));
        }
        if (limit >= 0) {
            if (batchSize < 0 && Math.abs(batchSize) < limit) {
                commandDocument.put("limit", new BsonInt32(Math.abs(batchSize)));
            } else if (batchSize != 0) {
                commandDocument.put("batchSize", new BsonInt32(Math.abs(batchSize)));
            }
        }
        if (limit < 0 || batchSize < 0) {
            commandDocument.put("singleBatch", BsonBoolean.TRUE);
        }
        if (isTailableCursor()) {
            commandDocument.put("tailable", BsonBoolean.TRUE);
        } else {
            addMaxTimeMSToNonTailableCursor(commandDocument, timeoutMode, operationContext);
        }
        if (isAwaitData()) {
            commandDocument.put("awaitData", BsonBoolean.TRUE);
        }
        if (noCursorTimeout) {
            commandDocument.put("noCursorTimeout", BsonBoolean.TRUE);
        }
        if (partial) {
            commandDocument.put("allowPartialResults", BsonBoolean.TRUE);
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (comment != null) {
            commandDocument.put("comment", comment);
        }
        if (hint != null) {
            commandDocument.put("hint", hint);
        }
        if (variables != null) {
            commandDocument.put("let", variables);
        }
        if (max != null) {
            commandDocument.put("max", max);
        }
        if (min != null) {
            commandDocument.put("min", min);
        }
        if (returnKey) {
            commandDocument.put("returnKey", BsonBoolean.TRUE);
        }
        if (showRecordId) {
            commandDocument.put("showRecordId", BsonBoolean.TRUE);
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        return commandDocument;
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) ->
                getCommand(operationContext, connectionDescription.getMaxWireVersion());
    }

    private boolean isTailableCursor() {
        return cursorType.isTailable();
    }

    private boolean isAwaitData() {
        return cursorType == CursorType.TailableAwait;
    }

    private TimeoutMode getTimeoutMode() {
        if (timeoutMode == null) {
            return isTailableCursor() ? TimeoutMode.ITERATION : TimeoutMode.CURSOR_LIFETIME;
        }
        return timeoutMode;
    }

    private CommandReadTransformer<BsonDocument, CommandBatchCursor<T>> transformer() {
        return (result, source, connection) ->
                new CommandBatchCursor<>(getTimeoutMode(), result, batchSize, getMaxTimeForCursor(source.getOperationContext()), decoder,
                        comment, source, connection);
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) ->
            new AsyncCommandBatchCursor<>(getTimeoutMode(), result, batchSize, getMaxTimeForCursor(source.getOperationContext()), decoder,
                    comment, source, connection);
    }

    private long getMaxTimeForCursor(final OperationContext operationContext) {
        return cursorType == CursorType.TailableAwait ? operationContext.getTimeoutContext().getMaxAwaitTimeMS() : 0;
    }

    @Nullable
    private IllegalStateException invalidTimeoutModeException() {
        if (isTailableCursor()) {
            if (timeoutMode == TimeoutMode.CURSOR_LIFETIME) {
                return new IllegalStateException("Tailable cursors only support the ITERATION value for the timeoutMode option.");
            }
        }
        return null;
    }
}
