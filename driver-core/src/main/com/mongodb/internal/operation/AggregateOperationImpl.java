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
import com.mongodb.MongoNamespace;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.setNonTailableCursorMaxTimeSupplier;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

class AggregateOperationImpl<T> implements ReadOperationCursor<T> {
    private static final String COMMAND_NAME = "aggregate";
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(RESULT, FIRST_BATCH);
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private final AggregateTarget aggregateTarget;
    private final PipelineCreator pipelineCreator;

    private boolean retryReads;
    private Boolean allowDiskUse;
    private Integer batchSize;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private BsonDocument variables;
    private TimeoutMode timeoutMode;
    private CursorType cursorType;

    AggregateOperationImpl(final MongoNamespace namespace,
            final List<BsonDocument> pipeline, final Decoder<T> decoder, final AggregationLevel aggregationLevel) {
        this(namespace, pipeline, decoder,
                defaultAggregateTarget(notNull("aggregationLevel", aggregationLevel),
                        notNull("namespace", namespace).getCollectionName()),
                defaultPipelineCreator(pipeline));
    }

    AggregateOperationImpl(final MongoNamespace namespace,
            final List<BsonDocument> pipeline, final Decoder<T> decoder, final AggregateTarget aggregateTarget,
            final PipelineCreator pipelineCreator) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
        this.aggregateTarget = notNull("aggregateTarget", aggregateTarget);
        this.pipelineCreator = notNull("pipelineCreator", pipelineCreator);
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    List<BsonDocument> getPipeline() {
        return pipeline;
    }

    Decoder<T> getDecoder() {
        return decoder;
    }

    Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    AggregateOperationImpl<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    Integer getBatchSize() {
        return batchSize;
    }

    AggregateOperationImpl<T> batchSize(@Nullable final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    Collation getCollation() {
        return collation;
    }

    AggregateOperationImpl<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Nullable
    BsonValue getComment() {
        return comment;
    }

    AggregateOperationImpl<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    AggregateOperationImpl<T> let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    AggregateOperationImpl<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * When {@link TimeoutContext#hasTimeoutMS()} then {@link TimeoutSettings#getMaxAwaitTimeMS()} usage in {@code getMore} commands
     * depends on the type of cursor. For {@link CursorType#TailableAwait} it is used, for others it is not.
     * {@link CursorType#TailableAwait} is used mainly for change streams in {@link AggregateOperationImpl}.
     *
     * @param cursorType
     * @return this
     */
    AggregateOperationImpl<T> cursorType(final CursorType cursorType) {
        this.cursorType = cursorType;
        return this;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    @Nullable
    BsonValue getHint() {
        return hint;
    }

    public AggregateOperationImpl<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        if (timeoutMode != null) {
            this.timeoutMode = timeoutMode;
        }
        return this;
    }

    AggregateOperationImpl<T> hint(@Nullable final BsonValue hint) {
        isTrueArgument("BsonString or BsonDocument", hint == null || hint.isDocument() || hint.isString());
        this.hint = hint;
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(),
                getCommandCreator(), CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        executeRetryableReadAsync(binding, namespace.getDatabaseName(),
                getCommandCreator(), CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                asyncTransformer(), retryReads,
                errHandlingCallback);
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) ->
                getCommand(operationContext, connectionDescription.getMaxWireVersion());
    }

    BsonDocument getCommand(final OperationContext operationContext, final int maxWireVersion) {
        BsonDocument commandDocument = new BsonDocument(getCommandName(), aggregateTarget.create());
        appendReadConcernToCommand(operationContext.getSessionContext(), maxWireVersion, commandDocument);
        commandDocument.put("pipeline", pipelineCreator.create());
        setNonTailableCursorMaxTimeSupplier(timeoutMode, operationContext);
        BsonDocument cursor = new BsonDocument();
        if (batchSize != null) {
            cursor.put("batchSize", new BsonInt32(batchSize));
        }
        commandDocument.put(CURSOR, cursor);
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
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

        return commandDocument;
    }

    private CommandReadTransformer<BsonDocument, CommandBatchCursor<T>> transformer() {
        return (result, source, connection) ->
                new CommandBatchCursor<>(getTimeoutMode(), result, batchSize != null ? batchSize : 0,
                        getMaxTimeForCursor(source.getOperationContext().getTimeoutContext()), decoder, comment, source, connection);
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) ->
            new AsyncCommandBatchCursor<>(getTimeoutMode(), result, batchSize != null ? batchSize : 0,
                    getMaxTimeForCursor(source.getOperationContext().getTimeoutContext()), decoder, comment, source, connection);
    }

    private TimeoutMode getTimeoutMode() {
        TimeoutMode localTimeoutMode = timeoutMode;
        if (localTimeoutMode == null) {
            localTimeoutMode = TimeoutMode.CURSOR_LIFETIME;
        }
        return localTimeoutMode;
    }

    private long getMaxTimeForCursor(final TimeoutContext timeoutContext) {
        long maxAwaitTimeMS = timeoutContext.getMaxAwaitTimeMS();
        if (timeoutContext.hasTimeoutMS()){
           return CursorType.TailableAwait == cursorType ? maxAwaitTimeMS : 0;
        }
        return maxAwaitTimeMS;
    }

    interface AggregateTarget {
        BsonValue create();
    }

    interface PipelineCreator {
        BsonArray create();
    }

    private static AggregateTarget defaultAggregateTarget(final AggregationLevel aggregationLevel, final String collectionName) {
        return () -> {
            if (aggregationLevel == AggregationLevel.DATABASE) {
                return new BsonInt32(1);
            } else {
                return new BsonString(collectionName);
            }
        };
    }

    private static PipelineCreator defaultPipelineCreator(final List<BsonDocument> pipeline) {
        return () -> new BsonArray(pipeline);
    }
}
