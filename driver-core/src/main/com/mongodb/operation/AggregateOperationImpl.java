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
import com.mongodb.client.model.AggregationLevel;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import com.mongodb.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationReadConcernHelper.appendReadConcernToCommand;

class AggregateOperationImpl<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(RESULT, FIRST_BATCH);

    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private final AggregateTarget aggregateTarget;
    private final PipelineCreator pipelineCreator;

    private Boolean allowDiskUse;
    private Integer batchSize;
    private Collation collation;
    private String comment;
    private BsonValue hint;
    private long maxAwaitTimeMS;
    private long maxTimeMS;
    private Boolean useCursor;

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                           final AggregationLevel aggregationLevel) {
        this(namespace, pipeline, decoder, defaultAggregateTarget(notNull("aggregationLevel", aggregationLevel),
                notNull("namespace", namespace).getCollectionName()), defaultPipelineCreator(pipeline));
    }

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                           final AggregateTarget aggregateTarget, final PipelineCreator pipelineCreator) {
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

    AggregateOperationImpl<T> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    Integer getBatchSize() {
        return batchSize;
    }

    AggregateOperationImpl<T> batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, TimeUnit.MILLISECONDS);
    }

    AggregateOperationImpl<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxAwaitTime >= 0", maxAwaitTime >= 0);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    AggregateOperationImpl<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime >= 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    Boolean getUseCursor() {
        return useCursor;
    }

    AggregateOperationImpl<T> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    Collation getCollation() {
        return collation;
    }

    AggregateOperationImpl<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    String getComment() {
        return comment;
    }

    AggregateOperationImpl<T> comment(final String comment) {
        this.comment = comment;
        return this;
    }

    BsonValue getHint() {
        return hint;
    }

    AggregateOperationImpl<T> hint(final BsonValue hint) {
        isTrueArgument("BsonString or BsonDocument", hint == null || hint.isDocument() || hint.isString());
        this.hint = hint;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                validateReadConcernAndCollation(connection, binding.getSessionContext().getReadConcern(), collation);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(),
                        getCommand(connection.getDescription(), binding.getSessionContext()),
                        CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                        connection, transformer(source, connection));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback =
                            releasingCallback(errHandlingCallback, source, connection);
                    validateReadConcernAndCollation(source, connection, binding.getSessionContext().getReadConcern(), collation,
                            new AsyncCallableWithConnectionAndSource() {
                                @Override
                                public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                                    if (t != null) {
                                        wrappedCallback.onResult(null, t);
                                    } else {
                                        executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                                getCommand(connection.getDescription(), binding.getSessionContext()),
                                                CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                                                connection, asyncTransformer(source, connection), wrappedCallback);
                                    }
                                }
                            });
                }
            }
        });
    }

    private boolean isInline(final ConnectionDescription description) {
        return !serverIsAtLeastVersionThreeDotSix(description) && ((useCursor != null && !useCursor));
    }

    private BsonDocument getCommand(final ConnectionDescription description, final SessionContext sessionContext) {
        BsonDocument commandDocument = new BsonDocument("aggregate", aggregateTarget.create());

        appendReadConcernToCommand(sessionContext, commandDocument);
        commandDocument.put("pipeline", pipelineCreator.create(description, sessionContext));
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (!isInline(description)) {
            BsonDocument cursor = new BsonDocument();
            if (batchSize != null) {
                cursor.put("batchSize", new BsonInt32(batchSize));
            }
            commandDocument.put(CURSOR, cursor);
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (comment != null) {
            commandDocument.put("comment", new BsonString(comment));
        }
        if (hint != null) {
            commandDocument.put("hint", hint);
        }

        return commandDocument;
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        if (!isInline(description) || result.containsKey(CURSOR)) {
            return cursorDocumentToQueryResult(result.getDocument(CURSOR), description.getServerAddress());
        } else {
            return new QueryResult<T>(namespace, BsonDocumentWrapperHelper.<T>toList(result, RESULT), 0L,
                    description.getServerAddress());
        }
    }

    private CommandTransformer<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new CommandTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new QueryBatchCursor<T>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder, source,
                        connection);
            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnectionSource source,
                                                                                   final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new AsyncQueryBatchCursor<T>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder,
                        source, connection);
            }
        };
    }

    interface AggregateTarget {
        BsonValue create();
    }

    interface PipelineCreator {
        BsonArray create(ConnectionDescription connectionDescription, SessionContext sessionContext);
    }

    private static AggregateTarget defaultAggregateTarget(final AggregationLevel aggregationLevel, final String collectionName) {
        return new AggregateTarget() {
            @Override
            public BsonValue create() {
                if (aggregationLevel == AggregationLevel.DATABASE) {
                    return new BsonInt32(1);
                } else {
                    return new BsonString(collectionName);
                }
            }
        };
    }

    private static PipelineCreator defaultPipelineCreator(final List<BsonDocument> pipeline) {
        return new PipelineCreator() {
            @Override
            public BsonArray create(final ConnectionDescription connectionDescription, final SessionContext sessionContext) {
                return new BsonArray(pipeline);
            }
        };
    }
}
