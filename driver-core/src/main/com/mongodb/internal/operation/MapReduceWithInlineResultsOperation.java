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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.MIN_WIRE_VERSION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>Operation that runs a Map Reduce against a MongoDB instance.  This operation only supports "inline" results, i.e. the results will be
 * returned as a result of running this operation.</p>
 *
 * <p>To run a map reduce operation into a given collection, use {@code MapReduceToCollectionOperation}.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MapReduceWithInlineResultsOperation<T> implements AsyncReadOperation<MapReduceAsyncBatchCursor<T>>,
                                                               ReadOperation<MapReduceBatchCursor<T>> {
    private final MongoNamespace namespace;
    private final BsonJavaScript mapFunction;
    private final BsonJavaScript reduceFunction;
    private final Decoder<T> decoder;
    private BsonJavaScript finalizeFunction;
    private BsonDocument scope;
    private BsonDocument filter;
    private BsonDocument sort;
    private int limit;
    private boolean jsMode;
    private boolean verbose;
    private long maxTimeMS;
    private Collation collation;

    public MapReduceWithInlineResultsOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                               final BsonJavaScript reduceFunction, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.decoder = notNull("decoder", decoder);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public Decoder<T> getDecoder() {
        return decoder;
    }

    public BsonJavaScript getMapFunction() {
        return mapFunction;
    }

    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    public MapReduceWithInlineResultsOperation<T> finalizeFunction(final BsonJavaScript finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    public BsonDocument getScope() {
        return scope;
    }

    public MapReduceWithInlineResultsOperation<T> scope(@Nullable final BsonDocument scope) {
        this.scope = scope;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public MapReduceWithInlineResultsOperation<T> filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getSort() {
        return sort;
    }

    public MapReduceWithInlineResultsOperation<T> sort(@Nullable final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public MapReduceWithInlineResultsOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public boolean isJsMode() {
        return jsMode;
    }

    public MapReduceWithInlineResultsOperation<T> jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public MapReduceWithInlineResultsOperation<T> verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public MapReduceWithInlineResultsOperation<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }


    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }


    public MapReduceWithInlineResultsOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }


    @Override
    public MapReduceBatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                CommandResultDocumentCodec.create(decoder, "results"), transformer(), false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<MapReduceAsyncBatchCursor<T>> callback) {
        SingleResultCallback<MapReduceAsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        executeRetryableReadAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                CommandResultDocumentCodec.create(decoder, "results"),
                asyncTransformer(), false, errHandlingCallback);
    }

    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    private CommandReadOperation<BsonDocument> createExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return new CommandReadOperation<>(namespace.getDatabaseName(),
                asExplainCommand(getCommand(NoOpSessionContext.INSTANCE, MIN_WIRE_VERSION),
                        explainVerbosity), new BsonDocumentCodec());
    }

    private CommandReadTransformer<BsonDocument, MapReduceBatchCursor<T>> transformer() {
        return (result, source, connection) -> new MapReduceInlineResultsCursor<>(createQueryResult(result, connection.getDescription()), decoder, source,
                MapReduceHelper.createStatistics(result));
    }

    private CommandReadTransformerAsync<BsonDocument, MapReduceAsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) -> new MapReduceInlineResultsAsyncCursor<>(createQueryResult(result, connection.getDescription()),
                MapReduceHelper.createStatistics(result));
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> getCommand(sessionContext, connectionDescription.getMaxWireVersion());
    }

    private BsonDocument getCommand(final SessionContext sessionContext, final int maxWireVersion) {
        BsonDocument commandDocument = new BsonDocument("mapreduce", new BsonString(namespace.getCollectionName()))
                                           .append("map", getMapFunction())
                                           .append("reduce", getReduceFunction())
                                           .append("out", new BsonDocument("inline", new BsonInt32(1)));

        putIfNotNull(commandDocument, "query", getFilter());
        putIfNotNull(commandDocument, "sort", getSort());
        putIfNotNull(commandDocument, "finalize", getFinalizeFunction());
        putIfNotNull(commandDocument, "scope", getScope());
        putIfTrue(commandDocument, "verbose", isVerbose());
        appendReadConcernToCommand(sessionContext, maxWireVersion, commandDocument);
        putIfNotZero(commandDocument, "limit", getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", isJsMode());
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<>(namespace, BsonDocumentWrapperHelper.toList(result, "results"), 0,
                description.getServerAddress());
    }
}
