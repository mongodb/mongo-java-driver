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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.session.SessionContext;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static com.mongodb.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.ReadConcernHelper.appendReadConcernToCommand;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>Operation that runs a Map Reduce against a MongoDB instance.  This operation only supports "inline" results, i.e. the results will be
 * returned as a result of running this operation.</p>
 *
 * <p>To run a map reduce operation into a given collection, use {@code MapReduceToCollectionOperation}.</p>
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual reference/command/mapReduce/ mapReduce
 * @mongodb.driver.manual core/map-reduce Map-Reduce
 * @since 3.0
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
    private ReadConcern readConcern = ReadConcern.DEFAULT;
    private Collation collation;

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param mapFunction a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param decoder the decoder for the result documents.
     * @mongodb.driver.manual reference/command/mapReduce/ mapReduce
     */
    public MapReduceWithInlineResultsOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                               final BsonJavaScript reduceFunction, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     * @since 3.4
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     * @since 3.4
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Gets the JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     *
     * @return the JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     */
    public BsonJavaScript getMapFunction() {
        return mapFunction;
    }

    /**
     * Gets the JavaScript function that "reduces" to a single object all the values associated with a particular key.
     *
     * @return the JavaScript function that "reduces" to a single object all the values associated with a particular key.
     */
    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    /**
     * Gets the JavaScript function that follows the reduce method and modifies the output. Default is null
     *
     * @return the JavaScript function that follows the reduce method and modifies the output.
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    /**
     * Sets the JavaScript function that follows the reduce method and modifies the output.
     *
     * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    public MapReduceWithInlineResultsOperation<T> finalizeFunction(final BsonJavaScript finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    /**
     * Gets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @return the global variables that are accessible in the map, reduce and finalize functions.
     * @mongodb.driver.manual reference/command/mapReduce Scope
     */
    public BsonDocument getScope() {
        return scope;
    }

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public MapReduceWithInlineResultsOperation<T> scope(final BsonDocument scope) {
        this.scope = scope;
        return this;
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
     * Sets the filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public MapReduceWithInlineResultsOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public MapReduceWithInlineResultsOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public MapReduceWithInlineResultsOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public boolean isJsMode() {
        return jsMode;
    }

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and
     *               reduce functions
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public MapReduceWithInlineResultsOperation<T> jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    /**
     * Gets whether to include the timing information in the result information. Defaults to true.
     *
     * @return whether to include the timing information in the result information
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    public MapReduceWithInlineResultsOperation<T> verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public MapReduceWithInlineResultsOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
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
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public MapReduceWithInlineResultsOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
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
    public MapReduceWithInlineResultsOperation<T> readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
        return this;
    }

    /**
     * Executing this will return a cursor with your results and the statistics in.
     *
     * @param binding the binding
     * @return a MapReduceCursor that can be iterated over to find all the results of the Map Reduce operation.
     */
    @Override
    @SuppressWarnings("unchecked")
    public MapReduceBatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<MapReduceBatchCursor<T>>() {
            @Override
            public MapReduceBatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                validateReadConcernAndCollation(connection, readConcern, collation);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(binding.getSessionContext()),
                                                     CommandResultDocumentCodec.create(decoder, "results"),
                                                     connection, transformer(source, connection));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<MapReduceAsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<MapReduceAsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<MapReduceAsyncBatchCursor<T>> wrappedCallback = releasingCallback(
                            errHandlingCallback, connection);
                    validateReadConcernAndCollation(connection, readConcern, collation, new AsyncCallableWithConnection() {
                        @Override
                        public void call(final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                        getCommand(binding.getSessionContext()),
                                        CommandResultDocumentCodec.create(decoder, "results"), connection,
                                        asyncTransformer(connection), wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    private CommandReadOperation<BsonDocument> createExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return new CommandReadOperation<BsonDocument>(namespace.getDatabaseName(),
                                                      asExplainCommand(getCommand(NoOpSessionContext.INSTANCE), explainVerbosity),
                                                      new BsonDocumentCodec());
    }

    private CommandTransformer<BsonDocument, MapReduceBatchCursor<T>> transformer(final ConnectionSource source,
                                                                                  final Connection connection) {
        return new CommandTransformer<BsonDocument, MapReduceBatchCursor<T>>() {
            @Override
            public MapReduceBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new MapReduceInlineResultsCursor<T>(createQueryResult(result, connection.getDescription()), decoder, source,
                                                           MapReduceHelper.createStatistics(result));
            }
        };
    }

    private CommandTransformer<BsonDocument, MapReduceAsyncBatchCursor<T>> asyncTransformer(final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, MapReduceAsyncBatchCursor<T>>() {
            @Override
            public MapReduceAsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new MapReduceInlineResultsAsyncCursor<T>(createQueryResult(result, connection.getDescription()),
                                                                MapReduceHelper.createStatistics(result));
            }
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument commandDocument = new BsonDocument("mapreduce", new BsonString(namespace.getCollectionName()))
                                           .append("map", getMapFunction())
                                           .append("reduce", getReduceFunction())
                                           .append("out", new BsonDocument("inline", new BsonInt32(1)))
                                           .append("query", asValueOrNull(getFilter()))
                                           .append("sort", asValueOrNull(getSort()))
                                           .append("finalize", asValueOrNull(getFinalizeFunction()))
                                           .append("scope", asValueOrNull(getScope()))
                                           .append("verbose", BsonBoolean.valueOf(isVerbose()));

        appendReadConcernToCommand(readConcern, sessionContext, commandDocument);
        putIfNotZero(commandDocument, "limit", getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", isJsMode());
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(namespace, BsonDocumentWrapperHelper.<T>toList(result, "results"), 0,
                                  description.getServerAddress());
    }

    private static BsonValue asValueOrNull(final BsonValue value) {
        return value == null ? BsonNull.VALUE : value;
    }
}
