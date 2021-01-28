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
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.validateCollation;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Operation that runs a Map Reduce against a MongoDB instance.  This operation does not support "inline" results, i.e. the results will
 * be output into the collection represented by the MongoNamespace provided.
 *
 * <p>To run a map reduce operation and receive the results inline (i.e. as a response to running the command) use {@code
 * MapReduceToCollectionOperation}.</p>
 *
 * @mongodb.driver.manual core/map-reduce Map Reduce
 * @since 3.0
 */
public class
MapReduceToCollectionOperation implements AsyncWriteOperation<MapReduceStatistics>, WriteOperation<MapReduceStatistics> {
    private final MongoNamespace namespace;
    private final BsonJavaScript mapFunction;
    private final BsonJavaScript reduceFunction;
    private final String collectionName;
    private final WriteConcern writeConcern;
    private BsonJavaScript finalizeFunction;
    private BsonDocument scope;
    private BsonDocument filter;
    private BsonDocument sort;
    private int limit;
    private boolean jsMode;
    private boolean verbose;
    private long maxTimeMS;
    private String action = "replace";
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private static final List<String> VALID_ACTIONS = asList("replace", "merge", "reduce");

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute
     *
     * @param namespace the database and collection namespace for the operation.
     * @param mapFunction a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param collectionName the name of the collection to output the results to.
     * @mongodb.driver.manual core/map-reduce Map Reduce
     */
    public MapReduceToCollectionOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                          final BsonJavaScript reduceFunction, final String collectionName) {
        this(namespace, mapFunction, reduceFunction, collectionName, null);
    }

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute
     *
     * @param namespace the database and collection namespace for the operation.
     * @param mapFunction a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param collectionName the name of the collection to output the results to.
     * @param writeConcern the write concern
     * @mongodb.driver.manual core/map-reduce Map Reduce
     *
     * @since 3.4
     */
    public MapReduceToCollectionOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                          final BsonJavaScript reduceFunction, final String collectionName,
                                          final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.collectionName = notNull("collectionName", collectionName);
        this.writeConcern = writeConcern;
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
     * Gets the name of the collection to output the results to.
     *
     * @return the name of the collection to output the results to.
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern, which may be null
     *
     * @since 3.4
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
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
    public MapReduceToCollectionOperation finalizeFunction(final BsonJavaScript finalizeFunction) {
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
    public MapReduceToCollectionOperation scope(final BsonDocument scope) {
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
    public MapReduceToCollectionOperation filter(final BsonDocument filter) {
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
    public MapReduceToCollectionOperation sort(final BsonDocument sort) {
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
    public MapReduceToCollectionOperation limit(final int limit) {
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
    public MapReduceToCollectionOperation jsMode(final boolean jsMode) {
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
    public MapReduceToCollectionOperation verbose(final boolean verbose) {
        this.verbose = verbose;
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
    public MapReduceToCollectionOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the output action, one of: "replace", "merge", "reduce". Defaults to "replace".
     *
     * @return the output action
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public String getAction() {
        return action;
    }

    /**
     * Sets the output action one of: "replace", "merge", "reduce"
     *
     * @param action the output action
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceToCollectionOperation action(final String action) {
        notNull("action", action);
        isTrue("action must be one of: \"replace\", \"merge\", \"reduce\"", VALID_ACTIONS.contains(action));
        this.action = action;
        return this;
    }

    /**
     * Gets the name of the database to output into.
     *
     * @return the name of the database to output into.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceToCollectionOperation databaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * True if the output database is sharded
     *
     * @return true if the output database is sharded
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public boolean isSharded() {
        return sharded;
    }

    /**
     * Sets if the output database is sharded
     *
     * @param sharded if the output database is sharded
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceToCollectionOperation sharded(final boolean sharded) {
        this.sharded = sharded;
        return this;
    }

    /**
     * True if the post-processing step will prevent MongoDB from locking the database.
     *
     * @return if true the post-processing step will prevent MongoDB from locking the database.
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public boolean isNonAtomic() {
        return nonAtomic;
    }

    /**
     * Sets if the post-processing step will prevent MongoDB from locking the database.
     *
     * Valid only with {@code "merge"} or {@code "reduce"} actions.
     *
     * @param nonAtomic if the post-processing step will prevent MongoDB from locking the database.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceToCollectionOperation nonAtomic(final boolean nonAtomic) {
        this.nonAtomic = nonAtomic;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public MapReduceToCollectionOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
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
    public MapReduceToCollectionOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Executing this will return a cursor with your results in.
     *
     * @param binding the binding
     * @return a MongoCursor that can be iterated over to find all the results of the Map Reduce operation.
     */
    @Override
    public MapReduceStatistics execute(final WriteBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnection<MapReduceStatistics>() {
            @Override
            public MapReduceStatistics call(final Connection connection) {
                validateCollation(connection, collation);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(connection.getDescription()),
                        connection, transformer());
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<MapReduceStatistics> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<MapReduceStatistics> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<MapReduceStatistics> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    validateCollation(connection, collation, new AsyncCallableWithConnection() {
                        @Override
                        public void call(final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                        getCommand(connection.getDescription()), connection, transformer(), wrappedCallback);
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
                                                      ExplainHelper.asExplainCommand(getCommand(null), explainVerbosity),
                                                      new BsonDocumentCodec());
    }

    private CommandTransformer<BsonDocument, MapReduceStatistics> transformer() {
        return new CommandTransformer<BsonDocument, MapReduceStatistics>() {
            @SuppressWarnings("unchecked")
            @Override
            public MapReduceStatistics apply(final BsonDocument result, final ServerAddress serverAddress) {
                throwOnWriteConcernError(result, serverAddress);
                return MapReduceHelper.createStatistics(result);
            }
        };
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument outputDocument = new BsonDocument(getAction(), new BsonString(getCollectionName()));
        if (description != null && !serverIsAtLeastVersionFourDotFour(description)) {
            putIfTrue(outputDocument, "sharded", isSharded());
            putIfTrue(outputDocument, "nonAtomic", isNonAtomic());
        }
        if (getDatabaseName() != null) {
            outputDocument.put("db", new BsonString(getDatabaseName()));
        }
        BsonDocument commandDocument = new BsonDocument("mapreduce", new BsonString(namespace.getCollectionName()))
                .append("map", getMapFunction())
                .append("reduce", getReduceFunction())
                .append("out", outputDocument);

        putIfNotNull(commandDocument, "query", getFilter());
        putIfNotNull(commandDocument, "sort", getSort());
        putIfNotNull(commandDocument, "finalize", getFinalizeFunction());
        putIfNotNull(commandDocument, "scope", getScope());
        putIfTrue(commandDocument, "verbose", isVerbose());      putIfNotZero(commandDocument, "limit", getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", isJsMode());
        if (bypassDocumentValidation != null && description != null && serverIsAtLeastVersionThreeDotTwo(description)) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
        if (description != null) {
            appendWriteConcernToCommand(writeConcern, commandDocument, description);
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }

    private static BsonValue asValueOrNull(final BsonValue value) {
        return value == null ? BsonNull.VALUE : value;
    }
}
