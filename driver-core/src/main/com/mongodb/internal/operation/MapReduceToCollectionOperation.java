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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.operation.CommandOperationHelper.CommandWriteTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandWriteTransformerAsync;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
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
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    public MapReduceToCollectionOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                          final BsonJavaScript reduceFunction, final String collectionName) {
        this(namespace, mapFunction, reduceFunction, collectionName, null);
    }

    public MapReduceToCollectionOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
                                          final BsonJavaScript reduceFunction, final String collectionName,
                                          final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.collectionName = notNull("collectionName", collectionName);
        this.writeConcern = writeConcern;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public BsonJavaScript getMapFunction() {
        return mapFunction;
    }

    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    public MapReduceToCollectionOperation finalizeFunction(final BsonJavaScript finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    public BsonDocument getScope() {
        return scope;
    }

    public MapReduceToCollectionOperation scope(final BsonDocument scope) {
        this.scope = scope;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public MapReduceToCollectionOperation filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getSort() {
        return sort;
    }

    public MapReduceToCollectionOperation sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public MapReduceToCollectionOperation limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public boolean isJsMode() {
        return jsMode;
    }

    public MapReduceToCollectionOperation jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public MapReduceToCollectionOperation verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    public MapReduceToCollectionOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public String getAction() {
        return action;
    }

    public MapReduceToCollectionOperation action(final String action) {
        notNull("action", action);
        isTrue("action must be one of: \"replace\", \"merge\", \"reduce\"", VALID_ACTIONS.contains(action));
        this.action = action;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public MapReduceToCollectionOperation databaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public boolean isSharded() {
        return sharded;
    }

    public MapReduceToCollectionOperation sharded(final boolean sharded) {
        this.sharded = sharded;
        return this;
    }

    public boolean isNonAtomic() {
        return nonAtomic;
    }

    public MapReduceToCollectionOperation nonAtomic(final boolean nonAtomic) {
        this.nonAtomic = nonAtomic;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public MapReduceToCollectionOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public MapReduceToCollectionOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public MapReduceStatistics execute(final WriteBinding binding) {
        return withConnection(binding, connection -> executeCommand(binding, namespace.getDatabaseName(), getCommand(connection.getDescription()),
                connection, transformer()));
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<MapReduceStatistics> callback) {
        withAsyncConnection(binding, (connection, t) -> {
            SingleResultCallback<MapReduceStatistics> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                executeCommandAsync(binding, namespace.getDatabaseName(),
                        getCommand(connection.getDescription()), connection, transformerAsync(),
                        releasingCallback(errHandlingCallback, connection));

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
        return new CommandReadOperation<>(namespace.getDatabaseName(),
                ExplainHelper.asExplainCommand(getCommand(null), explainVerbosity),
                new BsonDocumentCodec());
    }

    private CommandWriteTransformer<BsonDocument, MapReduceStatistics> transformer() {
        return (result, connection) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return MapReduceHelper.createStatistics(result);
        };
    }

    private CommandWriteTransformerAsync<BsonDocument, MapReduceStatistics> transformerAsync() {
        return (result, connection) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return MapReduceHelper.createStatistics(result);
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
        putIfTrue(commandDocument, "verbose", isVerbose());
        putIfNotZero(commandDocument, "limit", getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", isJsMode());
        if (bypassDocumentValidation != null && description != null) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
        if (description != null) {
            appendWriteConcernToCommand(writeConcern, commandDocument);
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }
}
