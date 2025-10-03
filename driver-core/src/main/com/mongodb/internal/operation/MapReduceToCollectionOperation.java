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
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.CommandHelper.applyMaxTimeMS;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandWriteTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandWriteTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;
import static java.util.Arrays.asList;

/**
 * Operation that runs a Map Reduce against a MongoDB instance.  This operation does not support "inline" results, i.e. the results will
 * be output into the collection represented by the MongoNamespace provided.
 *
 * <p>To run a map reduce operation and receive the results inline (i.e. as a response to running the command) use {@code
 * MapReduceToCollectionOperation}.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MapReduceToCollectionOperation implements WriteOperation<MapReduceStatistics> {
    private static final String COMMAND_NAME = "mapReduce";
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
    private String action = "replace";
    private String databaseName;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private static final List<String> VALID_ACTIONS = asList("replace", "merge", "reduce");

    public MapReduceToCollectionOperation(final MongoNamespace namespace, final BsonJavaScript mapFunction,
            final BsonJavaScript reduceFunction, @Nullable final String collectionName, @Nullable final WriteConcern writeConcern) {
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

    public MapReduceToCollectionOperation scope(@Nullable final BsonDocument scope) {
        this.scope = scope;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public MapReduceToCollectionOperation filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getSort() {
        return sort;
    }

    public MapReduceToCollectionOperation sort(@Nullable final BsonDocument sort) {
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

    public String getAction() {
        return action;
    }

    public MapReduceToCollectionOperation action(final String action) {
        notNull("action", action);
        isTrue("action must be one of: \"replace\", \"merge\", \"reduce\"", VALID_ACTIONS.contains(action));
        this.action = action;
        return this;
    }

    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    public MapReduceToCollectionOperation databaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public MapReduceToCollectionOperation bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public MapReduceToCollectionOperation collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MapReduceStatistics execute(final WriteBinding binding) {
        return executeCommand(binding, namespace.getDatabaseName(), getCommandCreator(), transformer(binding
                .getOperationContext()
                .getTimeoutContext()));
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<MapReduceStatistics> callback) {
        executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(), transformerAsync(binding
                .getOperationContext()
                .getTimeoutContext()), callback);
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperationSimple<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    private CommandReadOperation<BsonDocument> createExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return new CommandReadOperation<>(getNamespace().getDatabaseName(), getCommandName(),
                (operationContext, serverDescription, connectionDescription) -> {
                    BsonDocument command = getCommandCreator().create(operationContext, serverDescription, connectionDescription);
                    applyMaxTimeMS(operationContext.getTimeoutContext(), command);
                    return asExplainCommand(command, explainVerbosity);
                }, new BsonDocumentCodec());
    }

    private CommandWriteTransformer<BsonDocument, MapReduceStatistics> transformer(final TimeoutContext timeoutContext) {
        return (result, connection) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion(), timeoutContext);
            return MapReduceHelper.createStatistics(result);
        };
    }

    private CommandWriteTransformerAsync<BsonDocument, MapReduceStatistics> transformerAsync(final TimeoutContext timeoutContext) {
        return (result, connection) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion(), timeoutContext);
            return MapReduceHelper.createStatistics(result);
        };
    }


    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument outputDocument = new BsonDocument(getAction(), new BsonString(getCollectionName()));
            if (getDatabaseName() != null) {
                outputDocument.put("db", new BsonString(getDatabaseName()));
            }
            BsonDocument commandDocument = new BsonDocument("mapReduce", new BsonString(namespace.getCollectionName()))
                    .append("map", getMapFunction())
                    .append("reduce", getReduceFunction())
                    .append("out", outputDocument);

            putIfNotNull(commandDocument, "query", getFilter());
            putIfNotNull(commandDocument, "sort", getSort());
            putIfNotNull(commandDocument, "finalize", getFinalizeFunction());
            putIfNotNull(commandDocument, "scope", getScope());
            putIfTrue(commandDocument, "verbose", isVerbose());
            putIfNotZero(commandDocument, "limit", getLimit());
            putIfTrue(commandDocument, "jsMode", isJsMode());
            if (bypassDocumentValidation != null) {
                commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
            }
            appendWriteConcernToCommand(writeConcern, commandDocument);
            if (collation != null) {
                commandDocument.put("collation", collation.asDocument());
            }
            return commandDocument;
        };
    }

}
