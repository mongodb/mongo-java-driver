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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;

/**
 * Finds the distinct values for a specified field across a single collection.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DistinctOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String VALUES = "values";

    private final MongoNamespace namespace;
    private final String fieldName;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private long maxTimeMS;
    private Collation collation;
    private BsonValue comment;

    public DistinctOperation(final MongoNamespace namespace, final String fieldName, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
        this.decoder = notNull("decoder", decoder);
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public DistinctOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public DistinctOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public DistinctOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public DistinctOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public DistinctOperation<T> comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }


    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                createCommandDecoder(), transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeRetryableReadAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                createCommandDecoder(), asyncTransformer(), retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, VALUES);
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<>(namespace, BsonDocumentWrapperHelper.toList(result, VALUES), 0L,
                description.getServerAddress());
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return (result, source, connection) -> {
            QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
            return new QueryBatchCursor<>(queryResult, 0, 0, decoder, comment, source);
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) -> {
            QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
            return new AsyncSingleBatchQueryCursor<>(queryResult);
        };
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> getCommand(sessionContext, connectionDescription);
    }

    private BsonDocument getCommand(final SessionContext sessionContext, final ConnectionDescription connectionDescription) {
        BsonDocument commandDocument = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
        appendReadConcernToCommand(sessionContext, connectionDescription.getMaxWireVersion(), commandDocument);
        commandDocument.put("key", new BsonString(fieldName));
        putIfNotNull(commandDocument, "query", filter);
        putIfNotZero(commandDocument, "maxTimeMS", maxTimeMS);
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        putIfNotNull(commandDocument, "comment", comment);
        return commandDocument;
    }
}
