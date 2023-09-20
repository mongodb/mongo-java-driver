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

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;


/**
 * An operation that provides a cursor allowing iteration through the metadata of all the databases for a MongoClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ListDatabasesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final TimeoutSettings timeoutSettings;
    private final ClientSideOperationTimeout clientSideOperationTimeout;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;
    private BsonValue comment;

    public ListDatabasesOperation(final TimeoutSettings timeoutSettings, final Decoder<T> decoder) {
        this.timeoutSettings = timeoutSettings;
        this.clientSideOperationTimeout = new ClientSideOperationTimeout(timeoutSettings);
        this.decoder = notNull("decoder", decoder);
    }

    public ListDatabasesOperation<T> filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public ListDatabasesOperation<T> nameOnly(final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public ListDatabasesOperation<T> authorizedDatabasesOnly(final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    public ListDatabasesOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    public Boolean getNameOnly() {
        return nameOnly;
    }

    public Boolean getAuthorizedDatabasesOnly() {
        return authorizedDatabasesOnly;
    }

    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    public ListDatabasesOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(clientSideOperationTimeout, binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"), transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeRetryableReadAsync(clientSideOperationTimeout, binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"), asyncTransformer(),
                retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return (result, source, connection) -> new QueryBatchCursor<>(createQueryResult(result, connection.getDescription()), 0, 0, decoder, comment, source);
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) -> new AsyncQueryBatchCursor<>(createQueryResult(result, connection.getDescription()), 0, 0, 0, decoder,
                comment, source, connection, result);
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<>(null, BsonDocumentWrapperHelper.toList(result, "databases"), 0,
                description.getServerAddress());
    }

    private CommandCreator getCommandCreator() {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument("listDatabases", new BsonInt32(1));
            putIfNotNull(command, "filter", filter);
            putIfNotNull(command, "nameOnly", nameOnly);
            putIfNotNull(command, "authorizedDatabases", authorizedDatabasesOnly);
            putIfNotZero(command, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
            putIfNotNull(command, "comment", comment);
            return command;
        };
    }
}
