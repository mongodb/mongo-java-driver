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

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.asyncSingleBatchCursorTransformer;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.SyncOperationHelper.singleBatchCursorTransformer;


/**
 * An operation that provides a cursor allowing iteration through the metadata of all the databases for a MongoClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ListDatabasesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String COMMAND_NAME = "listDatabases";
    private static final String DATABASES = "databases";
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;
    private BsonValue comment;

    public ListDatabasesOperation(final Decoder<T> decoder) {
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
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, "admin", getCommandCreator(), CommandResultDocumentCodec.create(decoder, DATABASES),
                singleBatchCursorTransformer(DATABASES), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeRetryableReadAsync(binding, "admin", getCommandCreator(), CommandResultDocumentCodec.create(decoder, DATABASES),
                asyncSingleBatchCursorTransformer(DATABASES), retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument commandDocument = new BsonDocument(getCommandName(), new BsonInt32(1));
            putIfNotNull(commandDocument, "filter", filter);
            putIfNotNull(commandDocument, "nameOnly", nameOnly);
            putIfNotNull(commandDocument, "authorizedDatabases", authorizedDatabasesOnly);
            putIfNotNull(commandDocument, "comment", comment);
            return commandDocument;
        };
    }
}
