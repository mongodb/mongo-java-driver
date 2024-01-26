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
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.asyncSingleBatchCursorTransformer;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.SyncOperationHelper.singleBatchCursorTransformer;

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

    public DistinctOperation<T> filter(@Nullable final BsonDocument filter) {
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

    public Collation getCollation() {
        return collation;
    }

    public DistinctOperation<T> collation(@Nullable final Collation collation) {
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
        return executeRetryableRead(binding, namespace.getDatabaseName(), getCommandCreator(), createCommandDecoder(),
                singleBatchCursorTransformer(VALUES), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeRetryableReadAsync(binding, namespace.getDatabaseName(),
                                  getCommandCreator(), createCommandDecoder(), asyncSingleBatchCursorTransformer(VALUES), retryReads,
                                  errorHandlingCallback(callback, LOGGER));
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, VALUES);
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument commandDocument = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
            appendReadConcernToCommand(operationContext.getSessionContext(), connectionDescription.getMaxWireVersion(), commandDocument);
            commandDocument.put("key", new BsonString(fieldName));
            putIfNotNull(commandDocument, "query", filter);
            putIfNotZero(commandDocument, "maxTimeMS", operationContext.getTimeoutContext().getMaxTimeMS());
            if (collation != null) {
                commandDocument.put("collation", collation.asDocument());
            }
            putIfNotNull(commandDocument, "comment", comment);
            return commandDocument;
        };
    }
}
