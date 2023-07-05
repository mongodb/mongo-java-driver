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
import com.mongodb.MongoCommandException;
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
import org.bson.codecs.Decoder;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.OperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.createEmptyBatchCursor;


public class ListSearchIndexesOperation<T>
        implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final Boolean allowDiskUse;
    private final Integer batchSize;
    private final Collation collation;
    private final BsonValue comment;
    private final long maxAwaitTimeMS;
    private final long maxTimeMS;
    private final String indexName;

    public ListSearchIndexesOperation(final MongoNamespace namespace,
                                      final Decoder<T> decoder,
                                      final long maxTimeMS, final long maxAwaitTimeMS,
                                      @Nullable final String indexName,
                                      @Nullable final Integer batchSize,
                                      @Nullable final Collation collation,
                                      @Nullable final BsonValue comment,
                                      @Nullable final Boolean allowDiskUse) {
        this.namespace = namespace;
        this.decoder = decoder;
        this.maxTimeMS = maxTimeMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.batchSize = batchSize == null ? 0 : batchSize;
        this.collation = collation;
        this.comment = comment;
        this.allowDiskUse = allowDiskUse;
        this.indexName = indexName;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        try {
            return getAggregateOperation().execute(binding);
        } catch (MongoCommandException exception) {
            return rethrowIfNotNamespaceError(exception, createEmptyBatchCursor(namespace, decoder,
                    exception.getServerAddress(), batchSize));
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        getAggregateOperation().executeAsync(binding, (cursor, exception) -> {
            if (exception != null && !isNamespaceError(exception)) {
                callback.onResult(null, exception);
            } else {
                if (exception == null) {
                    callback.onResult(cursor, null);
                    return;
                }
                MongoCommandException commandException = (MongoCommandException) assertNotNull(exception);

                AsyncBatchCursor<T> emptyAsyncBatchCursor = createEmptyAsyncBatchCursor(namespace, commandException.getServerAddress());
                callback.onResult(emptyAsyncBatchCursor, null);
            }
        });
    }

    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return getAggregateOperation().asExplainableOperation(verbosity, resultDecoder);
    }

    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return getAggregateOperation().asAsyncExplainableOperation(verbosity, resultDecoder);
    }

    private AggregateOperation<T> getAggregateOperation() {
        BsonDocument searchDefinition = getSearchDefinition();
        BsonDocument bsonDocument = new BsonDocument("$listSearchIndexes", searchDefinition);

        return new AggregateOperation<>(namespace, Collections.singletonList(bsonDocument), decoder)
                .collation(collation)
                .comment(comment)
                .allowDiskUse(allowDiskUse)
                .batchSize(batchSize)
                .maxAwaitTime(maxAwaitTimeMS, TimeUnit.MILLISECONDS)
                .maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    @NotNull
    private BsonDocument getSearchDefinition() {
        if (indexName == null) {
            return new BsonDocument();
        }
        return new BsonDocument("name", new BsonString(indexName));
    }
}
