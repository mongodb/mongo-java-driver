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
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import static com.mongodb.internal.operation.AsyncOperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.OperationHelper.createEmptyBatchCursor;
import static java.util.Collections.singletonList;

/**
 * An operation that lists Alas Search indexes with the help of {@value #STAGE_LIST_SEARCH_INDEXES} pipeline stage.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class ListSearchIndexesOperation<T>
        implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private static final String STAGE_LIST_SEARCH_INDEXES = "$listSearchIndexes";
    private final TimeoutSettings timeoutSettings;
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    @Nullable
    private final Boolean allowDiskUse;
    @Nullable
    private final Integer batchSize;
    @Nullable
    private final Collation collation;
    @Nullable
    private final BsonValue comment;
    @Nullable
    private final String indexName;
    private final boolean retryReads;

    ListSearchIndexesOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
            final Decoder<T> decoder, @Nullable final String indexName, @Nullable final Integer batchSize,
            @Nullable final Collation collation, @Nullable final BsonValue comment, @Nullable final Boolean allowDiskUse,
            final boolean retryReads) {
        this.timeoutSettings = timeoutSettings;
        this.namespace = namespace;
        this.decoder = decoder;
        this.allowDiskUse = allowDiskUse;
        this.batchSize = batchSize;
        this.collation = collation;
        this.comment = comment;
        this.indexName = indexName;
        this.retryReads = retryReads;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        try {
            return asAggregateOperation().execute(binding);
        } catch (MongoCommandException exception) {
            int cursorBatchSize = batchSize == null ? 0 : batchSize;
            if (!isNamespaceError(exception)) {
                throw exception;
            } else {
                return createEmptyBatchCursor(namespace, decoder, exception.getServerAddress(), cursorBatchSize);
            }
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        asAggregateOperation().executeAsync(binding, (cursor, exception) -> {
            if (exception != null && !isNamespaceError(exception)) {
                callback.onResult(null, exception);
            } else if (exception != null) {
                MongoCommandException commandException = (MongoCommandException) exception;
                AsyncBatchCursor<T> emptyAsyncBatchCursor = createEmptyAsyncBatchCursor(namespace, commandException.getServerAddress());
                callback.onResult(emptyAsyncBatchCursor, null);
            } else {
                callback.onResult(cursor, null);
            }
        });
    }

    @Override
    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return asAggregateOperation().asExplainableOperation(verbosity, resultDecoder);
    }

    @Override
    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return asAggregateOperation().asAsyncExplainableOperation(verbosity, resultDecoder);
    }

    private AggregateOperation<T> asAggregateOperation() {
        BsonDocument searchDefinition = getSearchDefinition();
        BsonDocument listSearchIndexesStage = new BsonDocument(STAGE_LIST_SEARCH_INDEXES, searchDefinition);
        return new AggregateOperation<>(timeoutSettings, namespace, singletonList(listSearchIndexesStage), decoder)
                .retryReads(retryReads)
                .collation(collation)
                .comment(comment)
                .allowDiskUse(allowDiskUse)
                .batchSize(batchSize);
    }

    @NonNull
    private BsonDocument getSearchDefinition() {
        if (indexName == null) {
            return new BsonDocument();
        }
        return new BsonDocument("name", new BsonString(indexName));
    }
}
