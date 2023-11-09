/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncExplainableReadOperation;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ListSearchIndexesPublisher;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

final class ListSearchIndexesPublisherImpl<T> extends BatchCursorPublisher<T> implements ListSearchIndexesPublisher<T> {
    @Nullable
    private Boolean allowDiskUse;
    private long maxTimeMS;
    @Nullable
    private Collation collation;
    @Nullable
    private BsonValue comment;
    @Nullable
    private String indexName;

    ListSearchIndexesPublisherImpl(
            final MongoOperationPublisher<T> mongoOperationPublisher) {
        super(null, mongoOperationPublisher);
    }

    @Override
    public ListSearchIndexesPublisher<T> name(final String indexName) {
        this.indexName = notNull("indexName", indexName);
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public ListSearchIndexesPublisher<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public Publisher<Document> explain() {
        return publishExplain(Document.class, null);
    }

    @Override
    public Publisher<Document> explain(final ExplainVerbosity verbosity) {
        notNull("verbosity", verbosity);

        return publishExplain(Document.class, verbosity);
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass) {
        notNull("explainResultClass", explainResultClass);
        return publishExplain(explainResultClass, null);
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        notNull("verbosity", verbosity);
        notNull("explainResultClass", explainResultClass);
        return publishExplain(explainResultClass, verbosity);
    }

    private <E> Publisher<E> publishExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        return getMongoOperationPublisher().createReadOperationMono(() ->
                asAggregateOperation(1).asAsyncExplainableOperation(verbosity,
                        getCodecRegistry().get(explainResultClass)), getClientSession());
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(final int initialBatchSize) {
        return asAggregateOperation(initialBatchSize);
    }

    private AsyncExplainableReadOperation<AsyncBatchCursor<T>> asAggregateOperation(final int initialBatchSize) {
        return getOperations().listSearchIndexes(getDocumentClass(), maxTimeMS, indexName, initialBatchSize, collation,
                comment,
                allowDiskUse);
    }
}
