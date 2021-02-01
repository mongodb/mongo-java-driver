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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncExplainableReadOperation;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

final class FindPublisherImpl<T> extends BatchCursorPublisher<T> implements FindPublisher<T> {
    private final FindOptions findOptions;

    private Bson filter;

    FindPublisherImpl(@Nullable final ClientSession clientSession, final MongoOperationPublisher<T> mongoOperationPublisher,
            final Bson filter) {
        super(clientSession, mongoOperationPublisher);
        this.filter = notNull("filter", filter);
        this.findOptions = new FindOptions();
    }

    @Override
    public FindPublisher<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public FindPublisher<T> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public FindPublisher<T> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Override
    public FindPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindPublisher<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public FindPublisher<T> collation(@Nullable final Collation collation) {
        findOptions.collation(collation);
        return this;
    }

    @Override
    public FindPublisher<T> projection(@Nullable final Bson projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public FindPublisher<T> sort(@Nullable final Bson sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public FindPublisher<T> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    @Deprecated
    public FindPublisher<T> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindPublisher<T> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public FindPublisher<T> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public FindPublisher<T> comment(@Nullable final String comment) {
        findOptions.comment(comment);
        return this;
    }

    @Override
    public FindPublisher<T> hint(@Nullable final Bson hint) {
        findOptions.hint(hint);
        return this;
    }

    @Override
    public FindPublisher<T> hintString(@Nullable final String hint) {
        findOptions.hintString(hint);
        return this;
    }

    @Override
    public FindPublisher<T> max(@Nullable final Bson max) {
        findOptions.max(max);
        return this;
    }

    @Override
    public FindPublisher<T> min(@Nullable final Bson min) {
        findOptions.min(min);
        return this;
    }

    @Override
    public FindPublisher<T> returnKey(final boolean returnKey) {
        findOptions.returnKey(returnKey);
        return this;
    }

    @Override
    public FindPublisher<T> showRecordId(final boolean showRecordId) {
        findOptions.showRecordId(showRecordId);
        return this;
    }

    @Override
    public FindPublisher<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        findOptions.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public Publisher<Document> explain() {
        return publishExplain(Document.class, null);
    }

    @Override
    public Publisher<Document> explain(final ExplainVerbosity verbosity) {
        return publishExplain(Document.class, notNull("verbosity", verbosity));
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass) {
        return publishExplain(explainResultClass, null);
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return publishExplain(explainResultClass, notNull("verbosity", verbosity));
    }

    private <E> Publisher<E> publishExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        notNull("explainDocumentClass", explainResultClass);
        return getMongoOperationPublisher().createReadOperationMono(() ->
                        asAsyncReadOperation(0).asAsyncExplainableOperation(verbosity,
                                getCodecRegistry().get(explainResultClass)),
                getClientSession());
    }

    @Override
    AsyncExplainableReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(final int initialBatchSize) {
        int originalBatchSize = findOptions.getBatchSize();
        FindOperation<T> operation = getOperations().find(filter, getDocumentClass(), findOptions.batchSize(initialBatchSize));
        findOptions.batchSize(originalBatchSize);
        return operation;
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncFirstReadOperation() {
        return getOperations().findFirst(filter, getDocumentClass(), findOptions);
    }
}
