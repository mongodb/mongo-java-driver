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

import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

final class DistinctPublisherImpl<T> extends BatchCursorPublisher<T> implements DistinctPublisher<T> {

    private final String fieldName;
    private Bson filter;
    private long maxTimeMS;
    private Collation collation;

    DistinctPublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<T> mongoOperationPublisher,
            final String fieldName, final Bson filter) {
        super(clientSession, mongoOperationPublisher);
        this.fieldName = notNull("fieldName", fieldName);
        this.filter = notNull("filter", filter);
    }

    @Override
    public DistinctPublisher<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Deprecated
    @Override
    public DistinctPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public DistinctPublisher<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(final int initialBatchSize) {
        // initialBatchSize is ignored for distinct operations.
        return getOperations().distinct(getClientSideOperationTimeout(maxTimeMS), fieldName, filter, getDocumentClass(), collation);
    }
}
