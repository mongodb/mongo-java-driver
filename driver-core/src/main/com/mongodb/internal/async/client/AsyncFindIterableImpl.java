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

package com.mongodb.internal.async.client;

import com.mongodb.CursorType;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


class AsyncFindIterableImpl<TDocument, TResult> extends AsyncMongoIterableImpl<TResult> implements AsyncFindIterable<TResult> {
    private final AsyncOperations<TDocument> operations;
    private final Class<TResult> resultClass;
    private final FindOptions findOptions;

    private Bson filter;

    AsyncFindIterableImpl(@Nullable final AsyncClientSession clientSession, final MongoNamespace namespace,
                          final Class<TDocument> documentClass, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                          final ReadPreference readPreference, final ReadConcern readConcern, final OperationExecutor executor,
                          final Bson filter, final boolean retryReads) {
        super(clientSession, executor, readConcern, readPreference, retryReads);
        this.operations = new AsyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("resultClass", resultClass);
        this.filter = notNull("filter", filter);
        this.findOptions = new FindOptions();
    }

    @Override
    public AsyncFindIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> collation(@Nullable final Collation collation) {
        findOptions.collation(collation);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> projection(@Nullable final Bson projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> sort(@Nullable final Bson sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    @Deprecated
    public AsyncFindIterable<TResult> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> comment(@Nullable final String comment) {
        findOptions.comment(comment);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> hint(@Nullable final Bson hint) {
        findOptions.hint(hint);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> hintString(@Nullable final String hint) {
        findOptions.hintString(hint);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> max(@Nullable final Bson max) {
        findOptions.max(max);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> min(@Nullable final Bson min) {
        findOptions.min(min);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> returnKey(final boolean returnKey) {
        findOptions.returnKey(returnKey);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> showRecordId(final boolean showRecordId) {
        findOptions.showRecordId(showRecordId);
        return this;
    }

    @Override
    public AsyncFindIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        findOptions.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        getExecutor().execute(operations.findFirst(filter, resultClass, findOptions), getReadPreference(), getReadConcern(),
                getClientSession(), new SingleResultCallback<AsyncBatchCursor<TResult>>() {
                    @Override
                    public void onResult(final AsyncBatchCursor<TResult> batchCursor, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            batchCursor.next(new SingleResultCallback<List<TResult>>() {
                                @Override
                                public void onResult(final List<TResult> result, final Throwable t) {
                                    batchCursor.close();
                                    if (t != null) {
                                        callback.onResult(null, t);
                                    } else if (result == null || result.isEmpty()) {
                                        callback.onResult(null, null);
                                    } else {
                                        callback.onResult(result.get(0), null);
                                    }
                                }
                            });
                        }
                    }
                });
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<TResult>> asAsyncReadOperation() {
        return createFindOperation();
    }

    private AsyncReadOperation<AsyncBatchCursor<TResult>> createFindOperation() {
        return operations.find(filter, resultClass, findOptions);
    }
}
