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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class MongoIterableImpl<TResult> implements MongoIterable<TResult> {
    private final ClientSession clientSession;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;
    private final ReadPreference readPreference;
    private final boolean retryReads;
    private final TimeoutSettings timeoutSettings;
    private Integer batchSize;
    private TimeoutMode timeoutMode;

    public MongoIterableImpl(@Nullable final ClientSession clientSession, final OperationExecutor executor, final ReadConcern readConcern,
                             final ReadPreference readPreference, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        this.clientSession = clientSession;
        this.executor = notNull("executor", executor);
        this.readConcern = notNull("readConcern", readConcern);
        this.readPreference = notNull("readPreference", readPreference);
        this.retryReads = retryReads;
        this.timeoutSettings = timeoutSettings;
    }

    public abstract ReadOperationCursor<TResult> asReadOperation();

    @Nullable
    ClientSession getClientSession() {
        return clientSession;
    }

    protected abstract OperationExecutor getExecutor();

    OperationExecutor getExecutor(final TimeoutSettings timeoutSettings) {
        return executor.withTimeoutSettings(timeoutSettings);
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    protected ReadConcern getReadConcern() {
        return readConcern;
    }

    protected boolean getRetryReads() {
        return retryReads;
    }

    protected TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    @Override
    public MongoIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Nullable
    public TimeoutMode getTimeoutMode() {
        return timeoutMode;
    }

    public MongoIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        if (timeoutSettings.getTimeoutMS() == null) {
            throw new IllegalArgumentException("TimeoutMode requires timeoutMS to be set.");
        }
        this.timeoutMode = timeoutMode;
        return this;
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return new MongoBatchCursorAdapter<>(execute());
    }

    @Override
    public MongoCursor<TResult> cursor() {
        return iterator();
    }

    @Nullable
    @Override
    public TResult first() {
        try (MongoCursor<TResult> cursor = iterator()) {
            if (!cursor.hasNext()) {
                return null;
            }
            return cursor.next();
        }
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<>(this, mapper);
    }

    @Override
    public void forEach(final Consumer<? super TResult> action) {
        try (MongoCursor<TResult> cursor = iterator()) {
            while (cursor.hasNext()) {
                action.accept(cursor.next());
            }
        }
    }

    @Override
    public <A extends Collection<? super TResult>> A into(final A target) {
        forEach(target::add);
        return target;
    }

    private BatchCursor<TResult> execute() {
        return getExecutor().execute(asReadOperation(), readPreference, readConcern, clientSession);
    }


    protected long validateMaxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        long maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);

        isTrueArgument("maxAwaitTimeMS must be less than timeoutMS", timeoutMS == null || timeoutMS == 0
                || timeoutMS > maxAwaitTimeMS);

        return maxAwaitTimeMS;
    }
}
