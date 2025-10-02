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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListDatabasesPublisherImpl<T> extends BatchCursorPublisher<T> implements ListDatabasesPublisher<T> {

    private long maxTimeMS;
    private Bson filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;
    private BsonValue comment;

    ListDatabasesPublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<T> mongoOperationPublisher) {
        super(clientSession, mongoOperationPublisher);
    }

    public ListDatabasesPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListDatabasesPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    public ListDatabasesPublisher<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    public ListDatabasesPublisher<T> nameOnly(@Nullable final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public ListDatabasesPublisher<T> authorizedDatabasesOnly(@Nullable final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    @Override
    public ListDatabasesPublisher<T> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public ListDatabasesPublisher<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ListDatabasesPublisher<T> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    Function<Operations<?>, TimeoutSettings> getTimeoutSettings() {
        return (operations -> operations.createTimeoutSettings(maxTimeMS));
    }

    ReadOperationCursor<T> asReadOperation(final int initialBatchSize) {
        // initialBatchSize is ignored for distinct operations.
        return getOperations().listDatabases(getDocumentClass(), filter, nameOnly, authorizedDatabasesOnly, comment);
    }
}
