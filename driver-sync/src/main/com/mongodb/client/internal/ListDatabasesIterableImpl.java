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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ListDatabasesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListDatabasesIterable<TResult> {
    private final Operations<BsonDocument> operations;
    private final Class<TResult> resultClass;

    private long maxTimeMS;
    private Bson filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;
    private BsonValue comment;

    public ListDatabasesIterableImpl(@Nullable final ClientSession clientSession, final Class<TResult> resultClass,
            final CodecRegistry codecRegistry, final ReadPreference readPreference, final OperationExecutor executor,
            final boolean retryReads, final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads, timeoutSettings); // TODO: read concern?
        this.operations = new Operations<>(BsonDocument.class, readPreference, codecRegistry, retryReads, timeoutSettings);
        this.resultClass = notNull("clazz", resultClass);
    }

    @Override
    public ListDatabasesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> nameOnly(@Nullable final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> authorizedDatabasesOnly(@Nullable final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ReadOperationCursor<TResult> asReadOperation() {
        return operations.listDatabases(resultClass, filter, nameOnly, authorizedDatabasesOnly, comment);
    }

    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS));
    }
}
