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
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.ListCollectionsIterable;
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

class ListCollectionsIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListCollectionsIterable<TResult> {
    private final Operations<BsonDocument> operations;
    private final String databaseName;
    private final Class<TResult> resultClass;
    private Bson filter;
    private final boolean collectionNamesOnly;
    private boolean authorizedCollections;
    private long maxTimeMS;
    private BsonValue comment;

    ListCollectionsIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final boolean collectionNamesOnly,
                                final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                final OperationExecutor executor, final boolean retryReads,  final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads, timeoutSettings); // TODO: read concern?
        this.collectionNamesOnly = collectionNamesOnly;
        this.operations = new Operations<>(BsonDocument.class, readPreference, codecRegistry, retryReads, timeoutSettings);
        this.databaseName = notNull("databaseName", databaseName);
        this.resultClass = notNull("resultClass", resultClass);
    }

    @Override
    public ListCollectionsIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * @see ListCollectionNamesIterable#authorizedCollections(boolean)
     */
    ListCollectionsIterableImpl<TResult> authorizedCollections(final boolean authorizedCollections) {
        this.authorizedCollections = authorizedCollections;
        return this;
    }

    @Override
    public ReadOperationCursor<TResult> asReadOperation() {
        return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, authorizedCollections,
                getBatchSize(), comment, getTimeoutMode());
    }


    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS));
    }
}
