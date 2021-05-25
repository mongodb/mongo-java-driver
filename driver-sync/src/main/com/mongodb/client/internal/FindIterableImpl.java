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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeouts;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ExplainableReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class FindIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements FindIterable<TResult> {

    private final SyncOperations<TDocument> operations;

    private final Class<TResult> resultClass;
    private final FindOptions findOptions;
    private final CodecRegistry codecRegistry;

    private Bson filter;
    private long maxTimeMS;
    private long maxAwaitTimeMS;

    FindIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                     final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                     final ReadConcern readConcern, final OperationExecutor executor, final Bson filter, @Nullable final Long timeoutMS) {
        this(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern, executor, filter, true,
                timeoutMS);
    }

    FindIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                     final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                     final ReadConcern readConcern, final OperationExecutor executor, final Bson filter, final boolean retryReads,
                     @Nullable final Long timeoutMS) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutMS);
        this.operations = new SyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("resultClass", resultClass);
        this.filter = notNull("filter", filter);
        this.findOptions = new FindOptions();
        this.codecRegistry = codecRegistry;
    }

    @Override
    public FindIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public FindIterable<TResult> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public FindIterable<TResult> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Deprecated
    @Override
    public FindIterableImpl<TDocument, TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterableImpl<TDocument, TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<TResult> collation(@Nullable final Collation collation) {
        findOptions.collation(collation);
        return this;
    }

    @Override
    public FindIterable<TResult> projection(@Nullable final Bson projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public FindIterable<TResult> sort(@Nullable final Bson sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    @Deprecated
    public FindIterable<TResult> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindIterable<TResult> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public FindIterable<TResult> comment(@Nullable final String comment) {
        findOptions.comment(comment);
        return this;
    }

    @Override
    public FindIterable<TResult> hint(@Nullable final Bson hint) {
        findOptions.hint(hint);
        return this;
    }

    @Override
    public FindIterable<TResult> hintString(@Nullable final String hint) {
        findOptions.hintString(hint);
        return this;
    }

    @Override
    public FindIterable<TResult> max(@Nullable final Bson max) {
        findOptions.max(max);
        return this;
    }

    @Override
    public FindIterable<TResult> min(@Nullable final Bson min) {
        findOptions.min(min);
        return this;
    }

    @Override
    public FindIterable<TResult> returnKey(final boolean returnKey) {
        findOptions.returnKey(returnKey);
        return this;
    }

    @Override
    public FindIterable<TResult> showRecordId(final boolean showRecordId) {
        findOptions.showRecordId(showRecordId);
        return this;
    }

    @Override
    public FindIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        findOptions.allowDiskUse(allowDiskUse);
        return this;
    }

    @Nullable
    @Override
    public TResult first() {
        BatchCursor<TResult> batchCursor =
                getExecutor().execute(operations.findFirst(
                        ClientSideOperationTimeouts.create(getTimeoutMS(), maxTimeMS, maxAwaitTimeMS),
                        filter, resultClass, findOptions),
                getReadPreference(), getReadConcern(),
                getClientSession());
        try {
            return batchCursor.hasNext() ? batchCursor.next().iterator().next() : null;
        } finally {
            batchCursor.close();
        }
    }

    @Override
    public Document explain() {
        return executeExplain(Document.class, null);
    }

    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        return executeExplain(Document.class, notNull("verbosity", verbosity));
    }

    @Override
    public <E> E explain(final Class<E> explainDocumentClass) {
        return executeExplain(explainDocumentClass, null);
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return executeExplain(explainResultClass, notNull("verbosity", verbosity));
    }

    private <E> E executeExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        notNull("explainDocumentClass", explainResultClass);
        return getExecutor().execute(asReadOperation().asExplainableOperation(verbosity, codecRegistry.get(explainResultClass)),
                getReadPreference(), getReadConcern(), getClientSession());
    }

    public ExplainableReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.find(ClientSideOperationTimeouts.create(getTimeoutMS(), maxTimeMS, maxAwaitTimeMS), filter,
                               resultClass, findOptions);
    }
}
