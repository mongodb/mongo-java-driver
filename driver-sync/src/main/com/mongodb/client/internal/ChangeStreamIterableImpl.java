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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ChangeStreamIterableImpl<TResult> extends MongoIterableImpl<ChangeStreamDocument<TResult>>
        implements ChangeStreamIterable<TResult> {
    private final CodecRegistry codecRegistry;
    private final List<? extends Bson> pipeline;
    private final Codec<ChangeStreamDocument<TResult>> codec;
    private final ChangeStreamLevel changeStreamLevel;
    private final Operations<TResult> operations;
    private FullDocument fullDocument = FullDocument.DEFAULT;
    private FullDocumentBeforeChange fullDocumentBeforeChange = FullDocumentBeforeChange.DEFAULT;
    private BsonDocument resumeToken;
    private BsonDocument startAfter;
    private long maxAwaitTimeMS;
    private Collation collation;
    private BsonTimestamp startAtOperationTime;
    private BsonValue comment;
    private boolean showExpandedEvents;

    public ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final String databaseName,
            final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
            final OperationExecutor executor, final List<? extends Bson> pipeline, final Class<TResult> resultClass,
            final ChangeStreamLevel changeStreamLevel, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), codecRegistry, readPreference, readConcern, executor, pipeline,
                resultClass, changeStreamLevel, retryReads, timeoutSettings);
    }

    public ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace,
                                    final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                                    final OperationExecutor executor, final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                    final ChangeStreamLevel changeStreamLevel, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutSettings);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.codec = ChangeStreamDocument.createCodec(notNull("resultClass", resultClass), codecRegistry);
        this.changeStreamLevel = notNull("changeStreamLevel", changeStreamLevel);
        this.operations = new Operations<>(namespace, resultClass, readPreference, codecRegistry, retryReads, timeoutSettings);
    }

    @Override
    public ChangeStreamIterable<TResult> fullDocument(final FullDocument fullDocument) {
        this.fullDocument = notNull("fullDocument", fullDocument);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> fullDocumentBeforeChange(final FullDocumentBeforeChange fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = notNull("fullDocumentBeforeChange", fullDocumentBeforeChange);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> resumeAfter(final BsonDocument resumeAfter) {
        this.resumeToken = notNull("resumeAfter", resumeAfter);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS =  validateMaxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = notNull("collation", collation);
        return this;
    }

    @Override
    public <TDocument> MongoIterable<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        return new MongoIterableImpl<TDocument>(getClientSession(), getExecutor(), getReadConcern(), getReadPreference(), getRetryReads(),
                getTimeoutSettings()) {
            @Override
            public MongoCursor<TDocument> iterator() {
                return cursor();
            }

            @Override
            public MongoChangeStreamCursor<TDocument> cursor() {
                return new MongoChangeStreamCursorImpl<>(execute(), codecRegistry.get(clazz), initialResumeToken());
            }

            @Override
            public ReadOperationCursor<TDocument> asReadOperation() {
                throw new UnsupportedOperationException();
            }

            @Override

            protected OperationExecutor getExecutor() {
                return ChangeStreamIterableImpl.this.getExecutor();
            }
        };
    }

    @Override
    public ChangeStreamIterable<TResult> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = notNull("startAtOperationTime", startAtOperationTime);
        return this;
    }

    @Override
    public ChangeStreamIterableImpl<TResult> startAfter(final BsonDocument startAfter) {
        this.startAfter = notNull("startAfter", startAfter);
        return this;
    }


    @Override
    public ChangeStreamIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment == null ? null : new BsonString(comment);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> showExpandedEvents(final boolean showExpandedEvents) {
        this.showExpandedEvents = showExpandedEvents;
        return this;
    }

    @Override
    public MongoCursor<ChangeStreamDocument<TResult>> iterator() {
        return cursor();
    }

    @Override
    public MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor() {
        return new MongoChangeStreamCursorImpl<>(execute(), codec, initialResumeToken());
    }

    @Nullable
    @Override
    public ChangeStreamDocument<TResult> first() {
        try (MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor = cursor()) {
            if (!cursor.hasNext()) {
                return null;
            }
            return cursor.next();
        }
    }

    @Override
    public ReadOperationCursor<ChangeStreamDocument<TResult>> asReadOperation() {
        throw new UnsupportedOperationException();
    }


    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(0, maxAwaitTimeMS));
    }

    private ReadOperationCursor<RawBsonDocument> createChangeStreamOperation() {
        return operations.changeStream(fullDocument, fullDocumentBeforeChange, pipeline, new RawBsonDocumentCodec(), changeStreamLevel,
                getBatchSize(), collation, comment, resumeToken, startAtOperationTime, startAfter, showExpandedEvents);
    }

    private BatchCursor<RawBsonDocument> execute() {
        return getExecutor().execute(createChangeStreamOperation(), getReadPreference(), getReadConcern(), getClientSession());
    }

    private BsonDocument initialResumeToken() {
        return startAfter != null ? startAfter : resumeToken;
    }
}
