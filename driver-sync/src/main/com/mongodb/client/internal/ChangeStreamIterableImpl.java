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
import com.mongodb.internal.ClientSideOperationTimeouts;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ChangeStreamOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public class ChangeStreamIterableImpl<TResult> extends MongoIterableImpl<ChangeStreamDocument<TResult>>
        implements ChangeStreamIterable<TResult> {
    private final MongoNamespace namespace;
    private final CodecRegistry codecRegistry;
    private final List<? extends Bson> pipeline;
    private final Codec<ChangeStreamDocument<TResult>> codec;
    private final ChangeStreamLevel changeStreamLevel;

    private FullDocument fullDocument = FullDocument.DEFAULT;
    private BsonDocument resumeToken;
    private BsonDocument startAfter;
    private long maxAwaitTimeMS;
    private Collation collation;
    private BsonTimestamp startAtOperationTime;

    public ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final String databaseName,
                                    final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                                    final OperationExecutor executor, final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                    final ChangeStreamLevel changeStreamLevel, final boolean retryReads,
                                    @Nullable final Long timeoutMS) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), codecRegistry, readPreference, readConcern, executor, pipeline,
                resultClass, changeStreamLevel, retryReads, timeoutMS);
    }

    public ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace,
                                    final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                                    final OperationExecutor executor, final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                    final ChangeStreamLevel changeStreamLevel, final boolean retryReads, @Nullable final Long timeoutMS) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutMS);
        this.namespace = notNull("namespace", namespace);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.codec = ChangeStreamDocument.createCodec(notNull("resultClass", resultClass), codecRegistry);
        this.changeStreamLevel = notNull("changeStreamLevel", changeStreamLevel);
    }

    @Override
    public ChangeStreamIterable<TResult> fullDocument(final FullDocument fullDocument) {
        this.fullDocument = notNull("fullDocument", fullDocument);
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
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = notNull("collation", collation);
        return this;
    }

    @Override
    public <TDocument> MongoIterable<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        return new MongoIterableImpl<TDocument>(getClientSession(), getExecutor(), getReadConcern(), getReadPreference(),
                getRetryReads(), getTimeoutMS()) {
            @Override
            public MongoCursor<TDocument> iterator() {
                return cursor();
            }

            @Override
            public MongoChangeStreamCursor<TDocument> cursor() {
                return new MongoChangeStreamCursorImpl<TDocument>(execute(), codecRegistry.get(clazz), initialResumeToken());
            }

            @Override
            public ReadOperation<BatchCursor<TDocument>> asReadOperation() {
                throw new UnsupportedOperationException();
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
    public MongoCursor<ChangeStreamDocument<TResult>> iterator() {
        return cursor();
    }

    @Override
    public MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor() {
        return new MongoChangeStreamCursorImpl<ChangeStreamDocument<TResult>>(execute(), codec, initialResumeToken());
    }

    @Nullable
    @Override
    public ChangeStreamDocument<TResult> first() {
        MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor = cursor();
        try {
            if (!cursor.hasNext()) {
                return null;
            }
            return cursor.next();
        } finally {
            cursor.close();
        }
    }

    @Override
    public ReadOperation<BatchCursor<ChangeStreamDocument<TResult>>> asReadOperation() {
        throw new UnsupportedOperationException();
    }

    private ReadOperation<BatchCursor<RawBsonDocument>> createChangeStreamOperation() {
        return new ChangeStreamOperation<>(ClientSideOperationTimeouts.create(getTimeoutMS(), 0, maxAwaitTimeMS),
                namespace, fullDocument, createBsonDocumentList(pipeline),
                                                          new RawBsonDocumentCodec(), changeStreamLevel)
                        .batchSize(getBatchSize())
                        .collation(collation)
                        .resumeAfter(resumeToken)
                        .startAtOperationTime(startAtOperationTime)
                        .startAfter(startAfter)
                        .retryReads(getRetryReads());
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline cannot contain a null value");
            }
            aggregateList.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return aggregateList;
    }

    private BatchCursor<RawBsonDocument> execute() {
        return getExecutor().execute(createChangeStreamOperation(), getReadPreference(), getReadConcern(), getClientSession());
    }

    private BsonDocument initialResumeToken() {
        return startAfter != null ? startAfter : resumeToken;
    }
}
