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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.ChangeStreamOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ChangeStreamIterableImpl<TResult> extends MongoIterableImpl<ChangeStreamDocument<TResult>>
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


    ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final CodecRegistry codecRegistry,
                             final ReadPreference readPreference, final ReadConcern readConcern, final OperationExecutor executor,
                             final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                             final ChangeStreamLevel changeStreamLevel, final boolean retryReads) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), codecRegistry, readPreference, readConcern, executor, pipeline,
                resultClass, changeStreamLevel, retryReads);
    }

    ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final CodecRegistry codecRegistry,
                             final ReadPreference readPreference, final ReadConcern readConcern, final OperationExecutor executor,
                             final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                             final ChangeStreamLevel changeStreamLevel, final boolean retryReads) {
        super(clientSession, executor, readConcern, readPreference, retryReads);
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
        return new MongoIterableImpl<TDocument>(getClientSession(), getExecutor(), getReadConcern(), getReadPreference(), getRetryReads()) {
            private AsyncReadOperation<AsyncBatchCursor<TDocument>> operation = createChangeStreamOperation(codecRegistry.get(clazz));

            @Override
            AsyncReadOperation<AsyncBatchCursor<TDocument>> asAsyncReadOperation() {
                return operation;
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
    AsyncReadOperation<AsyncBatchCursor<ChangeStreamDocument<TResult>>> asAsyncReadOperation() {
        return createChangeStreamOperation(codec);
    }

    private <S> AsyncReadOperation<AsyncBatchCursor<S>> createChangeStreamOperation(final Codec<S> codec) {
        return new ChangeStreamOperation<S>(namespace, fullDocument,  createBsonDocumentList(pipeline), codec, changeStreamLevel)
                .batchSize(getBatchSize())
                .collation(collation)
                .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)
                .resumeAfter(resumeToken)
                .startAtOperationTime(startAtOperationTime)
                .startAfter(startAfter)
                .retryReads(getRetryReads());
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            aggregateList.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return aggregateList;
    }
}
