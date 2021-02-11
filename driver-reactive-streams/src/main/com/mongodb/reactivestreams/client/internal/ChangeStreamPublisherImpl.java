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
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.ChangeStreamOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;



final class ChangeStreamPublisherImpl<T> extends BatchCursorPublisher<ChangeStreamDocument<T>>
        implements ChangeStreamPublisher<T> {

    private final List<? extends Bson> pipeline;
    private final Codec<ChangeStreamDocument<T>> codec;
    private final ChangeStreamLevel changeStreamLevel;

    private FullDocument fullDocument = FullDocument.DEFAULT;
    private BsonDocument resumeToken;
    private BsonDocument startAfter;
    private long maxAwaitTimeMS;
    private Collation collation;
    private BsonTimestamp startAtOperationTime;

    ChangeStreamPublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<?> mongoOperationPublisher,
            final Class<T> innerResultClass,
            final List<? extends Bson> pipeline,
            final ChangeStreamLevel changeStreamLevel) {
        this(clientSession, mongoOperationPublisher,
             ChangeStreamDocument.createCodec(notNull("innerResultClass", innerResultClass),
                                              mongoOperationPublisher.getCodecRegistry()),
             notNull("pipeline", pipeline), notNull("changeStreamLevel", changeStreamLevel));
    }

    private ChangeStreamPublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<?> mongoOperationPublisher,
            final Codec<ChangeStreamDocument<T>> codec,
            final List<? extends Bson> pipeline,
            final ChangeStreamLevel changeStreamLevel) {
        super(clientSession, mongoOperationPublisher.withDocumentClass(codec.getEncoderClass()));
        this.pipeline = pipeline;
        this.codec = codec;
        this.changeStreamLevel = changeStreamLevel;
    }

    @Override
    public ChangeStreamPublisher<T> fullDocument(final FullDocument fullDocument) {
        this.fullDocument = notNull("fullDocument", fullDocument);
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> resumeAfter(final BsonDocument resumeAfter) {
        this.resumeToken = notNull("resumeAfter", resumeAfter);
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> collation(@Nullable final Collation collation) {
        this.collation = notNull("collation", collation);
        return this;
    }

    @Override
    public <TDocument> Publisher<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        BatchCursorPublisher<TDocument> result = new BatchCursorPublisher<TDocument>(
                getClientSession(), getMongoOperationPublisher().withDocumentClass(clazz)) {
            @Override
            AsyncReadOperation<AsyncBatchCursor<TDocument>> asAsyncReadOperation(final int initialBatchSize) {
                return createChangeStreamOperation(getMongoOperationPublisher().getCodecRegistry().get(clazz), initialBatchSize);
            }
        };
        Integer batchSize = getBatchSize();
        if (batchSize != null) {
            result.batchSize(batchSize);
        }
        return result;
    }

    @Override
    public ChangeStreamPublisher<T> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = notNull("startAtOperationTime", startAtOperationTime);
        return this;
    }

    @Override
    public ChangeStreamPublisherImpl<T> startAfter(final BsonDocument startAfter) {
        this.startAfter = notNull("startAfter", startAfter);
        return this;
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<ChangeStreamDocument<T>>> asAsyncReadOperation(final int initialBatchSize) {
        return createChangeStreamOperation(codec, initialBatchSize);
    }

    private <S> AsyncReadOperation<AsyncBatchCursor<S>> createChangeStreamOperation(final Codec<S> codec, final int initialBatchSize) {
        return new ChangeStreamOperation<>(getNamespace(), fullDocument,
                                           createBsonDocumentList(pipeline), codec, changeStreamLevel)
                .batchSize(initialBatchSize)
                .collation(collation)
                .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)
                .resumeAfter(resumeToken)
                .startAtOperationTime(startAtOperationTime)
                .startAfter(startAfter)
                .retryReads(getRetryReads());
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            aggregateList.add(obj.toBsonDocument(BsonDocument.class, getCodecRegistry()));
        }
        return aggregateList;
    }
}
