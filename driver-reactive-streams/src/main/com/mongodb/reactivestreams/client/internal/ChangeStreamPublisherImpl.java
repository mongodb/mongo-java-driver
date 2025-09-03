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
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;


final class ChangeStreamPublisherImpl<T> extends BatchCursorPublisher<ChangeStreamDocument<T>>
        implements ChangeStreamPublisher<T> {

    private final List<? extends Bson> pipeline;
    private final Codec<ChangeStreamDocument<T>> codec;
    private final ChangeStreamLevel changeStreamLevel;

    private FullDocument fullDocument = FullDocument.DEFAULT;
    private FullDocumentBeforeChange fullDocumentBeforeChange = FullDocumentBeforeChange.DEFAULT;
    private BsonDocument resumeToken;
    private BsonDocument startAfter;
    private long maxAwaitTimeMS;
    private Collation collation;
    private BsonValue comment;
    private BsonTimestamp startAtOperationTime;
    private boolean showExpandedEvents;

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
    public ChangeStreamPublisher<T> fullDocumentBeforeChange(final FullDocumentBeforeChange fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = notNull("fullDocumentBeforeChange", fullDocumentBeforeChange);
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
    public ChangeStreamPublisher<T> comment(@Nullable final String comment) {
        this.comment = comment == null ? null : new BsonString(comment);
        return this;
    }


    @Override
    public ChangeStreamPublisher<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS = validateMaxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamPublisher<T> collation(@Nullable final Collation collation) {
        this.collation = notNull("collation", collation);
        return this;
    }

    @Override
    public <TDocument> Publisher<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        return new BatchCursorPublisher<TDocument>(getClientSession(), getMongoOperationPublisher().withDocumentClass(clazz),
                getBatchSize()) {
            @Override
            ReadOperationCursor<TDocument> asReadOperation(final int initialBatchSize) {
                return createChangeStreamOperation(getMongoOperationPublisher().getCodecRegistry().get(clazz), initialBatchSize);
            }

            @Override
            Function<Operations<?>, TimeoutSettings> getTimeoutSettings() {
                return (operations -> operations.createTimeoutSettings(0, maxAwaitTimeMS));
            }
        };
    }

    @Override
    public ChangeStreamPublisher<T> showExpandedEvents(final boolean showExpandedEvents) {
        this.showExpandedEvents = showExpandedEvents;
        return this;
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
    ReadOperationCursor<ChangeStreamDocument<T>> asReadOperation(final int initialBatchSize) {
        return createChangeStreamOperation(codec, initialBatchSize);
    }


    @Override
    Function<Operations<?>, TimeoutSettings> getTimeoutSettings() {
        return (operations -> operations.createTimeoutSettings(0, maxAwaitTimeMS));
    }

    private <S> ReadOperationCursor<S> createChangeStreamOperation(final Codec<S> codec, final int initialBatchSize) {
        return getOperations().changeStream(fullDocument, fullDocumentBeforeChange, pipeline, codec, changeStreamLevel, initialBatchSize,
                collation, comment, resumeToken, startAtOperationTime, startAfter, showExpandedEvents);
    }
}
