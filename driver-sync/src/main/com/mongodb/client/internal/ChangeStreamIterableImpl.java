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
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ChangeStreamOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.client.ClientSession;
import org.bson.BsonDocument;
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

    private FullDocument fullDocument = FullDocument.DEFAULT;
    private BsonDocument resumeToken;
    private long maxAwaitTimeMS;
    private Collation collation;


    ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final CodecRegistry codecRegistry,
                             final ReadPreference readPreference, final ReadConcern readConcern, final OperationExecutor executor,
                             final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        super(clientSession, executor, readConcern, readPreference);
        this.namespace = notNull("namespace", namespace);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.codec = ChangeStreamDocument.createCodec(notNull("resultClass", resultClass), codecRegistry);
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
        return new MongoIterableImpl<TDocument>(getClientSession(), getExecutor(), getReadConcern(), getReadPreference()) {
            private ReadOperation<BatchCursor<TDocument>> operation = createChangeStreamOperation(codecRegistry.get(clazz));

            @Override
            public ReadOperation<BatchCursor<TDocument>> asReadOperation() {
                return operation;
            }
        };
    }

    @Override
    public ReadOperation<BatchCursor<ChangeStreamDocument<TResult>>> asReadOperation() {
        return createChangeStreamOperation(codec);
    }

    private <S> ReadOperation<BatchCursor<S>> createChangeStreamOperation(final Codec<S> codec) {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);

        ChangeStreamOperation<S> changeStreamOperation =
                new ChangeStreamOperation<S>(namespace, fullDocument, aggregateList, codec)
                        .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)
                        .batchSize(getBatchSize())
                        .readConcern(getReadConcern())
                        .collation(collation);

        if (resumeToken != null) {
            changeStreamOperation.resumeAfter(resumeToken);
        }

        return changeStreamOperation;
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
