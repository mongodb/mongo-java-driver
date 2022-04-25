/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import java.util.concurrent.TimeUnit;

class SyncChangeStreamIterable<T> extends SyncMongoIterable<ChangeStreamDocument<T>> implements ChangeStreamIterable<T> {
    private final ChangeStreamPublisher<T> wrapped;
    @Nullable
    private Integer batchSize;

    SyncChangeStreamIterable(final ChangeStreamPublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    public MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor() {
        final MongoCursor<ChangeStreamDocument<T>> wrapped = super.cursor();
        return new MongoChangeStreamCursor<ChangeStreamDocument<T>>() {
            @Override
            public BsonDocument getResumeToken() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                wrapped.close();
            }

            @Override
            public boolean hasNext() {
                return wrapped.hasNext();
            }

            @Override
            public ChangeStreamDocument<T> next() {
                return wrapped.next();
            }

            @Override
            public int available() {
                return wrapped.available();
            }

            @Override
            public ChangeStreamDocument<T> tryNext() {
                return wrapped.tryNext();
            }

            @Override
            public ServerCursor getServerCursor() {
                return wrapped.getServerCursor();
            }

            @Override
            public ServerAddress getServerAddress() {
                return wrapped.getServerAddress();
            }
        };
    }

    @Override
    public ChangeStreamIterable<T> fullDocument(final FullDocument fullDocument) {
        wrapped.fullDocument(fullDocument);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> fullDocumentBeforeChange(final FullDocumentBeforeChange fullDocumentBeforeChange) {
        wrapped.fullDocumentBeforeChange(fullDocumentBeforeChange);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> resumeAfter(final BsonDocument resumeToken) {
        wrapped.resumeAfter(resumeToken);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        this.batchSize = batchSize;
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public <TDocument> MongoIterable<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        SyncMongoIterable<TDocument> result = new SyncMongoIterable<>(wrapped.withDocumentClass(clazz));
        if (batchSize != null) {
            result.batchSize(batchSize);
        }
        return result;
    }

    @Override
    public ChangeStreamIterable<T> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        wrapped.startAtOperationTime(startAtOperationTime);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> startAfter(final BsonDocument startAfter) {
        wrapped.startAfter(startAfter);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ChangeStreamIterable<T> comment(@Nullable final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }
}
