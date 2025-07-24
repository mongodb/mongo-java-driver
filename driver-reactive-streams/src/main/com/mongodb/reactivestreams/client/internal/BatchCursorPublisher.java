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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

@VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PROTECTED)
public abstract class BatchCursorPublisher<T> implements Publisher<T> {
    private final ClientSession clientSession;
    private final MongoOperationPublisher<T> mongoOperationPublisher;
    private Integer batchSize;
    private TimeoutMode timeoutMode;

    BatchCursorPublisher(@Nullable final ClientSession clientSession, final MongoOperationPublisher<T> mongoOperationPublisher) {
        this(clientSession, mongoOperationPublisher, null);
    }

    BatchCursorPublisher(@Nullable final ClientSession clientSession, final MongoOperationPublisher<T> mongoOperationPublisher,
                         @Nullable final Integer batchSize) {
        this.clientSession = clientSession;
        this.mongoOperationPublisher = notNull("mongoOperationPublisher", mongoOperationPublisher);
        this.batchSize = batchSize;
    }

    abstract ReadOperationCursor<T> asReadOperation(int initialBatchSize);
    abstract Function<Operations<?>, TimeoutSettings> getTimeoutSettings();

    ReadOperationCursor<T> asReadOperationFirst() {
        return asReadOperation(1);
    }

    @Nullable
    ClientSession getClientSession() {
        return clientSession;
    }

    MongoOperationPublisher<T> getMongoOperationPublisher() {
        return mongoOperationPublisher;
    }

    Operations<T> getOperations() {
        return mongoOperationPublisher.getOperations();
    }

    MongoNamespace getNamespace() {
        return assertNotNull(mongoOperationPublisher.getNamespace());
    }

    ReadPreference getReadPreference() {
        return mongoOperationPublisher.getReadPreference();
    }

    CodecRegistry getCodecRegistry() {
        return mongoOperationPublisher.getCodecRegistry();
    }

    boolean getRetryReads() {
        return mongoOperationPublisher.getRetryReads();
    }

    Class<T> getDocumentClass() {
        return mongoOperationPublisher.getDocumentClass();
    }


    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    public Publisher<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Publisher<T> timeoutMode(final TimeoutMode timeoutMode) {
        if (mongoOperationPublisher.getTimeoutSettings().getTimeoutMS() == null) {
            throw new IllegalArgumentException("TimeoutMode requires timeoutMS to be set.");
        }
        this.timeoutMode = timeoutMode;
        return this;
    }

    @Nullable
    public TimeoutMode getTimeoutMode() {
        return timeoutMode;
    }

    public Publisher<T> first() {
        return batchCursor(this::asReadOperationFirst)
                .flatMap(batchCursor -> {
                    batchCursor.setBatchSize(1);
                    return Mono.from(batchCursor.next())
                            .doOnTerminate(batchCursor::close)
                            .flatMap(results -> {
                                if (results == null || results.isEmpty()) {
                                    return Mono.empty();
                                }
                                return Mono.fromCallable(() -> results.get(0));
                            });
                });
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        new BatchCursorFlux<>(this).subscribe(subscriber);
    }

    public Mono<BatchCursor<T>> batchCursor(final int initialBatchSize) {
        return batchCursor(() -> asReadOperation(initialBatchSize));
    }

    Mono<BatchCursor<T>> batchCursor(final Supplier<ReadOperation<?, AsyncBatchCursor<T>>> supplier) {
        return mongoOperationPublisher.createReadOperationMono(getTimeoutSettings(), supplier, clientSession).map(BatchCursor::new);
    }

    protected long validateMaxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        Long timeoutMS = mongoOperationPublisher.getTimeoutSettings().getTimeoutMS();
        long maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);

        isTrueArgument("maxAwaitTimeMS must be less than timeoutMS", timeoutMS == null || timeoutMS == 0
                || timeoutMS > maxAwaitTimeMS);

        return maxAwaitTimeMS;
    }
}
