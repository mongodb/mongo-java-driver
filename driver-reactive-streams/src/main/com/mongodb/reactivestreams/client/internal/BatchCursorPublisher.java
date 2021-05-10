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
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.Operations;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;

abstract class BatchCursorPublisher<T> implements Publisher<T> {
    private final ClientSession clientSession;
    private final MongoOperationPublisher<T> mongoOperationPublisher;
    private Integer batchSize;

    BatchCursorPublisher(@Nullable final ClientSession clientSession, final MongoOperationPublisher<T> mongoOperationPublisher) {
        this.clientSession = clientSession;
        this.mongoOperationPublisher = notNull("mongoOperationPublisher", mongoOperationPublisher);
    }

    abstract AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(int initialBatchSize);

    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncFirstReadOperation() {
        return asAsyncReadOperation(1);
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

    ClientSideOperationTimeoutFactory getClientSideOperationTimeoutFactory(final long maxTimeMS) {
        return mongoOperationPublisher.getClientSideOperationTimeoutFactory(maxTimeMS);
    }

    ClientSideOperationTimeoutFactory getClientSideOperationTimeoutFactory(final long maxTimeMS, final long maxAwaitTimeMS) {
        return mongoOperationPublisher.getClientSideOperationTimeoutFactory(maxTimeMS, maxAwaitTimeMS);
    }

    MongoNamespace getNamespace() {
        return mongoOperationPublisher.getNamespace();
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

    public Publisher<T> first() {
        return batchCursor(this::asAsyncFirstReadOperation)
                .flatMap(batchCursor -> Mono.create(sink -> {
                    batchCursor.setBatchSize(1);
                    Mono.from(batchCursor.next())
                            .doOnTerminate(batchCursor::close)
                            .doOnError(sink::error)
                            .doOnSuccess(results -> {
                                if (results == null || results.isEmpty()) {
                                    sink.success();
                                } else {
                                    sink.success(results.get(0));
                                }
                            })
                            .subscribe();
                }));
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        new BatchCursorFlux<>(this).subscribe(subscriber);
    }

    public Mono<BatchCursor<T>> batchCursor(final int initialBatchSize) {
        return batchCursor(() -> asAsyncReadOperation(initialBatchSize));
    }

    Mono<BatchCursor<T>> batchCursor(final Supplier<AsyncReadOperation<AsyncBatchCursor<T>>> supplier) {
        return mongoOperationPublisher.createReadOperationMono(supplier, clientSession).map(BatchCursor::new);
    }

}
