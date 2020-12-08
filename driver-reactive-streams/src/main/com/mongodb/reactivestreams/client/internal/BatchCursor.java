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

import com.mongodb.internal.async.AsyncBatchCursor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.reactivestreams.client.internal.MongoOperationPublisher.sinkToCallback;

public class BatchCursor<T> implements AutoCloseable {

    private final AsyncBatchCursor<T> wrapped;

    public BatchCursor(final AsyncBatchCursor<T> wrapped) {
        this.wrapped = wrapped;
    }

    public Publisher<List<T>> next() {
        return Mono.create(sink -> wrapped.next(sinkToCallback(sink)));
    }

    public Publisher<List<T>> tryNext() {
        return Mono.create(sink -> wrapped.tryNext(sinkToCallback(sink)));
    }

    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    public boolean isClosed() {
        return wrapped.isClosed();
    }

    public void close() {
        wrapped.close();
    }

}
