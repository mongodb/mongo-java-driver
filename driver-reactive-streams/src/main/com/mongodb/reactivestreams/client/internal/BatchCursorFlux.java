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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class BatchCursorFlux<T> implements Publisher<T> {

    private final BatchCursorPublisher<T> batchCursorPublisher;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final AtomicLong demandDelta = new AtomicLong(0);
    private BatchCursor<T> batchCursor;
    private FluxSink<T> sink;

    BatchCursorFlux(final BatchCursorPublisher<T> batchCursorPublisher) {
        this.batchCursorPublisher = batchCursorPublisher;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        Flux.<T>create(sink -> {
            this.sink = sink;
            sink.onRequest(demand -> {
                if (calculateDemand(demand) > 0 && inProgress.compareAndSet(false, true)) {
                    if (batchCursor == null) {
                        int batchSize = calculateBatchSize(sink.requestedFromDownstream());
                        batchCursorPublisher.batchCursor(batchSize).subscribe(bc -> {
                            batchCursor = bc;
                            inProgress.set(false);
                            recurseCursor();
                        }, sink::error);
                    } else {
                        inProgress.set(false);
                        recurseCursor();
                    }
                }
            });
            sink.onCancel(this::closeCursor);
            sink.onDispose(this::closeCursor);
        }, FluxSink.OverflowStrategy.BUFFER)
        .subscribe(subscriber);
    }

    private void closeCursor() {
        if (batchCursor != null && !batchCursor.isClosed()) {
            batchCursor.close();
        }
    }

    void recurseCursor(){
        if (!sink.isCancelled() && sink.requestedFromDownstream() > 0 && inProgress.compareAndSet(false, true)) {
            if (batchCursor.isClosed()) {
                sink.complete();
            } else {
                batchCursor.setBatchSize(calculateBatchSize(sink.requestedFromDownstream()));
                Mono.from(batchCursor.next())
                        .doOnCancel(this::closeCursor)
                        .doOnError((e) -> {
                            closeCursor();
                            sink.error(e);
                        })
                        .doOnSuccess(results -> {
                            if (results != null) {
                                results.forEach(sink::next);
                                calculateDemand(-results.size());
                            }
                            if (batchCursor.isClosed()) {
                                sink.complete();
                            } else {
                                inProgress.set(false);
                                recurseCursor();
                            }
                        })
                        .subscribe();
                }
        }
    }

    synchronized long calculateDemand(final long demand) {
        long originalDelta = demandDelta.get();
        long newDelta = demandDelta.addAndGet(demand);
        if (demand > 0 && newDelta < originalDelta) {
            demandDelta.set(Long.MAX_VALUE);
            return Long.MAX_VALUE;
        }
        return newDelta;
    }

    int calculateBatchSize(final long demand) {
        Integer setBatchSize = batchCursorPublisher.getBatchSize();
        if (setBatchSize != null) {
            return setBatchSize;
        } else if (demand > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return Math.max(2, (int) demand);
    }

}
