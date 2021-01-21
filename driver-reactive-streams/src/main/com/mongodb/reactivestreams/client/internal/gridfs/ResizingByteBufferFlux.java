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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.lang.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class ResizingByteBufferFlux extends Flux<ByteBuffer> {

    private final Publisher<ByteBuffer> source;
    private final int outputByteBufferSize;


    ResizingByteBufferFlux(final Publisher<ByteBuffer> source, final int outputByteBufferSize) {
        notNull("source must not be null", source);
        isTrue("'outputByteBufferSize' must be a positive number", outputByteBufferSize >= 0);
        this.source = source;
        this.outputByteBufferSize = outputByteBufferSize;
    }

    @Override
    public void subscribe(final CoreSubscriber<? super ByteBuffer> actual) {
        Flux.<ByteBuffer>push(sink -> {

            BaseSubscriber<ByteBuffer> subscriber = new BaseSubscriber<ByteBuffer>() {
                private volatile ByteBuffer remainder;
                private final AtomicLong requested = new AtomicLong();
                private volatile boolean startedProcessing = false;
                private volatile boolean finished = false;

                @Override
                protected void hookOnSubscribe(final Subscription subscription) {
                    sink.onCancel(() -> upstream().cancel());
                    sink.onRequest(l -> {
                        synchronized (this) {
                            requested.addAndGet(l);
                            if (!startedProcessing) {
                                startedProcessing = true;
                                upstream().request(1);
                            }
                        }
                    });
                }

                @Override
                protected void hookOnNext(@NonNull final ByteBuffer value) {
                    if (remainder == null || remainder.remaining() == 0) {
                        remainder = value;
                    } else {
                        byte[] byteArray = new byte[remainder.remaining() + value.remaining()];
                        ByteBuffer newBuffer = ByteBuffer.wrap(byteArray);
                        copyByteBuffer(remainder, newBuffer);
                        copyByteBuffer(value, newBuffer);
                        ((Buffer) newBuffer).flip();
                        remainder = newBuffer;
                    }

                    while (remainder != null && remainder.remaining() >= outputByteBufferSize) {
                        int newLimit = remainder.position() + outputByteBufferSize;
                        ByteBuffer next = remainder.duplicate();
                        ((Buffer) next).limit(newLimit);
                        requested.decrementAndGet();
                        sink.next(next);
                        ((Buffer) remainder).position(newLimit);
                    }

                    if (requested.get() > 0) {
                        upstream().request(1);
                    }
                }

                @Override
                protected void hookOnComplete() {
                    if (!finished) {
                        finished = true;
                        if (remainder != null && remainder.remaining() > 0) {
                            sink.next(remainder);
                        }
                        sink.complete();
                    }
                }

                @Override
                protected void hookOnError(@NonNull final Throwable throwable) {
                    sink.error(throwable);
                }

                private void copyByteBuffer(final ByteBuffer original, final ByteBuffer destination) {
                    if (original.hasArray() && destination.hasArray()) {
                        System.arraycopy(original.array(), original.position(), destination.array(), destination.position(),
                                         original.remaining());
                        ((Buffer) destination).position(destination.position() + original.remaining());
                    } else {
                        destination.put(original);
                    }
                }
            };

            source.subscribe(subscriber);
        }, FluxSink.OverflowStrategy.BUFFER)
                .subscribe(actual);
    }
}
