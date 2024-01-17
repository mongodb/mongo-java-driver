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

package com.mongodb.reactivestreams.client;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class TestEventPublisher<T> {
    private final Sinks.Many<T> sink;

    public TestEventPublisher() {
        this.sink = Sinks.many().unicast().onBackpressureBuffer();
    }

    // Method to send events
    public void sendEvent(final T event) {
        sink.tryEmitNext(event);
    }

    public Flux<T> getEventStream() {
        return sink.asFlux();
    }

    public long currentSubscriberCount() {
        return sink.currentSubscriberCount();
    }

    public void complete() {
        sink.tryEmitComplete();
    }
}
