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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class OneShotPublisherTest {
    @Test
    void from() {
        assertAll(
                () -> {
                    List<Integer> elements = asList(1, 2);
                    assertEquals(elements, Flux.from(OneShotPublisher.from(Flux.fromIterable(asList(1, 2)))).collectList().block());
                },
                () -> assertOneShot(OneShotPublisher.from(Mono.just(1))),
                () -> assertOneShot(OneShotPublisher.from(Flux.fromIterable(asList(1, 2))))
        );
    }

    void assertOneShot(final Publisher<?> publisher) {
        Executable blockFirst = () -> Flux.from(publisher).blockFirst();
        assertDoesNotThrow(blockFirst);
        assertThrows(IllegalStateException.class, blockFirst);
    }

    private OneShotPublisherTest() {
    }
}
