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

import com.mongodb.reactivestreams.client.internal.Publishers;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;

public class PublishersVerification extends PublisherVerification<Integer> {

    public PublishersVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }

    @Override
    public Publisher<Integer> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        return Publishers.publishAndFlatten(callback ->
                callback.onResult(IntStream.rangeClosed(1, (int) elements).boxed().collect(Collectors.toList()), null)
        );
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 100;
    }
}
