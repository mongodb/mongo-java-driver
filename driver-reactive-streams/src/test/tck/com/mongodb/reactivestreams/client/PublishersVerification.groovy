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

package com.mongodb.reactivestreams.client

import com.mongodb.Block
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.reactivestreams.client.internal.Publishers
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment

@SuppressWarnings(['CloseWithoutCloseable', 'UnusedMethodParameter', 'EmptyMethod'])
class PublishersVerification extends PublisherVerification<Integer> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 5000L
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L

    PublishersVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS)
    }

    @Override
    Publisher<Integer> createPublisher(long elements) {
        assert (elements <= maxElementsFromPublisher())

        Publishers.publishAndFlatten(new Block<SingleResultCallback<List<Integer>>>() {
            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult((0..<elements) as List<Integer>, null)
            }
        });
    }

    @Override
    Publisher<Integer> createFailedPublisher() {
        null
    }

    @Override
    long maxElementsFromPublisher() {
        100
    }
}
