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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.Block
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.reactivestreams.client.TestSubscriber
import spock.lang.Specification

class SingleResultObservableToPublisherSpecification extends Specification {

    def 'should be cold and nothing should happen until request is called'() {
        given:
        def subscriber = new TestSubscriber()
        def requested = false

        when:
        def publisher = new SingleResultObservableToPublisher<Integer>( new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(SingleResultCallback<Integer> callback) {
                requested = true;
                callback.onResult(1, null);
            }
        })

        then:
        !requested

        when:
        publisher.subscribe(subscriber)

        then:
        !requested

        when:
        subscriber.requestMore(1)

        then:
        requested
        subscriber.assertReceivedOnNext([1])
        subscriber.assertNoErrors()
        subscriber.assertTerminalEvent()
    }

    def 'should handle null values'() {
        given:
        def subscriber = new TestSubscriber()

        when:
        new SingleResultObservableToPublisher<Integer>( new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(SingleResultCallback<Integer> callback) {
                callback.onResult(null, null);
            }
        }).subscribe(subscriber)

        then:
        subscriber.assertReceivedOnNext([])
        subscriber.assertNoErrors()
        subscriber.assertNoTerminalEvent()

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertReceivedOnNext([])
        subscriber.assertNoErrors()
        subscriber.assertTerminalEvent()
    }

}
