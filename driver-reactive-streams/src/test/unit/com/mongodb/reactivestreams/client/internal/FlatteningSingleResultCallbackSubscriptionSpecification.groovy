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
import com.mongodb.MongoException
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.reactivestreams.client.TestSubscriber
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


class FlatteningSingleResultCallbackSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def block = Mock(Block)
        def subscriber = new TestSubscriber()

        when:
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        then:
        0 * block.apply(_)

        when:
        subscriber.requestMore(1)

        then:
        1 * block.apply(_)
    }

    def 'should call onComplete after all data has been consumed'() {
        given:
        SingleResultCallback<List> listSingleResultCallback = null
        def executor = Executors.newFixedThreadPool(5)
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(new Block<SingleResultCallback<List>>() {
            @Override
            void apply(final SingleResultCallback<List> callback) {
                listSingleResultCallback = callback
            }
        }).subscribe(subscriber)

        when:
        subscriber.requestMore(5)
        subscriber.requestMore(5)

        then:
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([])
        subscriber.assertNoTerminalEvent()

        when:
        100.times { executor.submit { subscriber.requestMore(1) } }
        listSingleResultCallback?.onResult([1, 2, 3, 4], null)

        then:
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1, 2, 3, 4])
        subscriber.assertTerminalEvent()

        cleanup:
        executor?.shutdown()
        executor?.awaitTermination(10, TimeUnit.SECONDS)
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def block = getBlock()
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        when:
        subscriber.requestMore(0)

        then:
        subscriber.assertErrored()
    }

    def 'should call onError if batch returns an throwable in the callback'() {
        given:
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(new Block<SingleResultCallback<List<Integer>>>() {
            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult(null, new MongoException('Failed'))
            }
        }).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def block = getBlock()
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.requestMore(5)

        then: // check that the subscriber is finished
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1, 2, 3, 4])
        subscriber.assertTerminalEvent()
    }

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def block = getBlock()
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        subscriber.assertReceivedOnNext([1])

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def block = getBlock()
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should not call onError after unsubscribe is called'() {
        given:
        def block = getBlock()
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                if (result == 2) {
                    throw new MongoException('Failure')
                }
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publishAndFlatten(block).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])

        when:
        subscriber.requestMore(5)

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should call onError if onNext causes an Error'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                throw new MongoException('Failure')
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publishAndFlatten(getBlock()).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        notThrown(MongoException)
        subscriber.assertTerminalEvent()
        subscriber.assertErrored()
    }

    def 'should throw the exception if calling onComplete raises one'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
                throw new MongoException('exception calling onComplete')
            }
        })
        Publishers.publishAndFlatten(getBlock()).subscribe(subscriber)

        when:
        subscriber.requestMore(100)

        then:
        def ex = thrown(MongoException)
        ex.message == 'Subscription has already been terminated'
        ex.cause.cause.message == 'exception calling onComplete'
        subscriber.assertTerminalEvent()
        subscriber.assertNoErrors()
    }

    def 'should throw the exception if calling onError raises one'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                throw new MongoException('fail')
            }

            @Override
            void onError(final Throwable e) {
                throw new MongoException('exception calling onError')
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publishAndFlatten(getBlock()).subscribe(subscriber)

        when:
        subscriber.requestMore(100)

        then:
        def ex = thrown(MongoException)
        ex.message == 'Subscription has already been terminated'
        ex.cause.cause.message == 'exception calling onError'
        subscriber.assertTerminalEvent()
        subscriber.assertErrored()
    }

    def 'should call onError if the passed block errors'() {
        given:
        def subscriber = new TestSubscriber()
        Publishers.publishAndFlatten(new Block<SingleResultCallback<List<Integer>>>() {
            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                throw new MongoException('failed')
            }
        }).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def getBlock() {
        new Block<SingleResultCallback<List<Integer>>>() {

            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult([1, 2, 3, 4], null)
            }
        }
    }
}
