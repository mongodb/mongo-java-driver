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

package com.mongodb.internal.async.client

import com.mongodb.Block
import com.mongodb.MongoException
import com.mongodb.internal.async.SingleResultCallback
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static com.mongodb.internal.async.client.Observables.observeAndFlatten

class FlatteningSingleResultCallbackSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def block = Mock(Block)
        def observer = new TestObserver()

        when:
        observeAndFlatten(block).subscribe(observer)

        then:
        0 * block.apply(_)

        when:
        observer.requestMore(1)

        then:
        1 * block.apply(_)
    }

    def 'should call onComplete after all data has been consumed'() {
        given:
        SingleResultCallback<List> listSingleResultCallback = null
        def executor = Executors.newFixedThreadPool(5)
        def observer = new TestObserver()
        observeAndFlatten(new Block<SingleResultCallback<List>>() {
            @Override
            void apply(final SingleResultCallback<List> callback) {
                listSingleResultCallback = callback
            }
        }).subscribe(observer)

        when:
        observer.requestMore(5)
        observer.requestMore(5)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([])
        observer.assertNoTerminalEvent()

        when:
        100.times { executor.submit { observer.requestMore(1) } }
        listSingleResultCallback?.onResult([1, 2, 3, 4], null)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()

        cleanup:
        executor?.shutdown()
        executor?.awaitTermination(10, TimeUnit.SECONDS)
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observeAndFlatten(block).subscribe(observer)

        when:
        observer.requestMore(0)

        then:
        thrown IllegalArgumentException
    }

    def 'should call onError if batch returns an throwable in the callback'() {
        given:
        def observer = new TestObserver()
        observeAndFlatten(new Block<SingleResultCallback<List<Integer>>>() {
            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult(null, new MongoException('Failed'));
            }
        }).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observeAndFlatten(block).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertSubscribed()

        when:
        observer.requestMore(5)

        then: // check that the observer is finished
        observer.assertSubscribed()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()

        when: // unsubscribe
        observer.getSubscription().unsubscribe()

        then: // check the subscriber is unsubscribed
        observer.assertUnsubscribed()
    }

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observeAndFlatten(block).subscribe(observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(1)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observeAndFlatten(block).subscribe(observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])
    }

    def 'should not call onError after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver(new Observer() {
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
        observeAndFlatten(block).subscribe(observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(5)

        then:
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])
    }

    def 'should call onError if onNext causes an Error'() {
        given:
        def observer = new TestObserver(new Observer() {
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
        observeAndFlatten(getBlock()).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        notThrown(MongoException)
        observer.assertTerminalEvent()
        observer.assertErrored()
    }

    def 'should throw the exception if calling onComplete raises one'() {
        given:
        def observer = new TestObserver(new Observer() {
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
        observeAndFlatten(getBlock()).subscribe(observer)

        when:
        observer.requestMore(100)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onComplete'
        observer.assertTerminalEvent()
        observer.assertNoErrors()
    }

    def 'should throw the exception if calling onError raises one'() {
        given:
        def observer = new TestObserver(new Observer() {
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
        observeAndFlatten(getBlock()).subscribe(observer)

        when:
        observer.requestMore(100)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onError'
        observer.assertTerminalEvent()
        observer.assertErrored()
    }

    def 'should call onError if the passed block errors'() {
        given:
        def observer = new TestObserver()
        observeAndFlatten(new Block<SingleResultCallback<List<Integer>>>() {
            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                throw new MongoException('failed')
            }
        }).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def getBlock() {
        new Block<SingleResultCallback<List<Integer>>>() {

            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult([1, 2, 3, 4], null);
            }
        }
    }
}
