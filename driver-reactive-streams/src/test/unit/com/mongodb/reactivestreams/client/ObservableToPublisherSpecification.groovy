/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.MongoException
import com.mongodb.async.client.Observable
import com.mongodb.async.client.Observer
import com.mongodb.async.client.Subscription
import com.mongodb.reactivestreams.client.internal.ObservableToPublisher
import spock.lang.Specification

class ObservableToPublisherSpecification extends Specification {

    def 'should be cold and nothing should happen until request is called'() {
        given:
        def subscriber = new TestSubscriber()
        def requested = false;

        when:
        def publisher = new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                        requested = true
                        observer.onComplete()
                    }

                    @Override
                    void unsubscribe() {
                    }

                    @Override
                    boolean isUnsubscribed() {
                        false
                    }
                })
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
        subscriber.assertNoErrors()
        subscriber.assertTerminalEvent()
    }

    def 'should pass Observer.onNext values to Subscriber.onNext'() {
        given:
        def subscriber = new TestSubscriber()

        when:
        new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                        (1..n).each{
                            observer.onNext(it.intValue())
                        }
                    }

                    @Override
                    void unsubscribe() {
                    }

                    @Override
                    boolean isUnsubscribed() {
                        false
                    }
                })
            }
        }).subscribe(subscriber)
        subscriber.requestMore(3)

        then:
        subscriber.assertReceivedOnNext([1, 2, 3])
    }

    def 'should pass Observer.onError values to Subscriber.onError'() {
        given:
        def subscriber = new TestSubscriber()

        when:
        new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                        observer.onError(new MongoException('failed'))
                    }

                    @Override
                    void unsubscribe() {
                    }

                    @Override
                    boolean isUnsubscribed() {
                        false
                    }
                })
            }
        }).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def 'should trigger Subscriber.onComplete when Observer.onComplete is called'() {
        given:
        def subscriber = new TestSubscriber()

        when:
        new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                        observer.onComplete()
                    }

                    @Override
                    void unsubscribe() {
                    }

                    @Override
                    boolean isUnsubscribed() {
                        false
                    }
                })
            }
        }).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        subscriber.assertNoErrors()
        subscriber.assertTerminalEvent()
    }

    def 'should unsubscribe from the async subscription if reactive streams subscription is cancelled'() {
        given:
        def unsubscribed = false
        def subscriber = new TestSubscriber()

        when:
        new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                    }

                    @Override
                    void unsubscribe() {
                        unsubscribed = true
                    }

                    @Override
                    boolean isUnsubscribed() {
                        unsubscribed
                    }
                })
            }
        }).subscribe(subscriber)

        then:
        !unsubscribed

        when:
        subscriber.getSubscription().cancel()

        then:
        unsubscribed
    }

    def 'should trigger onError if request is less than 1'() {
        given:
        def subscriber = new TestSubscriber()

        when:
        new ObservableToPublisher(new Observable() {
            @Override
            void subscribe(final Observer observer) {
                observer.onSubscribe(new Subscription() {
                    @Override
                    void request(final long n) {
                    }

                    @Override
                    void unsubscribe() {
                    }

                    @Override
                    boolean isUnsubscribed() {
                        false
                    }
                })
            }
        }).subscribe(subscriber)
        subscriber.requestMore(0)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

}
