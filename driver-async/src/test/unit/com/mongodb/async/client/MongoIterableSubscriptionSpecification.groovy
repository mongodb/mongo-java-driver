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

package com.mongodb.async.client

import com.mongodb.MongoException
import com.mongodb.async.AsyncBatchCursor
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static com.mongodb.async.client.Observables.observe

class MongoIterableSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def mongoIterable = Mock(MongoIterable)
        def observer = new TestObserver()

        when:
        observe(mongoIterable).subscribe(observer)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        observer.requestMore(1)

        then:
        1 * mongoIterable.batchCursor(_)
    }

    def 'should call batchCursor.next when requested data is more than queued data'() {
        given:
        def mongoIterable = getMongoIterable()
        def observer = new TestObserver()

        when:
        observe(mongoIterable).subscribe(observer)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        observer.requestMore(2)

        then:
        1 * mongoIterable.batchSize(2)
        observer.assertReceivedOnNext([1, 2])

        when:
        observer.requestMore(3)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()
    }

    def 'should call onComplete after cursor has completed and all onNext values requested'() {
        given:
        def mongoIterable = getMongoIterable()
        def executor = Executors.newFixedThreadPool(5)
        def observer = new TestObserver()
        observe(mongoIterable).subscribe(observer)

        when:
        100.times { executor.submit { observer.requestMore(1) } }
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()

        cleanup:
        executor?.shutdown()
        executor?.awaitTermination(10, TimeUnit.SECONDS)
    }

    def 'should call onError if batchCursor returns an throwable in the callback'() {
        given:
        def observer = new TestObserver()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(null, new MongoException('failed'))
            }
        }
        observe(mongoIterable).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def 'should call onError if batchCursor returns a null for the cursor in the callback'() {
        given:
        def observer = new TestObserver()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(null, null)
            }
        }
        observe(mongoIterable).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def 'should call onError if batchCursor.next returns an throwable in the callback'() {
        given:
        def observer = new TestObserver()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(Mock(AsyncBatchCursor) {
                    next(_) >> { it[0].onResult(null, new MongoException('failed')) }
                }, null)
            }
        }
        observe(mongoIterable).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def 'should set batchSize to 2 if request is passed 1'() {
        given:
        def observer = new TestObserver()
        def mockIterable = getMongoIterable()
        observe(mockIterable).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        1 * mockIterable.batchSize(2)
    }

    def 'should set batchSize to Integer.MAX_VALUE if request is passed a bigger value'() {
        given:
        def observer = new TestObserver()
        def mockIterable = getMongoIterable()
        observe(mockIterable).subscribe(observer)

        when:
        observer.requestMore(Long.MAX_VALUE)

        then:
        1 * mockIterable.batchSize(Integer.MAX_VALUE)
        observer.assertTerminalEvent()
    }

    def 'should set batchSize on the cursor to 2 if request is passed 1'() {
        given:
        def observer = new TestObserver()
        def cursor = getCursor()
        def mockIterable = getMongoIterable(cursor)
        observe(mockIterable).subscribe(observer)

        when:
        observer.requestMore(2)
        observer.requestMore(1)
        observer.requestMore(100)

        then:
        1 * mockIterable.batchSize(2)
        2 * cursor.setBatchSize(2)
        observer.assertTerminalEvent()
    }

    def 'should set batchSize to Integer.MAX_VALUE  on the cursor if request is passed a bigger value'() {
        given:
        def observer = new TestObserver()
        def cursor = getCursor()
        def mockIterable = getMongoIterable(cursor)
        observe(mockIterable).subscribe(observer)

        when:
        observer.requestMore(2)
        observer.requestMore(Long.MAX_VALUE)

        then:
        1 * mockIterable.batchSize(2)
        2 * cursor.setBatchSize(Integer.MAX_VALUE)
        observer.assertTerminalEvent()
    }

    def 'should use the set batchSize when configured on the mongoIterable'() {
        given:
        def observer = new TestObserver()
        def cursor = Mock(AsyncBatchCursor) {
            def cursorResults = [(1..3), (1..3), (1..3)]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
        def mockIterable = getMongoIterable(cursor)
        _ * mockIterable.getBatchSize() >> { 3 }
        observe(mockIterable).subscribe(observer)

        when:
        observer.getSubscription()
        observer.requestMore(4)

        then:
        1 * mockIterable.batchSize(3)
        2 * cursor.setBatchSize(3)

        when:
        observer.requestMore(Long.MAX_VALUE)

        then:
        2 * cursor.setBatchSize(3)
        observer.assertTerminalEvent()
    }

    def 'should use negative batchSize values when configured on the mongoIterable'() {
        given:
        def observer = new TestObserver()
        def cursor = Mock(AsyncBatchCursor) {
            def cursorResults = [(1..3)]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
        def mockIterable = getMongoIterable(cursor)
        _ * mockIterable.getBatchSize() >> { -3 }
        observe(mockIterable).subscribe(observer)

        when:
        observer.getSubscription()
        observer.requestMore(4)

        then:
        1 * mockIterable.batchSize(-3)
        2 * cursor.setBatchSize(-3)
        observer.assertTerminalEvent()
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def observer = new TestObserver()
        observe(Stub(MongoIterable)).subscribe(observer)

        when:
        observer.requestMore(0)

        then:
        thrown IllegalArgumentException
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def mongoIterable = getMongoIterable()
        def observer = new TestObserver()
        observe(mongoIterable).subscribe(observer)

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

    def 'should close the batchCursor when unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        observe(getMongoIterable(cursor)).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertSubscribed()

        when:
        observer.getSubscription().unsubscribe()

        then:
        1 * cursor.close()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        observe(getMongoIterable(cursor)).subscribe(observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(10)

        then:
        0 * cursor.next(_)
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        observe(getMongoIterable(cursor)).subscribe(observer)

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
        observe(getMongoIterable()).subscribe(observer)

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
        observe(getMongoIterable()).subscribe(observer)

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
        observe(getMongoIterable()).subscribe(observer)

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
        observe(getMongoIterable()).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onError'
        observer.assertTerminalEvent()
        observer.assertErrored()
    }

    def 'should call onError if MongoIterable errors'() {
        given:
        def observer = new TestObserver()
        observe(getMongoIterable(getFailingCursor(failImmediately))).subscribe(observer)

        when:
        observer.requestMore(3)

        then:
        observer.assertTerminalEvent()
        observer.assertErrored()

        where:
        failImmediately << [true, false]
    }

    def getMongoIterable() {
        getMongoIterable(getCursor())
    }

    def getMongoIterable(AsyncBatchCursor cursor) {
        Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(cursor, null)
            }
        }
    }

    def getCursor() {
        Mock(AsyncBatchCursor) {
            def cursorResults = [[1, 2], [3, 4]]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
    }

    def getFailingCursor(boolean failImmediately) {
        Mock(AsyncBatchCursor) {
            def cursorResults = [[1, 2]]
            def hasSetBatchSize = failImmediately
            setBatchSize(_) >> {
                if (hasSetBatchSize) {
                    throw new MongoException('Failure')
                } else {
                    hasSetBatchSize = true
                }
            }
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
    }
}
