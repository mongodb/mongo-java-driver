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

package com.mongodb.client.async

import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.async.FutureResultCallback
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class FutureResultCallbackSpecification extends Specification {

    def 'should return false if tried to cancel'() {
        when:
        def futureResultCallback = new FutureResultCallback()

        then:
        !futureResultCallback.cancel(false)
        !futureResultCallback.cancel(true)
        !futureResultCallback.isCancelled()
    }

    def 'should return true if done'() {
        when:
        def futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(null, null)

        then:
        futureResultCallback.isDone()
    }

    def 'should return the result on get'() {
        when:
        def futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(true, null)

        then:
        futureResultCallback.get()

        when:
        futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(null, new MongoException('failed'))
        futureResultCallback.get()

        then:
        thrown MongoException

        when:
        futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(true, null)

        then:
        futureResultCallback.get(1, SECONDS)

        when:
        futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(null, new MongoException('failed'))
        futureResultCallback.get(1, SECONDS)

        then:
        thrown MongoException
    }

    def 'should timeout when no result and called get'(){
        when:
        def futureResultCallback = new FutureResultCallback()
        futureResultCallback.get(1, MILLISECONDS)

        then:
        thrown MongoTimeoutException
    }

    def 'should throw an error if onResult called more than once'() {
        when:
        def futureResultCallback = new FutureResultCallback()
        futureResultCallback.onResult(true, null)

        then:
        futureResultCallback.get()

        when:
        futureResultCallback.onResult(false, null)

        then:
        thrown IllegalStateException
    }
}
