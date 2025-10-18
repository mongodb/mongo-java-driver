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

package com.mongodb.internal.operation

import com.mongodb.MongoClientSettings
import com.mongodb.MongoException
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.TimeoutSettings
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.connection.OperationContext
import com.mongodb.internal.tracing.TracingManager
import org.bson.Document
import org.bson.RawBsonDocument
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static java.util.concurrent.TimeUnit.SECONDS

class AsyncChangeStreamBatchCursorSpecification extends Specification {

    def 'should call the underlying AsyncCommandBatchCursor'() {
        given:
        def changeStreamOperation = Stub(ChangeStreamOperation)
        def binding = Mock(AsyncReadBinding)
        def operationContext = getOperationContext()
        operationContext.getTracingManager() >> TracingManager.NO_OP
        operationContext.getTimeoutContext().hasTimeoutMS() >> hasTimeoutMS

        def callback = Stub(SingleResultCallback)
        AsyncCursor<RawBsonDocument> wrapped = Mock(AsyncCursor)
        def cursor = new AsyncChangeStreamBatchCursor(changeStreamOperation,
                wrapped, binding, operationContext, null,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION)

        when:
        cursor.setBatchSize(10)

        then:
        1 * wrapped.setBatchSize(10)

        when:
        cursor.next(callback)

        then:
        1 * wrapped.next(_ as OperationContext, _) >> { it[1].onResult([], null) }

        when:
        cursor.close()

        then:
        1 * wrapped.close(_)
        1 * binding.release()

        when:
        cursor.close()

        then:
        0 * wrapped.close()
        0 * binding.release()

        where:
        hasTimeoutMS << [true, false]
    }

    def 'should not close the cursor in next if the cursor was closed before next completed'() {
        def changeStreamOpertation = Stub(ChangeStreamOperation)
        def binding = Mock(AsyncReadBinding)
        def operationContext = getOperationContext()
        operationContext.getTracingManager() >> TracingManager.NO_OP
        operationContext.getTimeoutContext().hasTimeoutMS() >> hasTimeoutMS

        def callback = Stub(SingleResultCallback)
        AsyncCursor<RawBsonDocument> wrapped = Mock(AsyncCursor)
        def cursor = new AsyncChangeStreamBatchCursor(changeStreamOpertation,
                wrapped, binding, operationContext, null,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION)

        when:
        cursor.next(callback)

        then:
        1 * wrapped.next(_ as OperationContext, _) >> {
            // Simulate the user calling close while wrapped.next() is in flight
            cursor.close()
            it[1].onResult([], null)
        }

        then:
        noExceptionThrown()

        then:
        cursor.isClosed()

        where:
        hasTimeoutMS << [true, false]
    }

    def 'should throw a MongoException when next/tryNext is called after the cursor is closed'() {
        def changeStreamOpertation = Stub(ChangeStreamOperation)
        def binding = Mock(AsyncReadBinding)
        def operationContext = getOperationContext()
        operationContext.getTracingManager() >> TracingManager.NO_OP
        operationContext.getTimeoutContext().hasTimeoutMS() >> hasTimeoutMS

        AsyncCursor<RawBsonDocument> wrapped = Mock(AsyncCursor)
        def cursor = new AsyncChangeStreamBatchCursor(changeStreamOpertation,
                wrapped, binding, operationContext, null,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION)

        given:
        cursor.close()

        when:
        nextBatch(cursor)

        then:
        def exception = thrown(MongoException)
        exception.getMessage() == 'next() called after the cursor was closed.'

        where:
        hasTimeoutMS << [true, false]
    }

    List<Document> nextBatch(AsyncChangeStreamBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get(1, SECONDS)
    }

    OperationContext getOperationContext() {
        def timeoutContext = Spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build())))
        Spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext, null))
    }
}
