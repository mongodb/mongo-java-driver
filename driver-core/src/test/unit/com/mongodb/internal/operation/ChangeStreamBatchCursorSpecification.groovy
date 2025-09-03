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
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.TimeoutSettings
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.connection.OperationContext
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.RawBsonDocument
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static java.util.Collections.emptyList

class ChangeStreamBatchCursorSpecification extends Specification {

    def 'should call the underlying CommandBatchCursor'() {
        given:
        def changeStreamOperation = Stub(ChangeStreamOperation)
        def binding = Stub(ReadBinding)
        def resumeToken = new BsonDocument('_id': new BsonInt32(1))
        def operationContext = getOperationContext()
        CoreCursor<RawBsonDocument> wrapped = Mock(CoreCursor)
        def cursor = new ChangeStreamBatchCursor(changeStreamOperation,
                wrapped, binding, operationContext, resumeToken,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION)


        when:
        cursor.setBatchSize(10)

        then:
        1 * wrapped.setBatchSize(10)

        when:
        cursor.tryNext()

        then:
        1 * wrapped.tryNext(_ as OperationContext)
        1 * wrapped.getPostBatchResumeToken()

        when:
        cursor.next()

        then:
        1 * wrapped.next(_ as OperationContext) >> emptyList()
        1 * wrapped.getPostBatchResumeToken()

        when:
        cursor.close()

        then:
        1 * wrapped.close(_ as OperationContext)

        when:
        cursor.close()

        then:
        0 * wrapped.close(_ as OperationContext)
    }

    OperationContext getOperationContext() {
        def timeoutContext = Spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build())))
        Spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext, null));
    }
}
