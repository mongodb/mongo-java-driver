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

import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncWriteBinding
import com.mongodb.internal.binding.WriteBinding
import com.mongodb.internal.connection.OperationContext
import com.mongodb.internal.session.SessionContext

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT

class CommitTransactionOperationUnitSpecification extends OperationUnitSpecification {
    def 'should add UnknownTransactionCommitResult error label to MongoTimeoutException'() {
        given:
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
            hasActiveTransaction() >> true
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource(_) >> { throw new MongoTimeoutException('Time out!') }
        }
        def operation = new CommitTransactionOperation(WriteConcern.ACKNOWLEDGED)

        when:
        operation.execute(writeBinding, OPERATION_CONTEXT.withSessionContext(sessionContext))

        then:
        def e = thrown(MongoTimeoutException)
        e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
    }

    def 'should add UnknownTransactionCommitResult error label to MongoTimeoutException asynchronously'() {
        given:
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
            hasActiveTransaction() >> true
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_ as OperationContext, _ as SingleResultCallback) >> {
                it[1].onResult(null, new MongoTimeoutException('Time out!'))
            }
        }
        def operation = new CommitTransactionOperation(WriteConcern.ACKNOWLEDGED)
        def callback = new FutureResultCallback()

        when:
        operation.executeAsync(writeBinding, OPERATION_CONTEXT.withSessionContext(sessionContext), callback)
        callback.get()

        then:
        def e = thrown(MongoTimeoutException)
        e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
    }
}
