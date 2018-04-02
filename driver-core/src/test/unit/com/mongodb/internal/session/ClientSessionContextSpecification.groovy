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

package com.mongodb.internal.session

import com.mongodb.ReadConcern
import com.mongodb.session.ClientSession
import com.mongodb.session.ServerSession
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import spock.lang.Specification

class ClientSessionContextSpecification extends Specification {

    class TestClientSessionContext extends ClientSessionContext {

        TestClientSessionContext(final ClientSession clientSession) {
            super(clientSession)
        }

        @Override
        boolean hasActiveTransaction() {
            false;
        }

        @Override
        ReadConcern getReadConcern() {
            ReadConcern.DEFAULT;
        }
    }

    def 'should have session'() {
        given:
        def clientSession = Mock(ClientSession)
        def context = new TestClientSessionContext(clientSession)

        expect:
        context.hasSession()
    }

    def 'should forward all methods to wrapped session'() {
        given:
        def expectedSessionId = new BsonDocument('id', new BsonInt32(1))
        def expectedClusterTime = new BsonDocument('x', BsonBoolean.TRUE)
        def expectedOperationTime = new BsonTimestamp(42, 1)
        def expectedTransactionNumber = 2

        def serverSession = Mock(ServerSession)
        def clientSession = Mock(ClientSession) {
            _ * getServerSession() >> {
                serverSession
            }
        }
        def context = new TestClientSessionContext(clientSession)

        when:
        def sessionId = context.getSessionId()

        then:
        sessionId == expectedSessionId
        1 * serverSession.getIdentifier() >> {
            expectedSessionId
        }

        when:
        context.isCausallyConsistent()

        then:
        1 * clientSession.isCausallyConsistent()

        when:
        context.advanceClusterTime(expectedClusterTime)

        then:
        1 * clientSession.advanceClusterTime(expectedClusterTime)

        when:
        context.getOperationTime()

        then:
        1 * clientSession.getOperationTime()

        when:
        context.advanceOperationTime(expectedOperationTime)

        then:
        1 * clientSession.advanceOperationTime(expectedOperationTime)

        when:
        context.advanceTransactionNumber()

        then:
        1 * serverSession.advanceTransactionNumber()

        when:
        def transactionNumber = context.getTransactionNumber()

        then:
        transactionNumber == expectedTransactionNumber
        1 * serverSession.getTransactionNumber() >> {
            expectedTransactionNumber
        }

        when:
        context.advanceStatementId(2)

        then:
        1 * serverSession.advanceStatementId(2)

        when:
        def clusterTime = context.getClusterTime()

        then:
        clusterTime == expectedClusterTime
        1 * clientSession.getClusterTime() >> {
            expectedClusterTime
        }
    }
}
