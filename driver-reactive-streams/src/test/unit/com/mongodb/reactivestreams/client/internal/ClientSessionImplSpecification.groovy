/*
 * Copyright 2018 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal

import com.mongodb.ClientSessionOptions
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.TransactionOptions
import com.mongodb.async.client.ClientSession
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.session.ServerSession
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.BsonTimestamp
import org.reactivestreams.Subscriber
import spock.lang.Specification

class ClientSessionImplSpecification extends Specification {
    def 'should forward methods to wrapped'() {
        given:
        def originator = Stub(MongoClient)
        def wrapped = Mock(ClientSession)
        def session = new ClientSessionImpl(wrapped, originator)
        def expectedTransactionOptions = TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build()
        def expectedSessionOptions = ClientSessionOptions.builder().causallyConsistent(false).build()
        def expectedClusterTime = new BsonDocument('x', BsonBoolean.TRUE)
        def expectedOperationTime = new BsonTimestamp(42, 1)
        def expectedServerSession = Stub(ServerSession)
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(1) }
        }
        def token = new BsonDocument('id', new BsonObjectId())
        def serverAddress = new ServerAddress('host')

        expect:
        session.getOriginator() == originator
        session.getWrapped() == wrapped

        when:
        def causallyConsistent = session.isCausallyConsistent()

        then:
        causallyConsistent
        1 * wrapped.isCausallyConsistent() >> true

        when:
        def transactionOptions = session.getTransactionOptions()

        then:
        transactionOptions == expectedTransactionOptions
        1 * wrapped.getTransactionOptions() >> expectedTransactionOptions

        when:
        def hasActiveTransaction = session.hasActiveTransaction()

        then:
        hasActiveTransaction
        1 * wrapped.hasActiveTransaction() >> true

        when:
        def sessionOptions = session.getOptions()

        then:
        sessionOptions == expectedSessionOptions
        1 * wrapped.getOptions() >> expectedSessionOptions

        when:
        def clusterTime = session.getClusterTime()

        then:
        clusterTime == expectedClusterTime
        1 * wrapped.getClusterTime() >> expectedClusterTime

        when:
        def operationTime = session.getOperationTime()

        then:
        operationTime == expectedOperationTime
        1 * wrapped.getOperationTime() >> expectedOperationTime

        when:
        def serverSession = session.getServerSession()

        then:
        serverSession == expectedServerSession
        1 * wrapped.getServerSession() >> expectedServerSession

        when:
        session.advanceClusterTime(expectedClusterTime)

        then:
        1 * wrapped.advanceClusterTime(expectedClusterTime)

        when:
        session.advanceOperationTime(expectedOperationTime)

        then:
        1 * wrapped.advanceOperationTime(expectedOperationTime)

        when:
        session.notifyMessageSent()

        then:
        1 * wrapped.notifyMessageSent()

        when:
        session.startTransaction()

        then:
        1 * wrapped.startTransaction()

        when:
        session.startTransaction(expectedTransactionOptions)

        then:
        1 * wrapped.startTransaction(expectedTransactionOptions)

        when:
        session.setRecoveryToken(token)

        then:
        1 * wrapped.setRecoveryToken(token)

        when:
        def returnedToken = session.getRecoveryToken()

        then:
        1 * wrapped.getRecoveryToken() >> token
        returnedToken == token

        when:
        session.setPinnedServerAddress(serverAddress)

        then:
        1 * wrapped.setPinnedServerAddress(serverAddress)

        when:
        def returnedServerAddress = session.getPinnedServerAddress()

        then:
        1 * wrapped.getPinnedServerAddress() >> serverAddress
        returnedServerAddress == serverAddress

        when:
        session.commitTransaction().subscribe(subscriber)

        then:
        1 * wrapped.commitTransaction(_)

        when:
        session.abortTransaction().subscribe(subscriber)

        then:
        1 * wrapped.abortTransaction(_)

        when:
        session.close()

        then:
        1 * wrapped.close()
    }
}
