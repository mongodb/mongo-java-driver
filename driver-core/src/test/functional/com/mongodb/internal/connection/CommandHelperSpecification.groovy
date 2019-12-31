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

package com.mongodb.internal.connection

import category.Async
import com.mongodb.MongoCommandException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.netty.NettyStreamFactory
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getCredentialWithCache
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.internal.connection.CommandHelper.executeCommand
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync

class CommandHelperSpecification extends Specification {
    InternalConnection connection

    def setup() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialWithCache(), null, null, [], null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open()
    }

    def cleanup() {
        connection?.close()
    }

    def 'should gossip cluster time'() {
        given:
        def connection = Mock(InternalStreamConnection) {
            getDescription() >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                    6, ServerType.REPLICA_SET_PRIMARY, 1000, 1000, 1000, [])
        }
        def clusterClock = new ClusterClock()
        clusterClock.advance(new BsonDocument('clusterTime', new BsonTimestamp(42L)))

        when:
        executeCommand('admin', new BsonDocument('ismaster', new BsonInt32(1)), clusterClock, connection)

        then:
        1 * connection.sendAndReceive(_, _, ) { it instanceof ClusterClockAdvancingSessionContext }
    }

    @Category(Async)
    def 'should execute command asynchronously'() {
        when:
        BsonDocument receivedDocument = null
        Throwable receivedException = null
        def latch1 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument('ismaster', new BsonInt32(1)), connection)
                { document, exception -> receivedDocument = document; receivedException = exception; latch1.countDown() }
        latch1.await()

        then:
        !receivedDocument.isEmpty()
        receivedDocument.containsKey('ok')
        !receivedException

        when:
        def latch2 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument('non-existent-command', new BsonInt32(1)), connection)
                { document, exception -> receivedDocument = document; receivedException = exception; latch2.countDown() }
        latch2.await()

        then:
        !receivedDocument
        receivedException instanceof MongoCommandException
    }

}
