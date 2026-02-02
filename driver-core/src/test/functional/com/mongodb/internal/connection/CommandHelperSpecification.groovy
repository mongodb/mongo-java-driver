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

import com.mongodb.LoggerSettings
import com.mongodb.MongoCommandException
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.internal.connection.netty.NettyStreamFactory
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Ignore
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.CLIENT_METADATA
import static com.mongodb.ClusterFixture.LEGACY_HELLO
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getClusterConnectionMode
import static com.mongodb.ClusterFixture.getCredentialWithCache
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync

class CommandHelperSpecification extends Specification {
    InternalConnection connection

    def setup() {
        InternalStreamConnection.setRecordEverything(true)
        connection = new InternalStreamConnectionFactory(ClusterConnectionMode.SINGLE,
                new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialWithCache(), CLIENT_METADATA, [], LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open(OPERATION_CONTEXT)
    }

    def cleanup() {
        InternalStreamConnection.setRecordEverything(false)
        connection?.close()
    }

    @Ignore("JAVA-5982")
    def 'should execute command asynchronously'() {
        when:
        BsonDocument receivedDocument = null
        Throwable receivedException = null
        def latch1 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument(LEGACY_HELLO, new BsonInt32(1)), getClusterConnectionMode(),
                getServerApi(), connection, OPERATION_CONTEXT)
                { document, exception -> receivedDocument = document; receivedException = exception; latch1.countDown() }
        latch1.await()

        then:
        !receivedException
        !receivedDocument.isEmpty()
        receivedDocument.containsKey('ok')

        when:
        def latch2 = new CountDownLatch(1)
        executeCommandAsync('admin', new BsonDocument('non-existent-command', new BsonInt32(1)), getClusterConnectionMode(),
                getServerApi(), connection, OPERATION_CONTEXT)
                { document, exception -> receivedDocument = document; receivedException = exception; latch2.countDown() }
        latch2.await()

        then:
        !receivedDocument
        receivedException instanceof MongoCommandException
    }

}
