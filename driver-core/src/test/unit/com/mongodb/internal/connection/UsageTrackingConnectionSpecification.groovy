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

import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ReadPreference.primary
import static com.mongodb.connection.ClusterConnectionMode.SINGLE

class UsageTrackingConnectionSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress())

    def 'generation returns wrapped value'() {
        when:
        def connection = createConnection()

        then:
        connection.generation == 0
    }

    def 'openAt should be set on open'() {
        when:
        def connection = createConnection()

        then:
        connection.openedAt == Long.MAX_VALUE

        when:
        connection.open(OPERATION_CONTEXT)

        then:
        connection.openedAt <= System.currentTimeMillis()
    }


    def 'openAt should be set on open asynchronously'() {
        when:
        def futureResultCallback = new FutureResultCallback<Void>()
        def connection = createConnection()

        then:
        connection.openedAt == Long.MAX_VALUE

        when:
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.openedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on open'() {
        when:
        def connection = createConnection()

        then:
        connection.lastUsedAt == Long.MAX_VALUE

        when:
        connection.open(OPERATION_CONTEXT)

        then:
        connection.lastUsedAt <= System.currentTimeMillis()
    }


    def 'lastUsedAt should be set on open asynchronously'() {
        when:
        def futureResultCallback = new FutureResultCallback<Void>()
        def connection = createConnection()

        then:
        connection.lastUsedAt == Long.MAX_VALUE

        when:
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on sendMessage'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt

        when:
        connection.sendMessage([], 1, OPERATION_CONTEXT)

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }


    def 'lastUsedAt should be set on sendMessage asynchronously'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        connection.sendMessageAsync([], 1, OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on receiveMessage'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt
        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on receiveMessage asynchronously'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        connection.receiveMessageAsync(1, OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on sendAndReceive'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt

        when:
        connection.sendAndReceive(new CommandMessage('test',
                new BsonDocument('ping', new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings.builder().build(), SINGLE, null), new BsonDocumentCodec(),  OPERATION_CONTEXT)

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    def 'lastUsedAt should be set on sendAndReceive asynchronously'() {
        given:
        def connection = createConnection()
        connection.open(OPERATION_CONTEXT)
        def openedLastUsedAt = connection.lastUsedAt
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        connection.sendAndReceiveAsync(new CommandMessage('test',
                new BsonDocument('ping', new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings.builder().build(), SINGLE, null),
                new BsonDocumentCodec(), OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.lastUsedAt >= openedLastUsedAt
        connection.lastUsedAt <= System.currentTimeMillis()
    }

    private static UsageTrackingInternalConnection createConnection() {
        new UsageTrackingInternalConnection(new TestInternalConnectionFactory().create(SERVER_ID),
                new DefaultConnectionPool.ServiceStateManager())
    }
}
