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

import com.mongodb.ClusterFixture
import com.mongodb.MongoCommandException
import com.mongodb.MongoCredential
import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.internal.connection.netty.NettyStreamFactory
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.AuthenticationMechanism.PLAIN
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getClusterConnectionMode
import static com.mongodb.ClusterFixture.getConnectionString
import static com.mongodb.ClusterFixture.getCredential
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.internal.connection.CommandHelper.executeCommand
import static java.util.concurrent.TimeUnit.SECONDS

@IgnoreIf({ getCredential() == null || getCredential().getAuthenticationMechanism() != PLAIN })
class PlainAuthenticationSpecification extends Specification {

    def 'should not authorize when not authenticated'() {
        given:
        def connection = createConnection(async, null)

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection, OPERATION_CONTEXT)

        then:
        thrown(MongoCommandException)

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    def 'should authorize when successfully authenticated'() {
        given:
        def connection = createConnection(async, getMongoCredential())

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection, OPERATION_CONTEXT)

        then:
        true

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    def 'should throw MongoSecurityException when authentication fails'() {
        given:
        def connection = createConnection(async, createPlainCredential('wrongUserName', '$external', 'wrongPassword'.toCharArray()))

        when:
        openConnection(connection, async)
        executeCommand(getConnectionString().getDatabase(), new BsonDocument('count', new BsonString('test')),
                getClusterConnectionMode(), null, connection, OPERATION_CONTEXT)

        then:
        thrown(MongoSecurityException)

        cleanup:
        connection?.close()

        where:
        async << [true, false]
    }

    private static MongoCredential getMongoCredential() {
        getCredential()
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        new InternalStreamConnection(SINGLE,
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                new TestConnectionGenerationSupplier(),
                async ? new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(new DefaultInetAddressResolver(), SocketSettings.builder().build(), getSslSettings()),
                [], null, new InternalStreamConnectionInitializer(SINGLE, createAuthenticator(credential), null, [], null)
        )
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        credential == null ? null : new PlainAuthenticator(new MongoCredentialWithCache(credential), clusterConnectionMode, null)
    }

    private static void openConnection(final InternalConnection connection, final boolean async) {
        if (async) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>()
            connection.openAsync(OPERATION_CONTEXT, futureResultCallback)
            futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS)
        } else {
            connection.open(OPERATION_CONTEXT)
        }
    }
}


