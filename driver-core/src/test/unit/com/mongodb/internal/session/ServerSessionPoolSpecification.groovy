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

import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerSettings
import com.mongodb.internal.connection.Cluster
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.Server
import com.mongodb.internal.connection.ServerTuple
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonArray
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.UNKNOWN
import static java.util.concurrent.TimeUnit.MINUTES

class ServerSessionPoolSpecification extends Specification {

    def connectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            [
                    ServerDescription.builder().ok(true)
                            .state(CONNECTED)
                            .address(new ServerAddress())
                            .type(REPLICA_SET_PRIMARY)
                            .logicalSessionTimeoutMinutes(30)
                            .build()
            ], ClusterSettings.builder().hosts([new ServerAddress()]).build(), ServerSettings.builder().build())

    def unconnectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            [
                    ServerDescription.builder().ok(true)
                            .state(CONNECTING)
                            .address(new ServerAddress())
                            .type(UNKNOWN)
                            .logicalSessionTimeoutMinutes(null)
                            .build()
            ], ClusterSettings.builder().hosts([new ServerAddress()]).build(), ServerSettings.builder().build())

    def 'should get session'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi())

        when:
        def session = pool.get()

        then:
        session != null
    }

    def 'should throw IllegalStateException if pool is closed'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi())
        pool.close()

        when:
        pool.get()

        then:
        thrown(IllegalStateException)
    }

    def 'should pool session'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi())
        def session = pool.get()

        when:
        pool.release(session)
        def pooledSession = pool.get()

        then:
        session == pooledSession
    }

    def 'should prune sessions when getting'() {
        given:
        def cluster = Mock(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >>> [0, MINUTES.toMillis(29) + 1,
            ]
        }
        def pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock)
        def sessionOne = pool.get()

        when:
        pool.release(sessionOne)

        then:
        !sessionOne.closed

        when:
        def sessionTwo = pool.get()

        then:
        sessionTwo != sessionOne
        sessionOne.closed
        0 * cluster.selectServer(_)
    }

    def 'should not prune session when timeout is null'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> unconnectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >>> [0, 0, 0]
        }
        def pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock)
        def session = pool.get()

        when:
        pool.release(session)
        def newSession = pool.get()

        then:
        session == newSession
    }

    def 'should initialize session'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >> 42
        }
        def pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock)

        when:
        def session = pool.get() as ServerSessionPool.ServerSessionImpl

        then:
        session.lastUsedAtMillis == 42
        session.transactionNumber == 0
        def uuid = session.identifier.getBinary('id')
        uuid != null
        uuid.type == BsonBinarySubType.UUID_STANDARD.value
        uuid.data.length == 16
    }

    def 'should advance transaction number'() {
        given:
        def cluster = Stub(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >> 42
        }
        def pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock)

        when:
        def session = pool.get() as ServerSessionPool.ServerSessionImpl

        then:
        session.transactionNumber == 0
        session.advanceTransactionNumber() == 1
        session.transactionNumber == 1
    }

    def 'should end pooled sessions when pool is closed'() {
        given:
        def connection = Mock(Connection)
        def server = Stub(Server) {
            getConnection(_) >> connection
        }
        def cluster = Mock(Cluster) {
            getCurrentDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi())
        def sessions = []
        10.times { sessions.add(pool.get()) }

        for (def cur : sessions) {
            pool.release(cur)
        }

        when:
        pool.close()

        then:
        1 * cluster.selectServer(_, _)  >> new ServerTuple(server, connectedDescription.serverDescriptions[0])
        1 * connection.command('admin',
                new BsonDocument('endSessions', new BsonArray(sessions*.getIdentifier())),
                { it instanceof NoOpFieldNameValidator }, primaryPreferred(),
                { it instanceof BsonDocumentCodec }, _) >> new BsonDocument()
        1 * connection.release()
    }
}
