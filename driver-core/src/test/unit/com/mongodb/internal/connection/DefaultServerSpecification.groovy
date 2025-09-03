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


import com.mongodb.MongoException
import com.mongodb.MongoNodeIsRecoveringException
import com.mongodb.MongoNotPrimaryException
import com.mongodb.MongoSecurityException
import com.mongodb.MongoServerUnavailableException
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.MongoSocketWriteException
import com.mongodb.MongoStalePrimaryException
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.client.syncadapter.SupplyingCallback
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.event.CommandListener
import com.mongodb.event.ServerDescriptionChangedEvent
import com.mongodb.event.ServerListener
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.inject.SameObjectProvider
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.time.Timeout
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.CLIENT_METADATA
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE

class DefaultServerSpecification extends Specification {
    def serverId = new ServerId(new ClusterId(), new ServerAddress())

    def 'should get a connection'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(Connection)

        connectionPool.get(_) >> { internalConnection }
        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, Mock(ServerMonitor),
                Mock(SdamServerDescriptionManager), Mock(ServerListener), Mock(CommandListener), new ClusterClock(), false)

        when:
        def receivedConnection = server.getConnection(OPERATION_CONTEXT)

        then:
        receivedConnection
        1 * connectionFactory.create(internalConnection, _, mode) >> connection

        where:
        mode << [SINGLE, MULTIPLE]
    }

    def 'should get a connection asynchronously'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(AsyncConnection)

        connectionPool.getAsync(_, _) >> {
            it.last().onResult(internalConnection, null)
        }

        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, Mock(ServerMonitor),
                Mock(SdamServerDescriptionManager), Mock(ServerListener), Mock(CommandListener), new ClusterClock(), false)

        when:
        def callback = new SupplyingCallback<AsyncConnection>()
        server.getConnectionAsync(OPERATION_CONTEXT, callback)

        then:
        callback.get() == connection
        1 * connectionFactory.createAsync(_, _, mode) >> connection

        where:
        mode << [SINGLE, MULTIPLE]
    }

    def 'should throw MongoServerUnavailableException getting a connection when the server is closed'() {
        given:
        def server = new DefaultServer(serverId, SINGLE, Stub(ConnectionPool), Stub(ConnectionFactory), Mock(ServerMonitor),
                Stub(SdamServerDescriptionManager), Stub(ServerListener), Stub(CommandListener), new ClusterClock(), false)
        server.close()

        when:
        server.getConnection(OPERATION_CONTEXT)

        then:
        def ex = thrown(MongoServerUnavailableException)
        ex.message == 'The server at 127.0.0.1:27017 is no longer available'

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync(OPERATION_CONTEXT) {
            result, throwable ->
                receivedConnection = result; receivedThrowable = throwable; latch.countDown()
        }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable instanceof MongoServerUnavailableException
        receivedThrowable.message == 'The server at 127.0.0.1:27017 is no longer available'
    }

    def 'invalidate should invoke server listeners'() {
        given:
        def serverListener = Mock(ServerListener)
        def connectionPool = Mock(ConnectionPool)
        def sdamProvider = SameObjectProvider.<SdamServerDescriptionManager>uninitialized()
        def serverMonitor = new TestServerMonitor(sdamProvider)
        sdamProvider.initialize(new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener, serverMonitor,
                connectionPool, ClusterConnectionMode.MULTIPLE))
        def server = defaultServer(Mock(ConnectionPool), serverMonitor, serverListener, sdamProvider.get(), Mock(CommandListener))
        serverMonitor.updateServerDescription(ServerDescription.builder()
                .address(serverId.getAddress())
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .build())

        when:
        server.invalidate(exceptionToThrow)

        then:
        1 * serverListener.serverDescriptionChanged(_)

        cleanup:
        server?.close()

        where:
        exceptionToThrow << [
                new MongoStalePrimaryException(""),
                new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()),
                new MongoNodeIsRecoveringException(new BsonDocument(), new ServerAddress()),
                new MongoSocketException("", new ServerAddress()),
                new MongoWriteConcernWithResponseException(new MongoException(""), new Object())
        ]
    }

    def 'invalidate should not invoke server listeners'() {
        given:
        def serverListener = Mock(ServerListener)
        def connectionPool = Mock(ConnectionPool)
        def sdamProvider = SameObjectProvider.<SdamServerDescriptionManager> uninitialized()
        def serverMonitor = new TestServerMonitor(sdamProvider)
        sdamProvider.initialize(new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener, serverMonitor,
                connectionPool, ClusterConnectionMode.MULTIPLE))
        def server = defaultServer(Mock(ConnectionPool), serverMonitor, serverListener, sdamProvider.get(), Mock(CommandListener))
        serverMonitor.updateServerDescription(ServerDescription.builder()
                .address(serverId.getAddress())
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .build())

        when:
        server.invalidate(exceptionToThrow)

        then:
        0 * serverListener.serverDescriptionChanged(_)

        cleanup:
        server?.close()

        where:
        exceptionToThrow << [
                new MongoException(""),
                new MongoSecurityException(createCredential("jeff", "admin", "123".toCharArray()), "Auth failed"),
        ]
    }

    def 'invalidate should do nothing when server is closed for any exception'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get(OPERATION_CONTEXT) >> { throw exceptionToThrow }

        def server = defaultServer(connectionPool, serverMonitor)
        server.close()

        when:
        server.invalidate(exceptionToThrow)

        then:
        0 * connectionPool.invalidate(null)
        0 * serverMonitor.connect()

        where:
        exceptionToThrow << [
                new MongoStalePrimaryException(""),
                new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()),
                new MongoNodeIsRecoveringException(new BsonDocument(), new ServerAddress()),
                new MongoSocketException("", new ServerAddress()),
                new MongoWriteConcernWithResponseException(new MongoException(""), new Object()),
                new MongoException(""),
                new MongoSecurityException(createCredential("jeff", "admin", "123".toCharArray()), "Auth failed"),
        ]
    }

    def 'failed open should invalidate the server'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get(_) >> { throw exceptionToThrow }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        server.getConnection(OPERATION_CONTEXT)

        then:
        def e = thrown(MongoException)
        e.is(exceptionToThrow)
        1 * connectionPool.invalidate(exceptionToThrow)
        1 * serverMonitor.cancelCurrentCheck()

        where:
        exceptionToThrow << [
                new MongoSocketOpenException('open failed', new ServerAddress(), new IOException()),
                new MongoSocketWriteException('Write failed', new ServerAddress(), new IOException()),
                new MongoSocketReadException('Read failed', new ServerAddress(), new IOException()),
                new MongoSocketReadTimeoutException('Read timed out', new ServerAddress(), new IOException()),
        ]
    }

    def 'failed authentication should invalidate the connection pool'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get(_) >> { throw exceptionToThrow }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        server.getConnection(OPERATION_CONTEXT)

        then:
        def e = thrown(MongoSecurityException)
        e.is(exceptionToThrow)
        1 * connectionPool.invalidate(exceptionToThrow)
        0 * serverMonitor.connect()

        where:
        exceptionToThrow << [
                new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed'),
        ]
    }

    def 'failed open should invalidate the server asynchronously'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.getAsync(_, _) >> { it.last().onResult(null, exceptionToThrow) }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync(OPERATION_CONTEXT) {
            result, throwable ->
                receivedConnection = result; receivedThrowable = throwable; latch.countDown()
        }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable.is(exceptionToThrow)
        1 * connectionPool.invalidate(exceptionToThrow)
        1 * serverMonitor.cancelCurrentCheck()


        where:
        exceptionToThrow << [
                new MongoSocketOpenException('open failed', new ServerAddress(), new IOException()),
                new MongoSocketWriteException('Write failed', new ServerAddress(), new IOException()),
                new MongoSocketReadException('Read failed', new ServerAddress(), new IOException()),
                new MongoSocketReadTimeoutException('Read timed out', new ServerAddress(), new IOException()),
        ]
    }

    def 'failed auth should invalidate the connection pool asynchronously'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.getAsync(_, _) >> { it.last().onResult(null, exceptionToThrow) }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync(OPERATION_CONTEXT) {
            result, throwable ->
                receivedConnection = result; receivedThrowable = throwable; latch.countDown()
        }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable.is(exceptionToThrow)
        1 * connectionPool.invalidate(exceptionToThrow)
        0 * serverMonitor.connect()


        where:
        exceptionToThrow << [
                new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed'),
        ]
    }

    def 'should propagate cluster time'() {
        given:
        def clusterClock = new ClusterClock()
        clusterClock.advance(clusterClockClusterTime)
        def server = new DefaultServer(serverId, SINGLE, Mock(ConnectionPool), new TestConnectionFactory(), Mock(ServerMonitor),
                Mock(SdamServerDescriptionManager), Mock(ServerListener), Mock(CommandListener), clusterClock, false)
        def testConnection = (TestConnection) server.getConnection()
        def sessionContext = new TestSessionContext(initialClusterTime)
        def response = BsonDocument.parse(
                '''{
                           ok : 1,
                           operationTime : { $timestamp : { "t" : 50, "i" : 20 } },
                           $clusterTime :  { clusterTime : { $timestamp : { "t" : 42, "i" : 21 } } }
                          }
                          ''')
        def protocol = new TestCommandProtocol(response)
        testConnection.enqueueProtocol(protocol)
        def operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext)

        when:
        if (async) {
            CountDownLatch latch = new CountDownLatch(1)
            testConnection.commandAsync('admin', new BsonDocument('ping', new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE,
                    ReadPreference.primary(), new BsonDocumentCodec(), operationContext) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()
        } else {
            testConnection.command('admin', new BsonDocument('ping', new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE,
                    ReadPreference.primary(), new BsonDocumentCodec(), operationContext)
        }

        then:
        clusterClock.getCurrent() == response.getDocument('$clusterTime')
        protocol.contextClusterTime == (initialClusterTime.getTimestamp('clusterTime')
                .compareTo(clusterClockClusterTime.getTimestamp('clusterTime')) > 0 ? initialClusterTime : clusterClockClusterTime)
        sessionContext.clusterTime == response.getDocument('$clusterTime')
        sessionContext.operationTime == response.getTimestamp('operationTime')

        where:
        [async, initialClusterTime, clusterClockClusterTime] << [
                [false, true],
                [
                        BsonDocument.parse('{clusterTime : {$timestamp : {"t" : 21, "i" : 11 } } }'),
                        BsonDocument.parse('{clusterTime : {$timestamp : {"t" : 42, "i" : 11 } } }')
                ],
                [
                        BsonDocument.parse('{clusterTime : {$timestamp : {"t" : 21, "i" : 11 } } }'),
                        BsonDocument.parse('{clusterTime : {$timestamp : {"t" : 42, "i" : 11 } } }')
                ]
        ].combinations()
    }

    private DefaultServer defaultServer(final ConnectionPool connectionPool, final ServerMonitor serverMonitor) {
        def serverListener = Mock(ServerListener)
        defaultServer(connectionPool, serverMonitor, serverListener,
                new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener, serverMonitor, connectionPool,
                        ClusterConnectionMode.MULTIPLE),
                Mock(CommandListener))
    }

    private DefaultServer defaultServer(final ConnectionPool connectionPool, final ServerMonitor serverMonitor,
                                        final ServerListener serverListener,
                                        final SdamServerDescriptionManager sdam, final CommandListener commandListener) {
        serverMonitor.start()
        new DefaultServer(serverId, SINGLE, connectionPool, new TestConnectionFactory(), serverMonitor,
                sdam, serverListener, commandListener, new ClusterClock(), false)
    }

    class TestCommandProtocol implements CommandProtocol<BsonDocument> {
        private final BsonDocument commandResult
        private final BsonDocument responseDocument
        private BsonDocument contextClusterTime

        TestCommandProtocol(BsonDocument result) {
            this.commandResult = result
            this.responseDocument = result
        }

        @Override
        BsonDocument execute(final InternalConnection connection) {
            commandResult
        }

        @Override
        void executeAsync(final InternalConnection connection, final SingleResultCallback callback) {
            callback.onResult(commandResult, null)
        }

        @Override
        TestCommandProtocol withSessionContext(final SessionContext sessionContext) {
            contextClusterTime = sessionContext.clusterTime
            sessionContext.advanceClusterTime(responseDocument.getDocument('$clusterTime'))
            sessionContext.advanceOperationTime(responseDocument.getTimestamp('operationTime'))
            this
        }
    }

    private Cluster mockCluster() {
        new BaseCluster(new ClusterId(), ClusterSettings.builder().build(), Mock(ClusterableServerFactory), CLIENT_METADATA) {
            @Override
            protected void connect() {
            }

            @Override
            Cluster.ServersSnapshot getServersSnapshot(final Timeout serverSelectionTimeout, final TimeoutContext timeoutContext) {
                Cluster.ServersSnapshot result = {
                    serverAddress -> throw new UnsupportedOperationException()
                }
                result
            }

            @Override
            void onChange(final ServerDescriptionChangedEvent event) {
            }
        }
    }
}
