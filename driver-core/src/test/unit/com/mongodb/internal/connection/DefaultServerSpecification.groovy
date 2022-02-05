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
import com.mongodb.MongoNamespace
import com.mongodb.MongoNodeIsRecoveringException
import com.mongodb.MongoNotPrimaryException
import com.mongodb.MongoSecurityException
import com.mongodb.MongoServerUnavailableException
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.MongoSocketWriteException
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernResult
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.syncadapter.SupplyingCallback
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.event.CommandListener
import com.mongodb.event.ServerDescriptionChangedEvent
import com.mongodb.event.ServerListener
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.inject.SameObjectProvider
import com.mongodb.internal.operation.ServerVersionHelper
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.FieldNameValidator
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE

class DefaultServerSpecification extends Specification {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator()
    def serverId = new ServerId(new ClusterId(), new ServerAddress())

    def 'should get a connection'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(Connection)

        connectionPool.get() >> { internalConnection }
        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, Mock(ServerMonitor),
                Mock(SdamServerDescriptionManager), Mock(ServerListener), Mock(CommandListener), new ClusterClock(), false)

        when:
        def receivedConnection = server.getConnection()

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

        connectionPool.getAsync(_) >> {
            it[0].onResult(internalConnection, null)
        }

        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, Mock(ServerMonitor),
                Mock(SdamServerDescriptionManager), Mock(ServerListener), Mock(CommandListener), new ClusterClock(), false)

        when:
        def callback = new SupplyingCallback<AsyncConnection>()
        server.getConnectionAsync(callback)

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
        server.getConnection()

        then:
        def ex = thrown(MongoServerUnavailableException)
        ex.message == 'The server at 127.0.0.1:27017 is no longer available'

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
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
        server.invalidate()

        then:
        1 * serverListener.serverDescriptionChanged(_)

        cleanup:
        server?.close()
    }

    def 'invalidate should do nothing when server is closed'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get() >> { throw exceptionToThrow }

        def server = defaultServer(connectionPool, serverMonitor)
        server.close()

        when:
        server.invalidate()

        then:
        0 * connectionPool.invalidate(null)
        0 * serverMonitor.connect()
    }

    def 'failed open should invalidate the server'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get() >> { throw exceptionToThrow }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        server.getConnection()

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
        connectionPool.get() >> { throw exceptionToThrow }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        server.getConnection()

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
        connectionPool.getAsync(_) >> { it[0].onResult(null, exceptionToThrow) }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
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
        connectionPool.getAsync(_) >> { it[0].onResult(null, exceptionToThrow) }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
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

    def 'should invalidate on MongoNotPrimaryException'() {
        given:
        def internalConnection = Mock(InternalConnection) {
            getGeneration() >> 0
            getDescription() >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 6,
                    ServerType.STANDALONE, 1000, 16777216, 48000000, [])
        }
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get() >> { internalConnection }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoNotPrimaryException(new BsonDocument(), serverId.address)))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE)

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate { it instanceof MongoNotPrimaryException }
        1 * serverMonitor.connect()

        when:
        def futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE, futureResultCallback);
        futureResultCallback.get()

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate { it instanceof MongoNotPrimaryException }
        1 * serverMonitor.connect()
    }

    def 'should invalidate on MongoNodeIsRecoveringException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get() >> { internalConnection() }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoNodeIsRecoveringException(new BsonDocument(), new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE)

        then:
        thrown(MongoNodeIsRecoveringException)
        1 * connectionPool.invalidate { it instanceof MongoNodeIsRecoveringException }
        1 * serverMonitor.connect()
    }


    def 'should invalidate on MongoSocketException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get() >> { internalConnection() }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoSocketException('socket error', new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE)

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate { it instanceof MongoSocketException }
        1 * serverMonitor.cancelCurrentCheck()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE, futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate { it instanceof MongoSocketException }
        1 * serverMonitor.cancelCurrentCheck()
    }

    def 'should not invalidate on MongoSocketReadTimeoutException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        connectionPool.get() >> { internalConnection() }
        def serverMonitor = Mock(ServerMonitor)
        def server = defaultServer(connectionPool, serverMonitor)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoSocketReadTimeoutException('socket timeout', new ServerAddress(),
                new IOException())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE)

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate { it instanceof MongoSocketReadTimeoutException }
        0 * serverMonitor.connect()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                IgnorableRequestContext.INSTANCE, futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate { it instanceof MongoSocketReadTimeoutException }
        0 * serverMonitor.connect()
    }

    def 'should enable command listener'() {
        given:
        def protocol = new TestLegacyProtocol()
        def commandListener = Mock(CommandListener)
        def server = defaultServer(Mock(ConnectionPool), Mock(ServerMonitor), Mock(ServerListener), Mock(SdamServerDescriptionManager),
                commandListener)
        def testConnection = (TestConnection) server.getConnection()

        testConnection.enqueueProtocol(protocol)

        when:
        if (async) {
            CountDownLatch latch = new CountDownLatch(1)
            testConnection.killCursorAsync(new MongoNamespace('test.test'), [], IgnorableRequestContext.INSTANCE) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()
        } else {
            testConnection.killCursor(new MongoNamespace('test.test'), [], IgnorableRequestContext.INSTANCE)
        }

        then:
        protocol.commandListener == commandListener

        where:
        async << [false, true]
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

        when:
        if (async) {
            CountDownLatch latch = new CountDownLatch(1)
            testConnection.commandAsync('admin', new BsonDocument('ping', new BsonInt32(1)), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), new BsonDocumentCodec(), sessionContext, getServerApi(), null) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()
        } else {
            testConnection.command('admin', new BsonDocument('ping', new BsonInt32(1)), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), new BsonDocumentCodec(), sessionContext, getServerApi(), IgnorableRequestContext.INSTANCE)
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

    private InternalConnection internalConnection() {
        Mock(InternalConnection) {
            getGeneration() >> 0
            getDescription() >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                    ServerVersionHelper.THREE_DOT_SIX_WIRE_VERSION, ServerType.STANDALONE, 1000, 0xff_ff_ff, 48_000_000, [])
        }
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

    class TestLegacyProtocol implements LegacyProtocol {
        private MongoException mongoException
        private CommandListener commandListener

        TestLegacyProtocol() {
            this(null)
        }

        TestLegacyProtocol(MongoException mongoException) {
            this.mongoException = mongoException
        }

        @Override
        Object execute(final InternalConnection connection) {
            if (mongoException != null) {
                throw mongoException
            }
            null
        }

        @Override
        void executeAsync(final InternalConnection connection, final SingleResultCallback callback) {
            callback.onResult(null, mongoException)
        }

        @Override
        void setCommandListener(final CommandListener commandListener) {
            this.commandListener = commandListener
        }
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
        TestCommandProtocol sessionContext(final SessionContext sessionContext) {
            contextClusterTime = sessionContext.clusterTime
            sessionContext.advanceClusterTime(responseDocument.getDocument('$clusterTime'))
            sessionContext.advanceOperationTime(responseDocument.getTimestamp('operationTime'))
            this
        }
    }

    private Cluster mockCluster() {
        new BaseCluster(new ClusterId(), ClusterSettings.builder().build(), Mock(ClusterableServerFactory)) {
            @Override
            protected void connect() {
            }

            @Override
            ClusterableServer getServer(final ServerAddress serverAddress) {
                throw new UnsupportedOperationException()
            }

            @Override
            void onChange(final ServerDescriptionChangedEvent event) {
            }
        }
    }
}
