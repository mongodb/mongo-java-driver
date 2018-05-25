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
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.MongoSocketWriteException
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernResult
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.bulk.InsertRequest
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ServerId
import com.mongodb.event.CommandListener
import com.mongodb.event.ServerListener
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.session.SessionContext
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.FieldNameValidator
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_LISTENER
import static java.util.concurrent.TimeUnit.SECONDS

class DefaultServerSpecification extends Specification {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator()
    def serverId = new ServerId(new ClusterId(), new ServerAddress())

    def 'should get a connection'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Stub(ServerMonitor)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(Connection)
        def clusterTime = new ClusterClock()

        serverMonitorFactory.create(_, _) >> { serverMonitor }
        connectionPool.get() >> { internalConnection }
        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)

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
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Stub(ServerMonitor)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(AsyncConnection)
        def clusterTime = new ClusterClock()

        connectionPool.getAsync(_) >> {
            it[0].onResult(internalConnection, null)
        }
        serverMonitorFactory.create(_, _) >> { serverMonitor }

        def server = new DefaultServer(serverId, mode, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        receivedConnection
        !receivedThrowable
        1 * connectionFactory.createAsync(_, _, mode) >> connection

        where:
        mode << [SINGLE, MULTIPLE]
    }

    def 'invalidate should invoke server listeners'() {
        given:
        def serverListener = Mock(ServerListener)
        def clusterTime = new ClusterClock()
        def connectionFactory = Mock(ConnectionFactory)
        def server = new DefaultServer(serverId, SINGLE, new TestConnectionPool(), connectionFactory,
                new TestServerMonitorFactory(serverId), serverListener, null, clusterTime)

        when:
        server.invalidate()

        then:
        1 * serverListener.serverDescriptionChanged(_)

        cleanup:
        server?.close()
    }

    def 'failed open should invalidate the server'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get() >> { throw exceptionToThrow }
        serverMonitorFactory.create(_) >> { serverMonitor }

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)

        when:
        server.getConnection()

        then:
        def e = thrown(MongoException)
        e.is(exceptionToThrow)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()

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
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get() >> { throw exceptionToThrow }
        serverMonitorFactory.create(_) >> { serverMonitor }

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)

        when:
        server.getConnection()

        then:
        def e = thrown(MongoSecurityException)
        e.is(exceptionToThrow)
        1 * connectionPool.invalidate()
        0 * serverMonitor.connect()

        where:
        exceptionToThrow << [
                new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed'),
        ]
    }

    def 'failed open should invalidate the server asynchronously'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.getAsync(_) >> { it[0].onResult(null, exceptionToThrow) }
        serverMonitorFactory.create(_) >> { serverMonitor }
        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory,
                serverMonitorFactory, NO_OP_SERVER_LISTENER, null, clusterTime)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable.is(exceptionToThrow)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()


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
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.getAsync(_) >> { it[0].onResult(null, exceptionToThrow) }
        serverMonitorFactory.create(_) >> { serverMonitor }
        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory,
                serverMonitorFactory, NO_OP_SERVER_LISTENER, null, clusterTime)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection = null
        def receivedThrowable = null
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable.is(exceptionToThrow)
        1 * connectionPool.invalidate()
        0 * serverMonitor.connect()


        where:
        exceptionToThrow << [
                new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed'),
        ]
    }

    def 'should invalidate on MongoNotPrimaryException'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoNotPrimaryException(serverId.address)))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()))

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()

        when:
        def futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                futureResultCallback);
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()

        when:
        futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                futureResultCallback);
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()
    }

    def 'should invalidate on MongoNodeIsRecoveringException'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoNodeIsRecoveringException(new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()))

        then:
        thrown(MongoNodeIsRecoveringException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()
    }


    def 'should invalidate on MongoSocketException'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoSocketException('socket error', new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()))

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.connect()
    }

    def 'should not invalidate on MongoSocketReadTimeoutException'() {
        given:
        def clusterTime = new ClusterClock()
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_, _) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterTime)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestLegacyProtocol(new MongoSocketReadTimeoutException('socket timeout', new ServerAddress(),
                new IOException())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()))

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate()
        0 * serverMonitor.connect()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, new InsertRequest(new BsonDocument()),
                futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate()
        0 * serverMonitor.connect()
    }

    def 'should enable command listener'() {
        given:
        def clusterTime = new ClusterClock()
        def protocol = new TestLegacyProtocol()
        def commandListener = Stub(CommandListener)
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_, _) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, commandListener, clusterTime)
        def testConnection = (TestConnection) server.getConnection()

        testConnection.enqueueProtocol(protocol)

        when:
        if (async) {
            CountDownLatch latch = new CountDownLatch(1)
            testConnection.killCursorAsync(new MongoNamespace('test.test'), []) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()
        } else {
            testConnection.killCursor(new MongoNamespace('test.test'), [])
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
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(serverId, SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                NO_OP_SERVER_LISTENER, null, clusterClock)
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
                    ReadPreference.primary(), new BsonDocumentCodec(), sessionContext) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()
        } else {
            testConnection.command('admin', new BsonDocument('ping', new BsonInt32(1)), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), new BsonDocumentCodec(), sessionContext)
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
}
