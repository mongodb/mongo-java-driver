/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.connection

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.MongoNodeIsRecoveringException
import com.mongodb.MongoNotPrimaryException
import com.mongodb.MongoSecurityException
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernResult
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.bulk.InsertRequest
import com.mongodb.event.CommandListener
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class DefaultServerSpecification extends Specification {

    def 'should get a connection'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Stub(ServerMonitor)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(Connection)

        serverMonitorFactory.create(_) >> { serverMonitor }
        connectionPool.get() >> { internalConnection }
        def server = new DefaultServer(new ServerAddress(), mode, connectionPool, connectionFactory, serverMonitorFactory, null)

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

        connectionPool.getAsync(_) >> {
            it[0].onResult(internalConnection, null)
        }
        serverMonitorFactory.create(_) >> { serverMonitor }

        def server = new DefaultServer(new ServerAddress(), mode, connectionPool, connectionFactory, serverMonitorFactory, null)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection
        def receivedThrowable
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        receivedConnection
        !receivedThrowable
        1 * connectionFactory.createAsync(_, _, mode) >> connection

        where:
        mode << [SINGLE, MULTIPLE]
    }

    def 'invalidate should invoke change listeners'() {
        given:
        def connectionFactory = Mock(ConnectionFactory)
        def server = new DefaultServer(new ServerAddress(), SINGLE, new TestConnectionPool(),
                                       connectionFactory, new TestServerMonitorFactory(), null)
        def stateChanged = false;

        server.addChangeListener(new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
            }
        })

        when:
        server.invalidate();

        then:
        stateChanged

        cleanup:
        server?.close()
    }

    def 'failed open should invalidate the server'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get() >> { throw new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed') }
        serverMonitorFactory.create(_) >> { serverMonitor }

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory, null)

        when:
        server.getConnection()

        then:
        thrown(MongoSecurityException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'failed open should invalidate the server asychronously'() {
        given:
        def connectionFactory = Mock(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        serverMonitorFactory.create(_) >> { serverMonitor }

        def exceptionToThrow = new MongoSecurityException(createCredential('jeff', 'admin',
                                                                           '123'.toCharArray()),
                                                          'Auth failed')
        def server = new DefaultServer(new ServerAddress(), SINGLE, new TestConnectionPool(exceptionToThrow), connectionFactory,
                                       serverMonitorFactory, null)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection
        def receivedThrowable
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        !receivedConnection
        receivedThrowable.is(exceptionToThrow)
        1 * serverMonitor.invalidate()
    }

    def 'should invalidate on MongoNotPrimaryException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory, null)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestProtocol(new MongoNotPrimaryException(new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback);
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback);
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoNotPrimaryException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'should invalidate on MongoNodeIsRecoveringException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                                       null)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestProtocol(new MongoNodeIsRecoveringException(new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(MongoNodeIsRecoveringException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }


    def 'should invalidate on MongoSocketException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                                       null)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestProtocol(new MongoSocketException('socket error', new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'should not invalidate on MongoSocketReadTimeoutException'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                                       null)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new TestProtocol(new MongoSocketReadTimeoutException('socket timeout', new ServerAddress(),
                                                                                                new IOException())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate()
        0 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown(MongoSocketReadTimeoutException)
        0 * connectionPool.invalidate()
        0 * serverMonitor.invalidate()
    }

    def 'should enable command listener'() {
        given:
        def protocol = new TestProtocol()
        def commandListener = Stub(CommandListener)
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), SINGLE, connectionPool, connectionFactory, serverMonitorFactory,
                commandListener)
        def testConnection = (TestConnection) server.getConnection()

        testConnection.enqueueProtocol(protocol)

        when:
        if (async) {
            CountDownLatch latch = new CountDownLatch(1)
            testConnection.commandAsync('admin', new BsonDocument('ping', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                    new BsonDocumentCodec()) {
                BsonDocument result, Throwable t -> latch.countDown()
            }
            latch.await()

        } else {
            testConnection.command('admin', new BsonDocument('ping', new BsonInt32(1)), false, new NoOpFieldNameValidator(),
                    new BsonDocumentCodec())
        }

        then:
        protocol.commandListener == commandListener

        where:
        async << [false, true]
    }

    class TestProtocol implements Protocol {
        private MongoException mongoException
        private CommandListener commandListener

        TestProtocol() {
            this(null)
        }

        TestProtocol(MongoException mongoException) {
            this.mongoException = mongoException
        }

        @Override
        Object execute(final InternalConnection connection) {
            if (mongoException != null) {
                throw mongoException;
            }
        }

        @Override
        void executeAsync(final InternalConnection connection, final SingleResultCallback callback) {
            callback.onResult(null, mongoException);
        }

        @Override
        void setCommandListener(final CommandListener commandListener) {
            this.commandListener = commandListener
        }
    }
}
