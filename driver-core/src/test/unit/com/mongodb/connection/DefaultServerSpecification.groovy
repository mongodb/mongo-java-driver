/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import com.mongodb.MongoSecurityException
import com.mongodb.MongoSocketException
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernException
import com.mongodb.WriteConcernResult
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.bulk.InsertRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static java.util.concurrent.TimeUnit.SECONDS
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList

class DefaultServerSpecification extends Specification {

    def 'should get a connection'() {
        given:
        def connectionPool = Stub(ConnectionPool)
        def connectionFactory = Stub(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Stub(ServerMonitor)
        def internalConnection = Stub(InternalConnection)
        def connection = Stub(Connection)

        serverMonitorFactory.create(_) >> { serverMonitor }
        connectionPool.get() >> { internalConnection }
        connectionFactory.create(_, _) >> connection
        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)

        expect:
        server.getConnection()
    }

    def 'should get a connection asynchronously'() {
        given:
        def connectionFactory = Stub(ConnectionFactory)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Stub(ServerMonitor)
        def connection = Stub(Connection)

        serverMonitorFactory.create(_) >> { serverMonitor }
        connectionFactory.create(_, _) >> connection

        def server = new DefaultServer(new ServerAddress(), new TestConnectionPool(), connectionFactory, serverMonitorFactory)

        when:
        def latch = new CountDownLatch(1)
        def receivedConnection
        def receivedThrowable
        server.getConnectionAsync { result, throwable -> receivedConnection = result; receivedThrowable = throwable; latch.countDown() }
        latch.await()

        then:
        receivedConnection
        !receivedThrowable
    }

    def 'invalidate should invoke change listeners'() {
        given:
        def connectionFactory = Mock(ConnectionFactory)
        def server = new DefaultServer(new ServerAddress(), new TestConnectionPool(),
                                       connectionFactory, new TestServerMonitorFactory())
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

        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)

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
        def server = new DefaultServer(new ServerAddress(), new TestConnectionPool(exceptionToThrow), connectionFactory,
                                       serverMonitorFactory)

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

    def 'should invalidate on not master errors'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new ThrowingProtocol(
                new WriteConcernException(new BsonDocument('ok', new BsonInt32(1))
                                                  .append('err', new BsonString('server is not master')),
                                          new ServerAddress(), WriteConcernResult.acknowledged(0, false, null))))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback);
        futureResultCallback.get(10, SECONDS)

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback);
        futureResultCallback.get(10, SECONDS)

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'should invalidate on node is recovering errors'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new ThrowingProtocol(
                new WriteConcernException(new BsonDocument('ok', new BsonInt32(1))
                                                  .append('err', new BsonString('the node is recovering')),
                                          new ServerAddress(), WriteConcernResult.acknowledged(0, false, null))))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'should invalidate on 10107 error code'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new ThrowingProtocol(
                new WriteConcernException(new BsonDocument('ok', new BsonInt32(1))
                                                  .append('err', new BsonString('error'))
                                                  .append('code', new BsonInt32(10107)),
                                          new ServerAddress(), WriteConcernResult.acknowledged(0, false, null))))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback)
        futureResultCallback.get(10, SECONDS)

        then:
        thrown(WriteConcernException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    def 'should invalidate on socket exceptions'() {
        given:
        def connectionPool = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        def internalConnection = Mock(InternalConnection)
        connectionPool.get() >> { internalConnection }
        serverMonitorFactory.create(_) >> { serverMonitor }

        TestConnectionFactory connectionFactory = new TestConnectionFactory()

        def server = new DefaultServer(new ServerAddress(), connectionPool, connectionFactory, serverMonitorFactory)
        def testConnection = (TestConnection) server.getConnection()

        when:
        testConnection.enqueueProtocol(new ThrowingProtocol(new MongoSocketException('socket error', new ServerAddress())))

        testConnection.insert(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())))

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()

        when:
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        testConnection.insertAsync(new MongoNamespace('test', 'test'), true, ACKNOWLEDGED, asList(new InsertRequest(new BsonDocument())),
                                   futureResultCallback)
        futureResultCallback.get(10, SECONDS)

        then:
        thrown(MongoSocketException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }

    class ThrowingProtocol implements Protocol {
        private final MongoException mongoException

        ThrowingProtocol(MongoException mongoException) {
            this.mongoException = mongoException
        }

        @Override
        Object execute(final InternalConnection connection) {
            throw mongoException;
        }

        @Override
        void executeAsync(final InternalConnection connection, final SingleResultCallback callback) {
            callback.onResult(null, mongoException);
        }
    }
}
