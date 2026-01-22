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
import com.mongodb.MongoConnectionPoolClearedException
import com.mongodb.MongoServerUnavailableException
import com.mongodb.MongoTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.event.ConnectionCheckOutFailedEvent
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.internal.inject.EmptyProvider
import com.mongodb.internal.inject.SameObjectProvider
import com.mongodb.internal.logging.LogMessage
import com.mongodb.logging.TestLoggingInterceptor
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Subject
import com.mongodb.spock.Slow

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.mongodb.ClusterFixture.getOperationContext
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS
import static com.mongodb.ClusterFixture.createOperationContext
import static com.mongodb.connection.ConnectionPoolSettings.builder
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES

class DefaultConnectionPoolSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()
    private static final ServerId SERVER_ID = new ServerId(new ClusterId("test"), SERVER_ADDRESS)

    private final TestInternalConnectionFactory connectionFactory = Spy(TestInternalConnectionFactory)
    private TestLoggingInterceptor interceptor;

    @Subject
    private DefaultConnectionPool pool

    def setup() {
        def filterConfig = [:]
        filterConfig[LogMessage.Component.CONNECTION] = LogMessage.Level.DEBUG
        interceptor = new TestLoggingInterceptor("test",
                new TestLoggingInterceptor.LoggingFilter(filterConfig))
    }

    def cleanup() {
        interceptor.close();
        pool.close()
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        expect:
        pool.get(getOperationContext()) != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def operationContext = getOperationContext()

        when:
        pool.get(operationContext).close()
        pool.get(operationContext)

        then:
        1 * connectionFactory.create(SERVER_ID, _)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        pool.get(getOperationContext()).close()

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        def first = pool.get(createOperationContext(TIMEOUT_SETTINGS.withMaxWaitTimeMS(50)))

        then:
        first != null

        when:
        pool.get(createOperationContext(TIMEOUT_SETTINGS.withMaxWaitTimeMS(50)))

        then:
        thrown(MongoTimeoutException)
    }

    def 'should throw on timeout'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        def timeoutSettings = TIMEOUT_SETTINGS.withMaxWaitTimeMS(50)
        pool.get(createOperationContext(timeoutSettings))

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool, timeoutSettings)
        new Thread(connectionGetter).start()

        connectionGetter.latch.await()

        then:
        connectionGetter.gotTimeout
    }

    def 'should have size of 0 with default settings'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(10).maintenanceInitialDelay(5, MINUTES).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        pool.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    @Slow
    def 'should ensure min pool size after maintenance task runs'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(10).minSize(5).maintenanceInitialDelay(5, MINUTES).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when: 'the maintenance tasks runs'
        pool.doMaintenance()
        //not cool - but we have no way of being notified that the maintenance task has finished
        Thread.sleep(500)

        then: 'it ensures the minimum size of the pool'
        connectionFactory.createdConnections.size() == 5
        connectionFactory.createdConnections.get(0).opened()  // if the first one is opened, they all should be

        when: 'the pool is invalidated and the maintenance tasks runs'
        pool.invalidate(null)
        pool.ready()
        pool.doMaintenance()
        //not cool - but we have no way of being notified that the maintenance task has finished
        Thread.sleep(500)

        then: 'it prunes the existing connections and again ensures the minimum size of the pool'
        connectionFactory.createdConnections.size() == 10
        connectionFactory.createdConnections.get(0).opened()  // if the first one is opened, they all should be
    }

    def 'should invoke connection pool created event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build()

        when:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        then:
        1 * listener.connectionPoolCreated { it.serverId == SERVER_ID && it.settings == settings }
    }

    def 'should invoke connection pool closed event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build()
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        when:
        pool.close()

        then:
        1 * listener.connectionPoolClosed { it.serverId == SERVER_ID }
    }

    def 'should fire connection created to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        pool.ready()
        pool.get(getOperationContext())

        then:
        1 * listener.connectionCreated { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionReady { it.connectionId.serverId == SERVER_ID }
    }

    def 'should log connection pool events'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build()
        def connection = Mock(InternalConnection)
        def connectionDescription = Mock(ConnectionDescription)
        def driverConnectionId = 1
        def id = new ConnectionId(SERVER_ID, driverConnectionId, 1);
        connectionFactory.create(SERVER_ID, _) >> connection
        connectionDescription.getConnectionId() >> id
        connection.getDescription() >> connectionDescription
        connection.opened() >> false
        def operationContext = getOperationContext()

        when: 'connection pool is created'
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        then: '"pool is created" log message is emitted'
        def poolCreatedLogMessage = getMessage("Connection pool created")
        "Connection pool created for ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()} using options " +
                "maxIdleTimeMS=${settings.getMaxConnectionIdleTime(MILLISECONDS)}, " +
                "minPoolSize=${settings.getMinSize()}, maxPoolSize=${settings.getMaxSize()}, " +
                "maxConnecting=${settings.getMaxConnecting()}, " +
                "waitQueueTimeoutMS=${settings.getMaxWaitTime(MILLISECONDS)}" == poolCreatedLogMessage

        when: 'connection pool is ready'
        pool.ready()
        then: '"pool is ready" log message is emitted'
        def poolReadyLogMessage = getMessage("Connection pool ready")
        "Connection pool ready for ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}" == poolReadyLogMessage

        when: 'connection is created'
        pool.get(operationContext)
        then: '"connection created" and "connection ready" log messages are emitted'
        def createdLogMessage = getMessage( "Connection created")
        def readyLogMessage = getMessage("Connection ready")
        "Connection created: address=${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}, " +
                "driver-generated ID=${driverConnectionId}" == createdLogMessage
        readyLogMessage ==~ "Connection ready: address=${quoteHostnameOrIp(SERVER_ADDRESS.getHost())}:${SERVER_ADDRESS.getPort()}" +
                ", driver-generated ID=${driverConnectionId}, established in=\\d+ ms"

        when: 'connection is released back into the pool on close'
        pool.get(operationContext).close()
        then: '"connection check out" and "connection checked in" log messages are emitted'
        def checkoutStartedMessage = getMessage("Connection checkout started")
        def connectionCheckedInMessage = getMessage("Connection checked in")
        def checkedOutLogMessage = getMessage("Connection checked out")
        checkedOutLogMessage ==~ "Connection checked out: " +
                "address=${quoteHostnameOrIp(SERVER_ADDRESS.getHost())}:${SERVER_ADDRESS.getPort()}, " +
                "driver-generated ID=${driverConnectionId}, duration=\\d+ ms"
        "Checkout started for connection to ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}" == checkoutStartedMessage
        "Connection checked in: address=${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}, " +
                "driver-generated ID=${driverConnectionId}" == connectionCheckedInMessage

        when: 'connection pool is cleared'
        pool.invalidate(null)
        then: '"connection pool cleared" log message is emitted'
        def poolClearedLogMessage = getMessage("Connection pool cleared")
        "Connection pool for ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()} cleared"  == poolClearedLogMessage

        when: 'the maintenance tasks runs'
        pool.doMaintenance()
        //not cool - but we have no way of being notified that the maintenance task has finished
        Thread.sleep(500)
        then: '"connection became stale" log message is emitted'
        def unstructuredMessage = getMessage("Connection closed")
        "Connection closed: address=${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}, " +
                "driver-generated ID=1. " +
                "Reason: Connection became stale because the pool was cleared." == unstructuredMessage

        when: 'pool is closed'
        pool.close()
        then: '"connection pool closed" log message is emitted'
        def poolClosedLogMessage = getMessage("Connection pool closed")
        "Connection pool closed for ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}"  == poolClosedLogMessage

        when: 'connection checked out on closed pool'
        pool.get(operationContext)
        then:
        thrown(MongoServerUnavailableException)
        def connectionCheckoutFailedInMessage = getMessage("Connection checkout failed")
        connectionCheckoutFailedInMessage ==~ "Checkout failed for connection to" +
                " ${quoteHostnameOrIp(SERVER_ADDRESS.getHost())}:${SERVER_ADDRESS.getPort()}." +
                " Reason: Connection pool was closed. Duration: \\d+ ms"
    }

    private String getMessage(messageId) {
        interceptor.getMessages()
                .find {
                    it.getMessageId() == messageId
                }
                .toUnstructuredLogMessage().interpolate()
    }


    def 'should log on checkout timeout fail'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        def timeoutSettings = ClusterFixture.TIMEOUT_SETTINGS.withMaxWaitTimeMS(50)
        pool.get(createOperationContext(timeoutSettings))

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool, timeoutSettings)
        new Thread(connectionGetter).start()
        connectionGetter.latch.await()

        then:
        connectionGetter.gotTimeout
        def unstructuredMessage = getMessage("Connection checkout failed")
        unstructuredMessage ==~ "Checkout failed for connection to" +
                " ${quoteHostnameOrIp(SERVER_ADDRESS.getHost())}:${SERVER_ADDRESS.getPort()}." +
                " Reason: Wait queue timeout elapsed without a connection becoming available." +
                " Duration: \\d+ ms"
    }

    def 'should log on connection become idle'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(2).minSize(0).maxConnectionIdleTime(1, MILLISECONDS).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        pool.ready()
        pool.get(getOperationContext()).close()
        //not cool - but we have no way of waiting for connection to become idle
        Thread.sleep(500)
        pool.close();

        then:
        def unstructuredMessage = getMessage("Connection closed")
        "Connection closed: address=${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()}," +
                " driver-generated ID=1." +
                " Reason: Connection has been available but unused for longer than the configured max " +
                "idle time." == unstructuredMessage
    }


    def 'should log on connection pool cleared in load-balanced mode'() {
        given:
        def serviceId = new ObjectId()
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1)
                        .minSize(0)
                        .maxConnectionIdleTime(1, MILLISECONDS)
                        .build(), EmptyProvider.instance(), OPERATION_CONTEXT_FACTORY)

        when:
        pool.ready()
        pool.invalidate(serviceId, 1);

        then:
        def poolClearedLogMessage = getMessage("Connection pool cleared")
        "Connection pool for ${SERVER_ADDRESS.getHost()}:${SERVER_ADDRESS.getPort()} " +
                "cleared for serviceId ${serviceId.toHexString()}"  == poolClearedLogMessage
    }

    def 'should log connection checkout failed with Reason.CONNECTION_ERROR if fails to open a connection'() {
        given:
        def operationContext = getOperationContext()
        def listener = Mock(ConnectionPoolListener)
        def connection = Mock(InternalConnection)
        connection.getDescription() >> new ConnectionDescription(SERVER_ID)
        connection.opened() >> false
        connection.open(operationContext) >> { throw new UncheckedIOException('expected failure', new IOException()) }
        connectionFactory.create(SERVER_ID, _) >> connection
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        try {
            pool.get(operationContext)
        } catch (UncheckedIOException e) {
            if ('expected failure' != e.getMessage()) {
                throw e
            }
        }

        then:
        def unstructuredMessage = getMessage("Connection checkout failed" )
        unstructuredMessage ==~ "Checkout failed for connection to" +
               " ${quoteHostnameOrIp(SERVER_ADDRESS.getHost())}:${SERVER_ADDRESS.getPort()}." +
               " Reason: An error occurred while trying to establish a new connection." +
               " Error: java.io.UncheckedIOException: expected failure." +
               " Duration: \\d+ ms"
    }

    def 'should fire asynchronous connection created to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        pool.ready()
        selectConnectionAsyncAndGet(pool)

        then:
        1 * listener.connectionCreated { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionReady { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = pool.get(getOperationContext())
        connection.close()

        when:
        pool.close()

        then:
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire asynchronous connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = selectConnectionAsyncAndGet(pool)
        connection.close()

        when:
        pool.close()

        then:
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection pool events on check out and check in'() {
        given:
        def operationContext = getOperationContext()
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = pool.get(operationContext)
        connection.close()

        when:
        connection = pool.get(operationContext)

        then:
        1 * listener.connectionCheckedOut { it.connectionId.serverId == SERVER_ID }

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire asynchronous connection pool events on check out and check in'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = selectConnectionAsyncAndGet(pool)
        connection.close()

        when:
        connection = pool.get(getOperationContext())

        then:
        1 * listener.connectionCheckedOut { it.connectionId.serverId == SERVER_ID }

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection checkout failed with Reason.CONNECTION_ERROR if fails to open a connection'() {
        given:
        def operationContext = getOperationContext()
        def listener = Mock(ConnectionPoolListener)
        def connection = Mock(InternalConnection)
        connection.getDescription() >> new ConnectionDescription(SERVER_ID)
        connection.opened() >> false
        connection.open(operationContext) >> { throw new UncheckedIOException('expected failure', new IOException()) }
        connectionFactory.create(SERVER_ID, _) >> connection
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        try {
            pool.get(operationContext)
        } catch (UncheckedIOException e) {
            if ('expected failure' != e.getMessage()) {
                throw e
            }
        }

        then:
        1 * listener.connectionCheckOutFailed { it.reason == ConnectionCheckOutFailedEvent.Reason.CONNECTION_ERROR }
    }

    def 'should fire connection checkout failed with Reason.CONNECTION_ERROR if fails to open a connection asynchronously'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def connection = Mock(InternalConnection)
        connection.getDescription() >> new ConnectionDescription(SERVER_ID)
        connection.opened() >> false
        connection.openAsync(_, _) >> {
            it.last().onResult(null, new UncheckedIOException('expected failure', new IOException()))
        }
        connectionFactory.create(SERVER_ID, _) >> connection
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        try {
            selectConnectionAsyncAndGet(pool)
        } catch (UncheckedIOException e) {
            if ('expected failure' != e.getMessage()) {
                throw e
            }
        }

        then:
        1 * listener.connectionCheckOutFailed { it.reason == ConnectionCheckOutFailedEvent.Reason.CONNECTION_ERROR }
    }

    def 'should fire MongoConnectionPoolClearedException when checking out in paused state'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        Throwable caught = null

        when:
        try {
            pool.get(getOperationContext())
        } catch (MongoConnectionPoolClearedException e) {
            caught = e
        }

        then:
        caught != null
    }

    def 'should fire MongoConnectionPoolClearedException when checking out asynchronously in paused state'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        CompletableFuture<Throwable> caught = new CompletableFuture<>()

        when:
        pool.getAsync(getOperationContext()) { InternalConnection result, Throwable t ->
            if (t != null) {
                caught.complete(t)
            }
        }

        then:
        caught.isDone()
        caught.get() instanceof MongoConnectionPoolClearedException
    }

    def 'invalidate should record cause'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        RuntimeException cause = new RuntimeException()
        Throwable caught = null

        when:
        pool.invalidate(cause)
        try {
            pool.get(getOperationContext())
        } catch (MongoConnectionPoolClearedException e) {
            caught = e
        }

        then:
        caught.getCause().is(cause)
    }

    def 'should not repeat ready/cleared events'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY)

        when:
        pool.ready()
        pool.ready()
        pool.invalidate(null)
        pool.invalidate(new RuntimeException())

        then:
        1 * listener.connectionPoolReady { it.getServerId() == SERVER_ID }
        1 * listener.connectionPoolCleared { it.getServerId() == SERVER_ID }
    }

    def 'should continue to fire events after pool is closed'() {
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = pool.get(getOperationContext())
        pool.close()

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
    }

    def 'should continue to fire events after pool is closed (asynchronous)'() {
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        def connection = selectConnectionAsyncAndGet(pool)
        pool.close()

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
    }

    def 'should select connection asynchronously if one is immediately available'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        expect:
        selectConnectionAsyncAndGet(pool).opened()
    }

    def 'should select connection asynchronously if one is not immediately available'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()

        when:
        def connection = pool.get(getOperationContext())
        def connectionLatch = selectConnectionAsync(pool)
        connection.close()

        then:
        connectionLatch.get()
    }

    def 'when getting a connection asynchronously should send MongoTimeoutException to callback after timeout period'() {
        given:
        def operationContext = getOperationContext()
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).maxWaitTime(5, MILLISECONDS).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.ready()
        pool.get(operationContext)
        def firstConnectionLatch = selectConnectionAsync(pool, operationContext)
        def secondConnectionLatch = selectConnectionAsync(pool, operationContext)

        when:
        firstConnectionLatch.get()

        then:
        thrown(MongoTimeoutException)

        when:
        secondConnectionLatch.get()

        then:
        thrown(MongoTimeoutException)
    }

    def 'invalidate should do nothing when pool is closed'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY)
        pool.close()

        when:
        pool.invalidate(null)

        then:
        noExceptionThrown()
    }

    def selectConnectionAsyncAndGet(DefaultConnectionPool pool) {
        selectConnectionAsync(pool).get()
    }

    def selectConnectionAsync(DefaultConnectionPool pool, operationContext = getOperationContext()) {
        def serverLatch = new ConnectionLatch()
        pool.getAsync(operationContext) { InternalConnection result, Throwable e ->
            serverLatch.connection = result
            serverLatch.throwable = e
            serverLatch.latch.countDown()
        }
        serverLatch
    }

    private mockSdamProvider() {
        SameObjectProvider.initialized(Mock(SdamServerDescriptionManager))
    }

    private static quoteHostnameOrIp(String hostnameOrIp) {
        hostnameOrIp.replaceAll(Pattern.quote("."), Matcher.quoteReplacement("\\."))
    }

    class ConnectionLatch {
        CountDownLatch latch = new CountDownLatch(1)
        InternalConnection connection
        Throwable throwable

        def get() {
            latch.await()
            if (throwable != null) {
                throw throwable
            }
            connection
        }
    }
}
