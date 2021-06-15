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

import com.mongodb.connection.ConnectionDescription
import com.mongodb.event.ConnectionCheckOutFailedEvent
import com.mongodb.internal.async.SingleResultCallback
import util.spock.annotations.Slow
import com.mongodb.MongoTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.event.ConnectionPoolListener
import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CountDownLatch

import static com.mongodb.connection.ConnectionPoolSettings.builder
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES

class DefaultConnectionPoolSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress())

    private final TestInternalConnectionFactory connectionFactory = Spy(TestInternalConnectionFactory)

    @Subject
    private DefaultConnectionPool pool

    def cleanup() {
        pool.close()
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).build())
        pool.start();

        expect:
        pool.get() != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).build())
        pool.start();

        when:
        pool.get().close()
        pool.get()

        then:
        1 * connectionFactory.create(SERVER_ID, _)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).build())
        pool.start();

        when:
        pool.get().close()

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).maxWaitTime(1, MILLISECONDS).build())
        pool.start();

        when:
        def first = pool.get()

        then:
        first != null

        when:
        pool.get()

        then:
        thrown(MongoTimeoutException)
    }

    def 'should throw on timeout'() throws InterruptedException {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).maxWaitTime(50, MILLISECONDS).build())
        pool.start();

        pool.get()

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool)
        new Thread(connectionGetter).start()

        connectionGetter.latch.await()

        then:
        connectionGetter.gotTimeout
    }

    def 'should have size of 0 with default settings'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(10).maintenanceInitialDelay(5, MINUTES).build())
        pool.start();

        when:
        pool.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    @Slow
    def 'should ensure min pool size after maintenance task runs'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(10).minSize(5).maintenanceInitialDelay(5, MINUTES).build())
        pool.start();

        when: 'the maintenance tasks runs'
        pool.doMaintenance()
        //not cool - but we have no way of being notified that the maintenance task has finished
        Thread.sleep(500)

        then: 'it ensures the minimum size of the pool'
        connectionFactory.createdConnections.size() == 5
        connectionFactory.createdConnections.get(0).opened()  // if the first one is opened, they all should be

        when: 'the pool is invalidated and the maintenance tasks runs'
        pool.invalidate()
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
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings)

        then:
        1 * listener.connectionPoolCreated { it.serverId == SERVER_ID && it.settings == settings }
        1 * listener.connectionPoolOpened { it.serverId == SERVER_ID && it.settings == settings }
    }

    def 'should invoke connection pool closed event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build()
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings)
        when:
        pool.close()

        then:
        1 * listener.connectionPoolClosed { it.serverId == SERVER_ID }
    }

    def 'should fire connection created to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build())

        when:
        pool.get()

        then:
        1 * listener.connectionCreated { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionAdded { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionReady { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire asynchronous connection created to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build())

        when:
        selectConnectionAsyncAndGet(pool)

        then:
        1 * listener.connectionCreated { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionAdded { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionReady { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build())
        def connection = pool.get()
        connection.close()

        when:
        pool.close()

        then:
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionRemoved { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire asynchronous connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build())
        def connection = selectConnectionAsyncAndGet(pool)
        connection.close()

        when:
        pool.close()

        then:
        1 * listener.connectionClosed { it.connectionId.serverId == SERVER_ID }
        1 * listener.connectionRemoved { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection pool events on check out and check in'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build())
        def connection = pool.get()
        connection.close()

        when:
        connection = pool.get()

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
                .addConnectionPoolListener(listener).build())
        def connection = selectConnectionAsyncAndGet(pool)
        connection.close()

        when:
        connection = pool.get()

        then:
        1 * listener.connectionCheckedOut { it.connectionId.serverId == SERVER_ID }

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID }
    }

    def 'should fire connection checkout failed with Reason.CONNECTION_ERROR if fails to open a connection'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def connection = Mock(InternalConnection)
        connection.getDescription() >> new ConnectionDescription(SERVER_ID)
        connection.opened() >> false
        connection.open() >> { throw new UncheckedIOException('expected failure', new IOException()) }
        connectionFactory.create(SERVER_ID, _) >> connection
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build())

        when:
        try {
            pool.get()
        } catch (UncheckedIOException e) {
            if ('expected failure' != e.getMessage()) {
                throw e;
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
        connection.openAsync(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, new UncheckedIOException('expected failure', new IOException()))
        }
        connectionFactory.create(SERVER_ID, _) >> connection
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().addConnectionPoolListener(listener).build())

        when:
        try {
            selectConnectionAsyncAndGet(pool)
        } catch (UncheckedIOException e) {
            if ('expected failure' != e.getMessage()) {
                throw e;
            }
        }

        then:
        1 * listener.connectionCheckOutFailed { it.reason == ConnectionCheckOutFailedEvent.Reason.CONNECTION_ERROR }
    }

    def 'should continue to fire events after pool is closed'() {
        def listener = Mock(ConnectionPoolListener)
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build())
        def connection = pool.get()
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
                .addConnectionPoolListener(listener).build())
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
                                         builder().maxSize(1).build())

        expect:
        selectConnectionAsyncAndGet(pool).opened()
    }

    def 'should select connection asynchronously if one is not immediately available'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).build())

        when:
        def connection = pool.get()
        def connectionLatch = selectConnectionAsync(pool)
        connection.close()

        then:
        connectionLatch.get()
    }

    def 'when getting a connection asynchronously should send MongoTimeoutException to callback after timeout period'() {
        given:
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                         builder().maxSize(1).maxWaitTime(5, MILLISECONDS).build())

        pool.get()
        def firstConnectionLatch = selectConnectionAsync(pool)
        def secondConnectionLatch = selectConnectionAsync(pool)

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
                builder().maxSize(1).build())
        pool.close()

        when:
        pool.invalidate()

        then:
        noExceptionThrown()
    }

    def selectConnectionAsyncAndGet(DefaultConnectionPool pool) {
        selectConnectionAsync(pool).get()
    }

    def selectConnectionAsync(DefaultConnectionPool pool) {
        def serverLatch = new ConnectionLatch()
        pool.getAsync { InternalConnection result, Throwable e ->
            serverLatch.connection = result
            serverLatch.throwable = e
            serverLatch.latch.countDown()
        }
        serverLatch
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
