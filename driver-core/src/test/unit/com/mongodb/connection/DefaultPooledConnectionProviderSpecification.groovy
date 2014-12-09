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

import category.Slow
import com.mongodb.MongoException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketWriteException
import com.mongodb.MongoTimeoutException
import com.mongodb.MongoWaitQueueFullException
import com.mongodb.ServerAddress
import com.mongodb.event.ConnectionPoolListener
import org.bson.ByteBuf
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CountDownLatch

import static com.mongodb.connection.ConnectionPoolSettings.builder
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES

class DefaultPooledConnectionProviderSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress())

    private final TestInternalConnectionFactory connectionFactory = Spy(TestInternalConnectionFactory)

    @Subject
    private DefaultConnectionPool provider

    def cleanup() {
        provider.close()
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(),
                                             new NoOpConnectionPoolListener())

        expect:
        provider.get() != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(),
                                             new NoOpConnectionPoolListener())

        when:
        provider.get().close()
        provider.get()

        then:
        1 * connectionFactory.create(SERVER_ID)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(),
                                             new NoOpConnectionPoolListener())

        when:
        provider.get().close()

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).maxWaitTime(1, MILLISECONDS).build(),
                                             new NoOpConnectionPoolListener())

        when:
        def first = provider.get()

        then:
        first != null

        when:
        provider.get()

        then:
        thrown(MongoTimeoutException)
    }

    def 'should throw on timeout'() throws InterruptedException {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).maxWaitTime(50, MILLISECONDS).build(),
                                             new NoOpConnectionPoolListener())
        provider.get()

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(provider)
        new Thread(connectionGetter).start()

        connectionGetter.latch.await()

        then:
        connectionGetter.gotTimeout
    }

    def 'should expire all connection after exception'() throws InterruptedException {
        given:
        int numberOfConnectionsCreated = 0

        def mockConnectionFactory = Mock(InternalConnectionFactory)
        mockConnectionFactory.create(_) >> {
            numberOfConnectionsCreated++
            Mock(InternalConnection) {
                sendMessage(_, _) >> { throw new MongoSocketWriteException('', SERVER_ID.address, new IOException()) }
                receiveMessage(_) >> { throw new MongoSocketReadException('', SERVER_ID.address, new IOException()) }
                getDescription() >> {
                    new ConnectionDescription(SERVER_ID);
                }
            }
        }

        provider = new DefaultConnectionPool(SERVER_ID, mockConnectionFactory, builder().maxSize(2).maxWaitQueueSize(1).build(),
                                             new NoOpConnectionPoolListener())
        when:
        def c1 = provider.get()
        def c2 = provider.get()

        and:
        c2.sendMessage(Collections.<ByteBuf> emptyList(), 1)

        then:
        thrown(MongoSocketWriteException)

        and:
        c1.close()
        c2.close()
        provider.get().close()

        then:
        numberOfConnectionsCreated == 3

        when:
        c1 = provider.get()
        c2 = provider.get()

        and:
        c2.receiveMessage(1)

        then:
        thrown(MongoSocketReadException)

        and:
        c1.close()
        c2.close()
        provider.get().close()

        then:
        numberOfConnectionsCreated == 5
    }

    def 'should expire all connection after exception asynchronously'()  {
        given:
        int numberOfConnectionsCreated = 0

        def mockConnectionFactory = Mock(InternalConnectionFactory)
        mockConnectionFactory.create(_) >> {
            numberOfConnectionsCreated++
            Mock(InternalConnection) {
                sendMessageAsync(_, _, _) >> {
                    it[2].onResult(null, new MongoSocketWriteException('', SERVER_ID.address, new IOException()))
                };
                receiveMessageAsync(_, _) >> {
                    it[1].onResult(null, new MongoSocketReadException('', SERVER_ID.address, new IOException()))
                };
                getDescription() >> {
                    new ConnectionDescription(SERVER_ID);
                }
            }
        }

        provider = new DefaultConnectionPool(SERVER_ID, mockConnectionFactory,
                                             builder().maxSize(2).maxWaitQueueSize(1).build(),
                                             new NoOpConnectionPoolListener())
        when:
        def c1 = provider.get()
        def c2 = provider.get()

        and:
        def e;
        c2.sendMessageAsync(Collections.<ByteBuf> emptyList(), 1) {
            result, t -> e = t }

        then:
        e instanceof MongoSocketWriteException

        and:
        c1.close()
        c2.close()
        provider.get().close()

        then:
        numberOfConnectionsCreated == 3

        when:
        c1 = provider.get()
        c2 = provider.get()

        and:
        c2.receiveMessageAsync(1) {
            result, t -> e = t
        }

        then:
        e instanceof MongoSocketReadException

        and:
        c1.close()
        c2.close()
        provider.get().close()

        then:
        numberOfConnectionsCreated == 5
    }

    def 'should have size of 0 with default settings'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(10).maintenanceInitialDelay(5, MINUTES).build(),
                                             new NoOpConnectionPoolListener())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    @Category(Slow)
    def 'should ensure min pool size after maintenance task runs'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(10).minSize(5).maintenanceInitialDelay(5, MINUTES).build(),
                                             new NoOpConnectionPoolListener())

        when:
        provider.doMaintenance()
        //not cool - but we have no way of being notified that the maintenance task has finished
        Thread.sleep(500)

        then:
        connectionFactory.createdConnections.size() == 5
    }

    def 'should invoke connection pool opened event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).build()

        when:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, listener)

        then:
        1 * listener.connectionPoolOpened { it.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId && it.settings == settings }
    }

    def 'should invoke connection pool closed event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = builder().maxSize(10).minSize(5).build()
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, listener)
        when:
        provider.close()

        then:
        1 * listener.connectionPoolClosed { it.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
    }

    def 'should fire connection added to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10).maxWaitQueueSize(1).build(), listener)

        when:
        provider.get()

        then:
        1 * listener.connectionAdded { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
    }

    def 'should fire connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10).maxWaitQueueSize(1).build(), listener)
        def connection = provider.get()
        connection.close()

        when:
        provider.close()

        then:
        1 * listener.connectionRemoved { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
    }

    def 'should fire connection pool events on check out and check in'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1).maxWaitQueueSize(1).build(), listener)
        def connection = provider.get()
        connection.close()

        when:
        connection = provider.get()

        then:
        1 * listener.waitQueueEntered { it.serverId == SERVER_ID && it.threadId == Thread.currentThread().getId() }
        1 * listener.connectionCheckedOut { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
        1 * listener.waitQueueExited { it.serverId == SERVER_ID && it.threadId == Thread.currentThread().getId() }

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
    }

    def 'should not fire any more events after pool is closed'() {
        def listener = Mock(ConnectionPoolListener)
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1).maxWaitQueueSize(1).build(), listener)
        def connection = provider.get()
        provider.close()

        when:
        connection.close()

        then:
        0 * listener.connectionCheckedIn { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
        0 * listener.connectionRemoved { it.connectionId.serverId == SERVER_ID && it.clusterId == SERVER_ID.clusterId }
    }

    def 'should select connection asynchronously if one is immediately available'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(), new NoOpConnectionPoolListener())

        expect:
        selectConnectionAsyncAndGet(provider).opened()
    }

    def 'should select connection asynchronously if one is not immediately available'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(), new NoOpConnectionPoolListener())

        when:
        def connection = provider.get()
        def connectionLatch = selectConnectionAsync(provider)
        connection.close()

        then:
        connectionLatch.get()
    }

    def 'when getting a connection asynchronously should send MongoTimeoutException to callback after timeout period'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(2).maxWaitTime(5, MILLISECONDS).build(),
                                             new NoOpConnectionPoolListener())

        provider.get()
        def firstConnectionLatch = selectConnectionAsync(provider)
        def secondConnectionLatch = selectConnectionAsync(provider)

        when:
        firstConnectionLatch.get()

        then:
        thrown(MongoTimeoutException)

        when:
        secondConnectionLatch.get()

        then:
        thrown(MongoTimeoutException)
    }

    def 'when getting a connection asynchronously should send MongoWaitQueueFullException to callback if there are too many waiters'() {
        given:
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             builder().maxSize(1).maxWaitQueueSize(1).build(), new NoOpConnectionPoolListener())

        when:
        provider.get()
        selectConnectionAsync(provider)
        selectConnectionAsyncAndGet(provider)

        then:
        thrown(MongoWaitQueueFullException)
    }

    def selectConnectionAsyncAndGet(DefaultConnectionPool pool) {
        selectConnectionAsync(pool).get()
    }

    def selectConnectionAsync(DefaultConnectionPool pool) {
        def serverLatch = new ConnectionLatch()
        pool.getAsync { InternalConnection result, MongoException e ->
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
