/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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





package org.mongodb.connection

import org.bson.ByteBuf
import org.mongodb.event.ConnectionEvent
import org.mongodb.event.ConnectionPoolEvent
import org.mongodb.event.ConnectionPoolListener
import org.mongodb.event.ConnectionPoolWaitQueueEvent
import spock.lang.Specification
import spock.lang.Subject

import javax.management.ObjectName
import java.lang.management.ManagementFactory

import static java.util.concurrent.TimeUnit.MILLISECONDS

class PooledConnectionProviderSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()

    private final TestInternalConnectionFactory connectionFactory = Spy(TestInternalConnectionFactory)

    @Subject
    private PooledConnectionProvider provider

    def cleanup() {
        provider.close()
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                new NoOpConnectionPoolListener())

        expect:
        provider.get() != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                new NoOpConnectionPoolListener())

        when:
        provider.get().close()
        provider.get()

        then:
        1 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .build(),
                new NoOpConnectionPoolListener())

        when:
        provider.get().close()

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
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
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(50, MILLISECONDS)
                        .build(),
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
                sendMessage(_) >> { throw new MongoSocketWriteException('', SERVER_ADDRESS, new IOException()) }
            }
        }

        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                mockConnectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(2)
                        .maxWaitQueueSize(1)
                        .build(),
                new NoOpConnectionPoolListener())
        when:
        def c1 = provider.get()
        def c2 = provider.get()

        and:
        c2.sendMessage(Collections.<ByteBuf> emptyList())

        then:
        thrown(MongoSocketWriteException)

        and:
        c1.close()
        c2.close()
        provider.get()

        then:
        numberOfConnectionsCreated == 3
    }

    def 'should have size of 0 with default settings'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(10)
                        .build(),
                new NoOpConnectionPoolListener())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    def 'statistics should reflect values from the provider'() {
        when:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build(),
                new NoOpConnectionPoolListener())
        provider.get()
        provider.get().close()

        then:
        with(provider.statistics) {
            host == SERVER_ADDRESS.host
            port == SERVER_ADDRESS.port
            minSize == 0
            maxSize == 5
            total == 2
            inUse == 1
        }
    }

    def 'should register MBean in org.mongodb.driver domain'() {
        when:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(1).maxSize(5).build(),
                new NoOpConnectionPoolListener())

        then:
        new ObjectName(provider.statistics.objectName).domain == 'org.mongodb.driver'
        ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(provider.statistics.objectName))
    }

    def 'should unregister MBean'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory,
                ConnectionPoolSettings.builder().minSize(1).maxSize(5).build(),
                new NoOpConnectionPoolListener())
        def beanName = new ObjectName(provider.statistics.objectName)

        when:
        provider.close()

        then:
        !ManagementFactory.getPlatformMBeanServer().isRegistered(beanName)
    }

    def 'should ensure min pool size after maintenance task runs'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(10)
                        .minSize(5)
                        .build(),
                new NoOpConnectionPoolListener())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 5
    }

    def 'should invoke connection pool opened event'() {
        given:
        def listener = Mock(ConnectionPoolListener)

        when:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(10).minSize(5).build(),
                listener)

        then:
        1 * listener.connectionPoolOpened(new ConnectionPoolEvent(CLUSTER_ID, SERVER_ADDRESS))
    }

    def 'should invoke connection pool closed event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(10).minSize(5).build(),
                listener)

        when:
        provider.close()

        then:
        1 * listener.connectionPoolClosed(new ConnectionPoolEvent(CLUSTER_ID, SERVER_ADDRESS))
    }

    def 'should fire connection added to pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(10).maxWaitQueueSize(1).build(),
                listener)

        when:
        provider.get()

        then:
        1 * listener.connectionAdded(_)
    }

    def 'should fire connection removed from pool event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(10).maxWaitQueueSize(1).build(),
                listener)
        def connection = provider.get()
        def connectionId = connection.getId()
        connection.close()

        when:
        provider.close()

        then:
        1 * listener.connectionRemoved(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS, connectionId))
    }

    def 'should fire connection pool events on check out and check in'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                listener)
        def connection = provider.get()
        def connectionId = connection.getId()
        connection.close()

        when:
        connection = provider.get()

        then:
        1 * listener.waitQueueEntered(new ConnectionPoolWaitQueueEvent(CLUSTER_ID, SERVER_ADDRESS, Thread.currentThread().getId()))
        1 * listener.connectionCheckedOut(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS, connectionId))
        1 * listener.waitQueueExited(new ConnectionPoolWaitQueueEvent(CLUSTER_ID, SERVER_ADDRESS, Thread.currentThread().getId()))

        when:
        connection.close()

        then:
        1 * listener.connectionCheckedIn(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS, connectionId))
    }
}
