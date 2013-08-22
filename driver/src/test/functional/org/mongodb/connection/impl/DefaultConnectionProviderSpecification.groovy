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







package org.mongodb.connection.impl

import org.bson.ByteBuf
import org.mongodb.connection.Connection
import org.mongodb.connection.ConnectionFactory
import org.mongodb.connection.MongoSocketWriteException
import org.mongodb.connection.MongoTimeoutException
import org.mongodb.connection.MongoWaitQueueFullException
import org.mongodb.connection.ServerAddress
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

import javax.management.ObjectName
import java.lang.management.ManagementFactory

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.mongodb.Fixture.getPrimary

class DefaultConnectionProviderSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = getPrimary()
    private final TestConnectionFactory connectionFactory = Spy(TestConnectionFactory)

    @Subject
    private DefaultConnectionProvider provider

    def cleanup() {
        provider.close()
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build())

        expect:
        provider.get() != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build())

        when:
        provider.get().close()
        provider.get()

        then:
        1 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .build())

        when:
        provider.get().close()

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build())

        when:
        Connection first = provider.get()

        then:
        first != null

        when:
        provider.get()

        then:
        thrown(MongoTimeoutException)
    }

    def 'should throw on timeout'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(50, MILLISECONDS)
                        .build())
        provider.get()

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(provider)
        new Thread(connectionGetter).start()

        connectionGetter.latch.await()

        then:
        connectionGetter.gotTimeout
    }

    @Ignore
    def 'should throw on wait queue full'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(1000, MILLISECONDS)
                        .build())

        provider.get()

        TimeoutTrackingConnectionGetter timeoutTrackingGetter = new TimeoutTrackingConnectionGetter(provider)
        Thread.sleep(100)
        new Thread(timeoutTrackingGetter).start()

        when:
        provider.get()

        then:
        thrown(MongoWaitQueueFullException)
    }

    def 'should expire connection after max life time'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(20, MILLISECONDS).build())

        when:
        provider.get().close()
        Thread.sleep(50)
        provider.doMaintenance()
        provider.get()

        then:
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should expire connection after max life time on close'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionLifeTime(20, MILLISECONDS).build())

        when:
        Connection connection = provider.get()
        Thread.sleep(50)
        connection.close()

        then:
        connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should expire connection after max idle time'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionIdleTime(5, MILLISECONDS).build())

        when:
        Connection connection = provider.get()
        connection.close()
        Thread.sleep(10)
        provider.doMaintenance()
        provider.get()

        then:
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should close connection after expiration'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionLifeTime(20, MILLISECONDS).build())

        when:
        provider.get().close()
        Thread.sleep(50)
        provider.doMaintenance()
        provider.get()

        then:
        connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should create new connection after expiration'() throws InterruptedException {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionLifeTime(5, MILLISECONDS).build())

        when:
        provider.get().close()
        Thread.sleep(50)
        provider.doMaintenance()
        Connection secondConnection = provider.get()

        then:
        secondConnection != null
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should expire all connections after exception'() throws InterruptedException {
        given:
        int numberOfConnectionsCreated = 0

        ConnectionFactory mockConnectionFactory = Mock()
        mockConnectionFactory.create(_) >> {
            numberOfConnectionsCreated++
            Mock(Connection) {
                sendMessage(_) >> { throw new MongoSocketWriteException('', SERVER_ADDRESS, new IOException()) }
            }
        }

        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                mockConnectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(2)
                        .maxWaitQueueSize(1)
                        .build())
        when:
        Connection c1 = provider.get()
        Connection c2 = provider.get()

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
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(10)
                        .build())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    def 'statistics should reflect values from the provider'() {
        when:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().minSize(0).maxSize(5).maxWaitQueueSize(1).build())
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
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().minSize(1).maxSize(5).build())

        then:
        new ObjectName(provider.statistics.objectName).domain == 'org.mongodb.driver'
        ManagementFactory.getPlatformMBeanServer().isRegistered(new ObjectName(provider.statistics.objectName))
    }

    def 'should unregister MBean'() {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                ConnectionProviderSettings.builder().minSize(1).maxSize(5).build())
        def beanName = new ObjectName(provider.statistics.objectName)

        when:
        provider.close()

        then:
        !ManagementFactory.getPlatformMBeanServer().isRegistered(beanName)
    }

    def 'should ensure min pool size after maintenance task runs'() {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(10)
                        .minSize(5)
                        .build())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 5
    }

    def 'should prune after maintenance task runs'() {
        given:
        provider = new DefaultConnectionProvider(SERVER_ADDRESS,
                connectionFactory,
                ConnectionProviderSettings.builder()
                        .maxSize(10)
                        .maxConnectionLifeTime(1, MILLISECONDS)
                        .maintenanceFrequency(5, MILLISECONDS)
                        .maxWaitQueueSize(1)
                        .build())
        provider.get().close()

        when:
        Thread.sleep(10)
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.get(0).isClosed()
    }
}
