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





package com.mongodb

import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.TimeUnit

import static java.util.concurrent.TimeUnit.SECONDS

class PooledConnectionProviderSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()

    private final TestInternalConnectionFactory connectionFactory = new TestInternalConnectionFactory();

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
        provider.release(provider.get());
        provider.get()

        then:
        connectionFactory.getNumCreatedConnections() == 1
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
        provider.release(provider.get())

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed()
    }

    def 'should prune a closed connection'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .build(),
                                                new NoOpConnectionPoolListener())
        def connection = provider.get()
        connection.close()

        when:
        provider.release(connection)
        provider.get()

        then:
        connectionFactory.getNumCreatedConnections() == 2
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
                                                                      .maxWaitTime(50, TimeUnit.MILLISECONDS)
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

    def 'should expire all connection after invalidate'() throws InterruptedException {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(2)
                                                                      .maxWaitQueueSize(1)
                                                                      .build(),
                                                new NoOpConnectionPoolListener())
        when:
        def c1 = provider.get()
        def c2 = provider.get()

        and:
        provider.release(c1)
        provider.invalidate()
        provider.release(c2)

        and:
        provider.get()

        then:
        connectionFactory.numCreatedConnections == 3
    }

    def 'should have size of 0 with default settings'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(10)
                                                                      .maintenanceInitialDelay(60, SECONDS)
                                                                      .build(),
                                                new NoOpConnectionPoolListener())

        when:
        provider.doMaintenance()

        then:
        connectionFactory.createdConnections.size() == 0
    }

    def 'should ensure min pool size after maintenance task runs'() {
        given:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(10)
                                                                      .minSize(5)
                                                                      .maintenanceInitialDelay(60, SECONDS)
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
        def settings = ConnectionPoolSettings.builder().maxSize(10).minSize(5).build()

        when:
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                settings,
                                                listener)

        then:
        1 * listener.connectionPoolOpened(new ConnectionPoolOpenedEvent(CLUSTER_ID, SERVER_ADDRESS, settings))
    }

    def 'should invoke connection pool closed event'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        def settings = ConnectionPoolSettings.builder().maxSize(10).minSize(5).build()
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS, connectionFactory, settings, listener)
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
        provider.release(connection)

        when:
        provider.close()

        then:
        1 * listener.connectionRemoved(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS))
    }

    def 'should fire connection pool events on check out and check in'() {
        given:
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                                listener)
        def connection = provider.get()
        provider.release(connection)

        when:
        connection = provider.get()

        then:
        1 * listener.waitQueueEntered(new ConnectionPoolWaitQueueEvent(CLUSTER_ID, SERVER_ADDRESS, Thread.currentThread().getId()))
        1 * listener.connectionCheckedOut(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS))
        1 * listener.waitQueueExited(new ConnectionPoolWaitQueueEvent(CLUSTER_ID, SERVER_ADDRESS, Thread.currentThread().getId()))

        when:
        provider.release(connection)

        then:
        1 * listener.connectionCheckedIn(new ConnectionEvent(CLUSTER_ID, SERVER_ADDRESS))
    }

    def 'should not fire any more events after pool is closed'() {
        def listener = Mock(ConnectionPoolListener)
        provider = new PooledConnectionProvider(CLUSTER_ID, SERVER_ADDRESS,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                                                listener)
        def connection = provider.get()
        provider.close()

        when:
        provider.release(connection)

        then:
        0 * listener.connectionCheckedIn(_)
        0 * listener.connectionRemoved(_)
    }
}
