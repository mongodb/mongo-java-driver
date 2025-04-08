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
import com.mongodb.MongoClientException
import com.mongodb.MongoException
import com.mongodb.MongoInternalException
import com.mongodb.MongoInterruptedException
import com.mongodb.MongoTimeoutException
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterType
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.event.ServerDescriptionChangedEvent
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.selector.ReadPreferenceServerSelector
import com.mongodb.internal.selector.ServerAddressSelector
import com.mongodb.internal.selector.WritableServerSelector
import com.mongodb.internal.time.Timeout
import spock.lang.Specification
import com.mongodb.spock.Slow

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS
import static com.mongodb.ClusterFixture.createOperationContext
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterSettings.builder
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

/**
 * Add new tests to {@link BaseClusterTest}.
 */
class BaseClusterSpecification extends Specification {

    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')
    private final List<ServerAddress> allServers = [firstServer, secondServer, thirdServer]
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should have current description immediately after construction'() {
        given:
        def clusterSettings = builder().mode(MULTIPLE)
                .hosts([firstServer, secondServer, thirdServer])
                .serverSelector(new ServerAddressSelector(firstServer))
                .build()
        def cluster = new BaseCluster(new ClusterId(), clusterSettings, factory) {
            @Override
            protected void connect() {
            }

            @Override
            Cluster.ServersSnapshot getServersSnapshot(final Timeout serverSelectionTimeout,  final TimeoutContext timeoutContext) {
                Cluster.ServersSnapshot result = {
                    serverAddress -> throw new UnsupportedOperationException()
                }
                result
            }

            @Override
            void onChange(final ServerDescriptionChangedEvent event) {
            }
        }

        expect: 'the description is initialized after construction'
        cluster.getCurrentDescription() == new ClusterDescription(clusterSettings.getMode(), ClusterType.UNKNOWN, [], clusterSettings,
                factory.getSettings())

        when: 'a server is selected before initialization'
        cluster.selectServer({ def clusterDescription -> [] },
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(1)))

        then: 'a MongoTimeoutException is thrown'
        thrown(MongoTimeoutException)

        when: 'a server is selected before initialization and timeoutMS is set'
        cluster.selectServer({ def clusterDescription -> [] },
                createOperationContext(TIMEOUT_SETTINGS
                        .withServerSelectionTimeoutMS(1)
                        .withTimeout(1, MILLISECONDS)))

        then: 'a MongoTimeoutException is thrown'
        thrown(MongoTimeoutException)
    }

    def 'should get cluster settings'() {
        given:
        def clusterSettings = builder().mode(MULTIPLE)
                .hosts([firstServer, secondServer, thirdServer])
                .serverSelectionTimeout(1, SECONDS)
                .serverSelector(new ServerAddressSelector(firstServer))
                .build()
        def cluster = new MultiServerCluster(new ClusterId(), clusterSettings, factory)

        expect:
        cluster.getSettings() == clusterSettings
    }

    def 'should compose server selector passed to selectServer with server selector in cluster settings'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .serverSelectionTimeout(1, SECONDS)
                        .serverSelector(new ServerAddressSelector(firstServer))
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()), OPERATION_CONTEXT)
                .serverDescription.address == firstServer
    }

    def 'should use server selector passed to selectServer if server selector in cluster settings is null'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ServerAddressSelector(firstServer),
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(1_000)))
                .serverDescription.address == firstServer
    }

    def 'should apply local threshold when custom server selector is present'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .serverSelectionTimeout(1, SECONDS)
                        .serverSelector(new ReadPreferenceServerSelector(ReadPreference.secondary()))
                        .localThreshold(5, MILLISECONDS)
                        .build(),
                factory)
        factory.sendNotification(firstServer, 1, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, 7, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, 1, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.nearest()), OPERATION_CONTEXT)
                .serverDescription.address == firstServer
    }

    def 'should apply local threshold when custom server selector in absent'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .serverSelectionTimeout(1, SECONDS)
                        .hosts([firstServer, secondServer, thirdServer])
                        .localThreshold(5, MILLISECONDS)
                        .build(),
                factory)
        factory.sendNotification(firstServer, 1, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, 7, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, 1, REPLICA_SET_PRIMARY, allServers)

        expect: // firstServer is the only secondary within the latency threshold
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()), OPERATION_CONTEXT)
                .serverDescription.address == firstServer
    }

    def 'should timeout with useful message'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer])
                        .build(),
                factory)

        when:
        factory.sendNotification(firstServer, ServerDescription.builder().type(ServerType.UNKNOWN)
                                                               .state(ServerConnectionState.CONNECTING)
                                                               .address(firstServer)
                                                               .exception(new MongoInternalException('oops'))
                                                               .build())

        cluster.selectServer(new WritableServerSelector(),
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS)))

        then:
        def e = thrown(MongoTimeoutException)

        e.getMessage().startsWith("Timed out while waiting for a server " +
                'that matches WritableServerSelector. Client view of cluster state is {type=UNKNOWN')
        e.getMessage().contains('{address=localhost:27017, type=UNKNOWN, state=CONNECTING, ' +
                'exception={com.mongodb.MongoInternalException: oops}}')
        e.getMessage().contains('{address=localhost:27018, type=UNKNOWN, state=CONNECTING}')

        where:
        serverSelectionTimeoutMS << [1, 0]
    }

    def 'should select server'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.primary()),
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS)))
                .serverDescription.address == thirdServer

        cleanup:
        cluster?.close()

        where:
        serverSelectionTimeoutMS << [30, 0, -1]
    }

    @Slow
    def 'should wait indefinitely for a server until interrupted'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)

        when:
        def latch = new CountDownLatch(1)
        def thread = new Thread({
            try {
                cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.primary()),
                        createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(-1_000)))
            } catch (MongoInterruptedException e) {
                latch.countDown()
            }
        })
        thread.start()
        sleep(1000)
        thread.interrupt()
        def interrupted = latch.await(ClusterFixture.TIMEOUT, SECONDS)

        then:
        interrupted

        cleanup:
        cluster?.close()
    }

    def 'should select server asynchronously when server is already available'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)

        when:
        def serverDescription = selectServerAsync(cluster, firstServer, serverSelectionTimeoutMS).getDescription()

        then:
        serverDescription.address == firstServer

        cleanup:
        cluster?.close()

        where:
        serverSelectionTimeoutMS << [30, 0, -1]
    }

    def 'should select server asynchronously when server is not yet available'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)

        when:
        def secondServerLatch = selectServerAsync(cluster, secondServer, serverSelectionTimeoutMS)
        def thirdServerLatch = selectServerAsync(cluster, thirdServer, serverSelectionTimeoutMS)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_SECONDARY, allServers)

        then:
        secondServerLatch.getDescription().address == secondServer
        thirdServerLatch.getDescription().address == thirdServer

        cleanup:
        cluster?.close()

        where:
        serverSelectionTimeoutMS << [500, -1]
    }

    def 'when selecting server asynchronously should send MongoClientException to callback if cluster is closed before success'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)

        when:
        def serverLatch = selectServerAsync(cluster, firstServer)
        cluster.close()
        serverLatch.get()

        then:
        thrown(MongoClientException)

        cleanup:
        cluster?.close()
    }

    def 'when selecting server asynchronously should send MongoTimeoutException to callback after timeout period'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)

        when:
        selectServerAsyncAndGet(cluster, firstServer, serverSelectionTimeoutMS)

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()


        where:
        serverSelectionTimeoutMS << [100, 0]
    }

    def selectServerAsyncAndGet(BaseCluster cluster, ServerAddress serverAddress) {
        selectServerAsync(cluster, serverAddress, 1_000)
    }

    def selectServerAsyncAndGet(BaseCluster cluster, ServerAddress serverAddress, long serverSelectionTimeoutMS) {
        selectServerAsync(cluster, serverAddress, serverSelectionTimeoutMS).get()
    }

    def selectServerAsync(BaseCluster cluster, ServerAddress serverAddress) {
        selectServerAsync(cluster, serverAddress, 1_000)
    }

    def selectServerAsync(BaseCluster cluster, ServerAddress serverAddress, long serverSelectionTimeoutMS) {
        def serverLatch = new ServerLatch()
        cluster.selectServerAsync(new ServerAddressSelector(serverAddress),
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS))) {
            ServerTuple result, MongoException e ->
                serverLatch.server = result != null ? result.getServer() : null
                serverLatch.serverDescription = result != null ? result.serverDescription : null
                serverLatch.throwable = e
                serverLatch.latch.countDown()
        }
        serverLatch
    }

    class ServerLatch {
        CountDownLatch latch = new CountDownLatch(1)
        Server server
        ServerDescription serverDescription
        Throwable throwable

        def get() {
            latch.await()
            if (throwable != null) {
                throw throwable
            }
            server
        }

        def getDescription() {
            latch.await()
            if (throwable != null) {
                throw throwable
            }
            serverDescription
        }
    }
}
