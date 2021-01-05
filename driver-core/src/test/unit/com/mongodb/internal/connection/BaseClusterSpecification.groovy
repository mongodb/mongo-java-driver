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

import util.spock.annotations.Slow
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
import com.mongodb.internal.selector.ReadPreferenceServerSelector
import com.mongodb.internal.selector.ServerAddressSelector
import com.mongodb.internal.selector.WritableServerSelector
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterSettings.builder
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

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
                .serverSelectionTimeout(1, MILLISECONDS)
                .serverSelector(new ServerAddressSelector(firstServer))
                .build()
        def cluster = new BaseCluster(new ClusterId(), clusterSettings, factory) {
            @Override
            protected void connect() {
            }

            @Override
            protected ClusterableServer getServer(final ServerAddress serverAddress) {
                throw new UnsupportedOperationException()
            }
        }

        expect: 'the description is initialized after construction'
        cluster.getCurrentDescription() == new ClusterDescription(clusterSettings.getMode(), ClusterType.UNKNOWN, [], clusterSettings,
                factory.getSettings())

        when: 'the description is accessed before initialization'
        cluster.getDescription()

        then: 'a MongoTimeoutException is thrown'
        thrown(MongoTimeoutException)

        when: 'a server is selected before initialization'
        cluster.selectServer { def clusterDescription -> [] }

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
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()))
                .serverDescription.address == firstServer
    }

    def 'should use server selector passed to selectServer if server selector in cluster settings is null'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .serverSelectionTimeout(1, SECONDS)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ServerAddressSelector(firstServer)).serverDescription.address == firstServer
    }

    def 'should timeout with useful message'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer])
                        .serverSelectionTimeout(serverSelectionTimeoutMS, MILLISECONDS)
                        .build(),
                factory)

        when:
        factory.sendNotification(firstServer, ServerDescription.builder().type(ServerType.UNKNOWN)
                                                               .state(ServerConnectionState.CONNECTING)
                                                               .address(firstServer)
                                                               .exception(new MongoInternalException('oops'))
                                                               .build())

        cluster.getDescription()

        then:
        def e = thrown(MongoTimeoutException)
        e.getMessage().startsWith("Timed out after ${serverSelectionTimeoutMS} ms while waiting to connect. " +
                'Client view of cluster state is {type=UNKNOWN')
        e.getMessage().contains('{address=localhost:27017, type=UNKNOWN, state=CONNECTING, ' +
                'exception={com.mongodb.MongoInternalException: oops}}');
        e.getMessage().contains('{address=localhost:27018, type=UNKNOWN, state=CONNECTING}');

        when:
        cluster.selectServer(new WritableServerSelector())

        then:
        e = thrown(MongoTimeoutException)
        e.getMessage().startsWith("Timed out after ${serverSelectionTimeoutMS} ms while waiting for a server " +
                'that matches WritableServerSelector. Client view of cluster state is {type=UNKNOWN')
        e.getMessage().contains('{address=localhost:27017, type=UNKNOWN, state=CONNECTING, ' +
                'exception={com.mongodb.MongoInternalException: oops}}');
        e.getMessage().contains('{address=localhost:27018, type=UNKNOWN, state=CONNECTING}');

        where:
        serverSelectionTimeoutMS << [1, 0]
    }

    def 'should select server'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .serverSelectionTimeout(serverSelectionTimeoutMS, SECONDS)
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.primary()))
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
                        .serverSelectionTimeout(-1, SECONDS)
                        .build(),
                factory)

        when:
        def latch = new CountDownLatch(1)
        def thread = new Thread({
            try {
                cluster.selectServer()(new ReadPreferenceServerSelector(ReadPreference.primary()))
            } catch (MongoInterruptedException e) {
                latch.countDown()
            }
        })
        thread.start()
        sleep(1000)
        thread.interrupt()
        latch.await(ClusterFixture.TIMEOUT, SECONDS)

        then:
        true

        cleanup:
        cluster?.close()
    }

    @Slow
    def 'should wait indefinitely for a cluster description until interrupted'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts([firstServer, secondServer, thirdServer])
                        .serverSelectionTimeout(-1, SECONDS)
                        .build(),
                factory)

        when:
        def latch = new CountDownLatch(1)
        def thread = new Thread({
            try {
                cluster.getDescription()
            } catch (MongoInterruptedException e) {
                latch.countDown()
            }
        })
        thread.start()
        sleep(1000)
        thread.interrupt()
        latch.await(ClusterFixture.TIMEOUT, SECONDS)

        then:
        true

        cleanup:
        cluster?.close()
    }

    def 'should select server asynchronously when server is already available'() {
        given:
        def cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .serverSelectionTimeout(serverSelectionTimeoutMS, MILLISECONDS)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)

        when:
        def serverDescription = selectServerAsync(cluster, firstServer).getDescription()

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
                        .serverSelectionTimeout(serverSelectionTimeoutMS, MILLISECONDS)
                        .hosts([firstServer, secondServer, thirdServer])
                        .build(),
                factory)

        when:
        def secondServerLatch = selectServerAsync(cluster, secondServer)
        def thirdServerLatch = selectServerAsync(cluster, thirdServer)
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
                        .serverSelectionTimeout(serverSelectionTimeoutMS, MILLISECONDS)
                        .build(),
                factory)

        when:
        selectServerAsyncAndGet(cluster, firstServer)

        then:
        thrown(MongoTimeoutException)

        cleanup:
        cluster?.close()


        where:
        serverSelectionTimeoutMS << [100, 0]
    }

    def selectServerAsyncAndGet(BaseCluster cluster, ServerAddress serverAddress) {
        selectServerAsync(cluster, serverAddress).get()
    }

    def selectServerAsync(BaseCluster cluster, ServerAddress serverAddress) {
        def serverLatch = new ServerLatch()
        cluster.selectServerAsync(new ServerAddressSelector(serverAddress)) { ServerTuple result, MongoException e ->
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
