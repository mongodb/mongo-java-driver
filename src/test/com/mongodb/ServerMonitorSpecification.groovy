package com.mongodb

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.Fixture.getMongoClient
import static com.mongodb.Fixture.serverIsAtLeastVersion
import static com.mongodb.ServerMonitor.exceptionHasChanged
import static com.mongodb.ServerMonitor.stateHasChanged
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class ServerMonitorSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverMonitor
    CountDownLatch latch = new CountDownLatch(1)

    def setup() {
        def connectionProvider = new PooledConnectionProvider('cluster-1', new ServerAddress(), new DBPortFactory(new MongoOptions()),
                                                              ConnectionPoolSettings.builder().maxSize(1).build(),
                                                              new JMXConnectionPoolListener());
        serverMonitor = new ServerMonitor(new ServerAddress(),
                                          new ChangeListener<ServerDescription>() {
                                              @Override
                                              void stateChanged(final ChangeEvent<ServerDescription> event) {
                                                  newDescription = event.newValue
                                                  latch.countDown()
                                              }
                                          },
                                          SocketSettings.builder().build(), ServerSettings.builder().build(),
                                          'cluster-1', getMongoClient(), connectionProvider)
        serverMonitor.start()
    }

    def cleanup() {
        serverMonitor.close();
    }

    def 'should set server version'() {
        given:
        CommandResult commandResult = database.command(new BasicDBObject('buildinfo', 1))
        def expectedVersion = new ServerVersion((commandResult.get('versionArray') as List<Integer>).subList(0, 3))

        when:
        latch.await()

        then:
        newDescription.version == expectedVersion
    }

    def 'should set max wire batch size when provided by server'() {
        assumeTrue(serverIsAtLeastVersion(2.5))

        given:
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expected = commandResult.get('maxWriteBatchSize')

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == expected
    }

    def 'should set default max wire batch size when not provided by server'() {
        assumeFalse(serverIsAtLeastVersion(2.5))

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == ServerDescription.getDefaultMaxWriteBatchSize()
    }

    def 'should report exception has changed when the current and previous are different'() {
        expect:
        exceptionHasChanged(null, new NullPointerException())
        exceptionHasChanged(new NullPointerException(), null)
        exceptionHasChanged(new SocketException(), new SocketException('A message'))
        exceptionHasChanged(new SocketException('A message'), new SocketException())
        exceptionHasChanged(new SocketException('A message'), new MongoException('A message'))
        exceptionHasChanged(new SocketException('A message'), new SocketException('A different message'))
    }

    def 'should report exception has not changed when the current and previous are the same'() {
        expect:
        !exceptionHasChanged(null, null)
        !exceptionHasChanged(new NullPointerException(), new NullPointerException())
        !exceptionHasChanged(new MongoException('A message'), new MongoException('A message'))
    }

    def 'should report state has changed if descriptions are different'() {
        expect:
        stateHasChanged(ServerDescription.builder()
                                         .type(ServerType.Unknown)
                                         .state(ServerConnectionState.Connecting)
                                         .address(new ServerAddress())
                                         .build(),
                        ServerDescription.builder()
                                         .type(ServerType.StandAlone)
                                         .state(ServerConnectionState.Connected)
                                         .address(new ServerAddress())
                                         .averageLatency(5, TimeUnit.MILLISECONDS)
                                         .build());
    }

    def 'should report state has changed if latencies are different'() {
        expect:
        stateHasChanged(ServerDescription.builder()
                                         .type(ServerType.StandAlone)
                                         .state(ServerConnectionState.Connected)
                                         .address(new ServerAddress())
                                         .averageLatency(5, TimeUnit.MILLISECONDS)
                                         .build(),
                        ServerDescription.builder()
                                         .type(ServerType.StandAlone)
                                         .state(ServerConnectionState.Connected)
                                         .address(new ServerAddress())
                                         .averageLatency(6, TimeUnit.MILLISECONDS)
                                         .build());
    }

    def 'should report state has not changed if descriptions and latencies are the same'() {
        expect:
        !stateHasChanged(ServerDescription.builder()
                                          .type(ServerType.StandAlone)
                                          .state(ServerConnectionState.Connected)
                                          .address(new ServerAddress())
                                          .averageLatency(5, TimeUnit.MILLISECONDS)
                                          .build(),
                         ServerDescription.builder()
                                          .type(ServerType.StandAlone)
                                          .state(ServerConnectionState.Connected)
                                          .address(new ServerAddress())
                                          .averageLatency(5, TimeUnit.MILLISECONDS)
                                          .build());
    }
}
