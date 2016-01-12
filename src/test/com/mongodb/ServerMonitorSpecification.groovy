package com.mongodb

import spock.lang.IgnoreIf

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.Fixture.getMongoClient
import static com.mongodb.Fixture.serverIsAtLeastVersion
import static com.mongodb.ServerMonitor.exceptionHasChanged
import static com.mongodb.ServerMonitor.stateHasChanged

class ServerMonitorSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverMonitor
    CountDownLatch latch = new CountDownLatch(1)

    def cleanup() {
        serverMonitor?.close();
    }

    def 'should set server version'() {
        given:
        initializeServerMonitor(new ServerAddress())
        CommandResult commandResult = database.command(new BasicDBObject('buildinfo', 1))
        def expectedVersion = new ServerVersion((commandResult.get('versionArray') as List<Integer>).subList(0, 3))

        when:
        latch.await()

        then:
        newDescription.version == expectedVersion
    }

    def 'should set canonicalAddress'() {
        given:
        initializeServerMonitor(new ServerAddress())
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expectedCanonicalAddress = commandResult.getString('me')

        when:
        latch.await()

        then:
        newDescription.canonicalAddress == expectedCanonicalAddress
    }

    @IgnoreIf( { !serverIsAtLeastVersion(2.6) } )
    def 'should set max wire batch size when provided by server'() {
        given:
        initializeServerMonitor(new ServerAddress())
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expected = commandResult.get('maxWriteBatchSize')

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == expected
    }

    def 'should set electionId'() {
        given:
        initializeServerMonitor(new ServerAddress())
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expected = commandResult.get('electionId')

        when:
        latch.await()

        then:
        newDescription.electionId == expected
    }

    def 'should set setVersion'() {
        given:
        initializeServerMonitor(new ServerAddress())
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expected = commandResult.get('setVersion')

        when:
        latch.await()

        then:
        newDescription.setVersion == expected
    }

    @IgnoreIf( { serverIsAtLeastVersion(2.6) } )
    def 'should set default max wire batch size when not provided by server'() {
        given:
        initializeServerMonitor(new ServerAddress())

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == ServerDescription.getDefaultMaxWriteBatchSize()
    }

    def 'should report current exception'() {
        given:
        initializeServerMonitor(new ServerAddress('some_unknown_server_name:34567'))

        when:
        latch.await()

        then:
        newDescription.exception instanceof MongoSocketException
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

    def 'should report correct server type'() {
        given:
        CommandResult isMasterCommandResult = new CommandResult(new ServerAddress())
        isMasterCommandResult.putAll(response)

        expect:
        ServerMonitor.getServerType(isMasterCommandResult) == serverType

        where:
        response << [
                new BasicDBObject('ok', 0),
                new BasicDBObject('ok', 1)
                        .append('setName', 'test')
                        .append('hidden', true)
                        .append('secondary', true),
                new BasicDBObject('ok', 1)
                        .append('setName', 'test')
                        .append('ismaster', true),
                new BasicDBObject('ok', 1)
                        .append('setName', 'test')
                        .append('secondary', true),
                new BasicDBObject('ok', 1)
                        .append('setName', 'test')
                        .append('arbiterOnly', true),
                new BasicDBObject('ok', 1)
                        .append('setName', 'test')
                        .append('hosts', ['server1:27017']),
                new BasicDBObject('ok', 1)
                        .append('isreplicaset', true),
                new BasicDBObject('ok', 1),
                new BasicDBObject('ok', 1)
                        .append('msg', 'isdbgrid'),
                new BasicDBObject('ok', 1)
                        .append('msg', 'whatever')
        ]
        serverType << [
                ServerType.Unknown,
                ServerType.ReplicaSetOther,
                ServerType.ReplicaSetPrimary,
                ServerType.ReplicaSetSecondary,
                ServerType.ReplicaSetArbiter,
                ServerType.ReplicaSetOther,
                ServerType.ReplicaSetGhost,
                ServerType.StandAlone,
                ServerType.ShardRouter,
                ServerType.StandAlone
        ]
    }

    def initializeServerMonitor(ServerAddress address) {
        def options = new MongoOptions()
        options.connectTimeout = 1000
        def connectionProvider = new PooledConnectionProvider('cluster-1', address, new DBPortFactory(getMongoClient()),
                                                              ConnectionPoolSettings.builder().maxSize(1).build(),
                                                              new JMXConnectionPoolListener());
        serverMonitor = new ServerMonitor(address,
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
}
