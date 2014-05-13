package com.mongodb

import java.util.concurrent.CountDownLatch

import static com.mongodb.Fixture.getMongoClient
import static com.mongodb.Fixture.serverIsAtLeastVersion
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class ServerMonitorSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverStateNotifier
    CountDownLatch latch = new CountDownLatch(1)

    def setup() {
        serverStateNotifier = new ServerMonitor(new ServerAddress(),
                                                new ChangeListener<ServerDescription>() {
                                                    @Override
                                                    void stateChanged(final ChangeEvent<ServerDescription> event) {
                                                        newDescription = event.newValue
                                                        latch.countDown()
                                                    }
                                                },
                                                SocketSettings.builder().build(), ServerSettings.builder().build(),
                                                'cluster-1', getMongoClient())
        serverStateNotifier.start()
    }

    def cleanup() {
        serverStateNotifier.close();
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
}