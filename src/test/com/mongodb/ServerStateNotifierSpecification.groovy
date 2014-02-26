package com.mongodb

import static com.mongodb.Fixture.getMongoClient
import static com.mongodb.Fixture.serverIsAtLeastVersion
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class ServerStateNotifierSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerStateNotifier serverStateNotifier

    def setup() {
        serverStateNotifier = new ServerStateNotifier(new ServerAddress(),
                                                      new ChangeListener<ServerDescription>() {
                                                          @Override
                                                          void stateChanged(final ChangeEvent<ServerDescription> event) {
                                                              newDescription = event.newValue
                                                          }
                                                      },
                                                      SocketSettings.builder().build(), getMongoClient())
    }

    def cleanup() {
        serverStateNotifier.close();
    }

    def 'should set server version'() {
        given:
        CommandResult commandResult = database.command(new BasicDBObject('buildinfo', 1))
        def expectedVersion = new ServerVersion((commandResult.get('versionArray') as List<Integer>).subList(0, 3))

        when:
        serverStateNotifier.run()

        then:
        newDescription.version == expectedVersion
    }

    def 'should set max wire batch size when provided by server'() {
        assumeTrue(serverIsAtLeastVersion(2.5))

        given:
        CommandResult commandResult = database.command(new BasicDBObject('ismaster', 1))
        def expected = commandResult.get('maxWriteBatchSize')

        when:
        serverStateNotifier.run()

        then:
        newDescription.maxWriteBatchSize == expected
    }

    def 'should set default max wire batch size when not provided by server'() {
        assumeFalse(serverIsAtLeastVersion(2.5))

        when:
        serverStateNotifier.run()

        then:
        newDescription.maxWriteBatchSize == ServerDescription.getDefaultMaxWriteBatchSize()
    }
}