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


package org.mongodb.connection

import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.ReadPreference

import java.util.concurrent.CountDownLatch

import static java.util.Arrays.asList
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getCredentialList
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSSLSettings
import static org.mongodb.Fixture.serverVersionAtLeast

class ServerStateNotifierSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerMonitor serverStateNotifier
    CountDownLatch latch = new CountDownLatch(1)

    def setup() {
        serverStateNotifier = new ServerMonitor(getPrimary(), ServerSettings.builder().build(), 'cluster-1',
                                                new ChangeListener<ServerDescription>() {
                                                    @Override
                                                    void stateChanged(final ChangeEvent<ServerDescription> event) {
                                                        newDescription = event.newValue
                                                        latch.countDown()
                                                    }
                                                },
                                                new InternalStreamConnectionFactory('1',
                                                                                    new SocketStreamFactory(SocketSettings.builder()
                                                                                                                          .build(),
                                                                                                            getSSLSettings()),
                                                                                    getCredentialList(),
                                                                                    new NoOpConnectionListener())
        )
        serverStateNotifier.start()
    }

    def cleanup() {
        serverStateNotifier.close();
    }

    def 'should return server version'() {
        given:
        CommandResult commandResult = database.executeCommand(new Document('buildinfo', 1), ReadPreference.primary())
        def expectedVersion = new ServerVersion(commandResult.getResponse().getArray('versionArray')
                                                             .subList(0, 3)*.getValue() as List<Integer>)

        when:
        latch.await()

        then:
        newDescription.version == expectedVersion

        cleanup:
        serverStateNotifier.close()
    }

    def 'should set max wire batch size when provided by server'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 5)))

        given:
        CommandResult commandResult = database.executeCommand(new Document('ismaster', 1), ReadPreference.primary())
        def expected = commandResult.response.getInt32('maxWriteBatchSize').getValue()

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == expected
    }

    def 'should set default max wire batch size when not provided by server'() {
        assumeFalse(serverVersionAtLeast(asList(2, 5, 5)))

        when:
        latch.await()

        then:
        newDescription.maxWriteBatchSize == ServerDescription.getDefaultMaxWriteBatchSize()
    }

}