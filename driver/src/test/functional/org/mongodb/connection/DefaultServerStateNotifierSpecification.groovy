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

import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.FunctionalSpecification

import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getCredentialList
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSSLSettings

class DefaultServerStateNotifierSpecification extends FunctionalSpecification {
    ServerDescription newDescription
    ServerStateNotifier serverStateNotifier

    def setup() {
        serverStateNotifier = new ServerStateNotifier(getPrimary(),
                new ChangeListener<ServerDescription>() {
                    @Override
                    void stateChanged(final ChangeEvent<ServerDescription> event) {
                        newDescription = event.newValue
                    }
                },
                new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                        getBufferProvider(), getCredentialList(), new NoOpConnectionListener()), getBufferProvider())
    }

    def cleanup() {
        serverStateNotifier.close();
    }

    def 'should return server version'() {
        given:
        CommandResult commandResult = database.executeCommand(new Document('buildinfo', 1), null)
        def expectedVersion = new ServerVersion((commandResult.getResponse().get('versionArray') as List<Integer>).subList(0, 3))

        when:
        serverStateNotifier.run()

        then:
        newDescription.version == expectedVersion

        cleanup:
        serverStateNotifier.close()
    }
}