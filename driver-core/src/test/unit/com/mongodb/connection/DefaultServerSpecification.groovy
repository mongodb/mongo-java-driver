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

package com.mongodb.connection

import com.mongodb.MongoSecurityException
import com.mongodb.ServerAddress
import spock.lang.Specification

import static com.mongodb.MongoCredential.createCredential

class DefaultServerSpecification extends Specification {

    DefaultServer server;

    def 'invalidate should invoke change listeners'() {
        given:
        server = new DefaultServer(new ServerAddress(), new TestConnectionPool(), new TestServerMonitorFactory())
        def stateChanged = false;

        server.addChangeListener(new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
            }
        })

        when:
        server.invalidate();

        then:
        stateChanged

        cleanup:
        server?.close()
    }

    def 'failed open should invalidate the server'() {
        given:
        def connectionPool  = Mock(ConnectionPool)
        def serverMonitorFactory = Stub(ServerMonitorFactory)
        def serverMonitor = Mock(ServerMonitor)
        connectionPool.get() >> { throw new MongoSecurityException(createCredential('jeff', 'admin', '123'.toCharArray()), 'Auth failed') }
        serverMonitorFactory.create(_) >> { serverMonitor }

        server = new DefaultServer(new ServerAddress(), connectionPool, serverMonitorFactory)

        when:
        server.getConnection()

        then:
        thrown(MongoSecurityException)
        1 * connectionPool.invalidate()
        1 * serverMonitor.invalidate()
    }
}
