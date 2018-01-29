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

package com.mongodb.async.client

import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import org.bson.Document

import static java.util.concurrent.TimeUnit.SECONDS

class MongoClientListenerRegistrationSpecification extends FunctionalSpecification {

    def 'should register event listeners'() {
        given:
        def clusterListener = Mock(ClusterListener) {
            (1.._) * _
        }
        def commandListener = Mock(CommandListener) {
            (1.._) * _
        }
        def connectionPoolListener = Mock(ConnectionPoolListener) {
            (1.._) * _
        }
        def serverListener = Mock(ServerListener) {
            (1.._) * _
        }
        def serverMonitorListener = Mock(ServerMonitorListener) {
            (1.._) * _
        }

        when:
        def defaultSettings = Fixture.mongoClientBuilderFromConnectionString.build()

        def clusterSettings = ClusterSettings.builder(defaultSettings.getClusterSettings()).addClusterListener(clusterListener).build()
        def connectionPoolSettings = ConnectionPoolSettings.builder(defaultSettings.getConnectionPoolSettings())
                .addConnectionPoolListener(connectionPoolListener).build()
        def serverSettings = ServerSettings.builder(defaultSettings.getServerSettings()).addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener).build()
        def clientSettings = MongoClientSettings.builder()
                .clusterSettings(clusterSettings)
                .connectionPoolSettings(connectionPoolSettings)
                .serverSettings(serverSettings)
                .credentialList(defaultSettings.getCredentialList())
                .sslSettings(defaultSettings.getSslSettings())
                .socketSettings(defaultSettings.getSocketSettings())
                .addCommandListener(commandListener)
                .build()

        def client = MongoClients.create(clientSettings)

        then:
        run(client.getDatabase('admin').&runCommand, new Document('ping', 1))
    }

    def 'should register multiple command listeners'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def client =  MongoClients.create(Fixture.mongoClientBuilderFromConnectionString
                .addCommandListener(first).addCommandListener(second).build())

        when:
        run(client.getDatabase('admin').&runCommand, new Document('ping', 1))

        then:
        1 * first.commandStarted(_)
        1 * second.commandStarted(_)
        1 * first.commandSucceeded(_)
        1 * second.commandSucceeded(_)
    }

    def run(operation, ... args) {
        def futureResultCallback = new FutureResultCallback()
        def opArgs = (args != null) ? args : []
        operation.call(*opArgs + futureResultCallback)
        futureResultCallback.get(60, SECONDS)
    }

}
