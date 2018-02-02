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
        def builder = Fixture.mongoClientBuilderFromConnectionString
        builder.applyToClusterSettings { it.addClusterListener(clusterListener) }
                .applyToConnectionPoolSettings { it.addConnectionPoolListener(connectionPoolListener) }
                .applyToServerSettings { it.addServerListener(serverListener).addServerMonitorListener(serverMonitorListener) }
                .addCommandListener(commandListener)
        def client = MongoClients.create(builder.build())

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
