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

package com.mongodb.reactivestreams.client

import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import org.bson.Document
import reactor.core.publisher.Mono
import spock.lang.Ignore

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION

class MongoClientListenerRegistrationSpecification extends FunctionalSpecification {

    @Ignore
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
                .applyToServerSettings {
                    it.addServerListener(serverListener)
                    it.heartbeatFrequency(1, TimeUnit.MILLISECONDS)
                    it.addServerMonitorListener(serverMonitorListener)
                }
                .addCommandListener(commandListener)
        def settings = builder.build()
        def client = MongoClients.create(settings)

        then:
        Mono.from(client.getDatabase('admin').runCommand(new Document('ping', 1))).block(TIMEOUT_DURATION)

        cleanup:
        client?.close()
    }

    def 'should register multiple command listeners'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def client =  MongoClients.create(Fixture.mongoClientBuilderFromConnectionString
                .addCommandListener(first).addCommandListener(second).build())

        when:
        Mono.from(client.getDatabase('admin').runCommand(new Document('ping', 1))).block(TIMEOUT_DURATION)

        then:
        1 * first.commandStarted(_)
        1 * second.commandStarted(_)
        1 * first.commandSucceeded(_)
        1 * second.commandSucceeded(_)

        cleanup:
        client?.close()
    }

}
