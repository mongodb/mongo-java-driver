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

package com.mongodb

import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import org.bson.Document

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.Fixture.getMongoClientURI
import static com.mongodb.Fixture.mongoClientURI

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
        def optionBuilder = MongoClientOptions.builder(mongoClientURI.options)
                .addClusterListener(clusterListener)
                .addCommandListener(commandListener)
                .addConnectionPoolListener(connectionPoolListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
        def client = new MongoClient(getMongoClientURI(optionBuilder))

        then:
        client.getDatabase('admin').runCommand(new Document('ping', 1))
    }

    def 'should register single command listener'() {
        given:
        def first = Mock(CommandListener)
        def optionsBuilder = MongoClientOptions.builder(mongoClientURI.options)
                .addCommandListener(first)
        def client = new MongoClient(getMongoClientURI(optionsBuilder))

        when:
        client.getDatabase('admin').runCommand(new Document('ping', 1))

        then:
        1 * first.commandStarted(_)
        1 * first.commandSucceeded(_)
    }

    def 'should register multiple command listeners'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def optionsBuilder = MongoClientOptions.builder(mongoClientURI.options)
                .addCommandListener(first)
                .addCommandListener(second)
        def client = new MongoClient(getMongoClientURI(optionsBuilder));

        when:
        client.getDatabase('admin').runCommand(new Document('ping', 1))

        then:
        1 * first.commandStarted(_)
        1 * second.commandStarted(_)
        1 * first.commandSucceeded(_)
        1 * second.commandSucceeded(_)
    }

    def 'should register single listeners for monitor events'() {
        given:
        def latch = new CountDownLatch(1)
        def clusterListener = Mock(ClusterListener) {
           1 * clusterOpening(_)
        }
        def serverListener = Mock(ServerListener) {
            (1.._) * serverOpening(_)
        }
        def serverMonitorListener = Mock(ServerMonitorListener){
            (1.._) * serverHearbeatStarted(_) >> {
                if (latch.count > 0) {
                    latch.countDown()
                }
            }
        }

        def optionsBuilder = MongoClientOptions.builder(mongoClientURI.options)
                .addClusterListener(clusterListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
        def client = new MongoClient(getMongoClientURI(optionsBuilder));

        when:
        def finished = latch.await(5, TimeUnit.SECONDS)

        then:
        finished

        cleanup:
        client?.close()
    }

    def 'should register multiple listeners for monitor events'() {
        given:
        def latch = new CountDownLatch(2)
        def clusterListener = Mock(ClusterListener) {
            1 * clusterOpening(_)
        }
        def serverListener = Mock(ServerListener) {
            (1.._) * serverOpening(_)
        }
        def serverMonitorListener = Mock(ServerMonitorListener){
            (1.._) * serverHearbeatStarted(_) >> {
                if (latch.count > 0) {
                    latch.countDown()
                }
            }
        }
        def clusterListenerTwo = Mock(ClusterListener) {
            1 * clusterOpening(_)
        }
        def serverListenerTwo = Mock(ServerListener) {
            (1.._) * serverOpening(_)
        }
        def serverMonitorListenerTwo = Mock(ServerMonitorListener){
            (1.._) * serverHearbeatStarted(_) >> {
                if (latch.count > 0) {
                    latch.countDown()
                }
            }
        }

        def optionsBuilder = MongoClientOptions.builder(mongoClientURI.options)
                .addClusterListener(clusterListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .addClusterListener(clusterListenerTwo)
                .addServerListener(serverListenerTwo)
                .addServerMonitorListener(serverMonitorListenerTwo)
        def client = new MongoClient(getMongoClientURI(optionsBuilder));

        when:
        def finished = latch.await(5, TimeUnit.SECONDS)

        then:
        finished

        cleanup:
        client?.close()
    }
}
