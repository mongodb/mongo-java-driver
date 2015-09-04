/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.event.CommandListener
import org.bson.Document

import static com.mongodb.Fixture.mongoClientURI

class MongoClientListenerRegistrationSpecification extends FunctionalSpecification {

    def 'should register single command listener'() {
        given:
        def first = Mock(CommandListener)
        def client = new MongoClient(mongoClientURI.getHosts().collect { new ServerAddress(it) },
                MongoClientOptions.builder(mongoClientURI.options)
                                                       .addCommandListener(first)
                                                       .build());

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
        def client = new MongoClient(mongoClientURI.getHosts().collect { new ServerAddress(it) },
                                     MongoClientOptions.builder(mongoClientURI.options)
                                                       .addCommandListener(first)
                                                       .addCommandListener(second).build());

        when:
        client.getDatabase('admin').runCommand(new Document('ping', 1))

        then:
        1 * first.commandStarted(_)
        1 * second.commandStarted(_)
        1 * first.commandSucceeded(_)
        1 * second.commandSucceeded(_)
    }
}
