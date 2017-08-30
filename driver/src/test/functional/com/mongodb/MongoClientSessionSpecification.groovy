/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.connection.TestCommandListener
import com.mongodb.event.CommandStartedEvent
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getMongoClientURI

class MongoClientSessionSpecification extends FunctionalSpecification {

    @IgnoreIf({ serverVersionAtLeast(3, 5) })
    def 'should throw MongoClientException starting a session when sessions are not supported'() {
        when:
        Fixture.getMongoClient().startSession(ClientSessionOptions.builder().build())

        then:
        thrown(MongoClientException)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'should create session with correct defaults'() {
        when:
        def options = ClientSessionOptions.builder().build()
        def clientSession = Fixture.getMongoClient().startSession(options)

        then:
        clientSession != null
        clientSession.getMongoClient() == Fixture.getMongoClient()
        !clientSession.isCausallyConsistent()
        clientSession.getOptions() == options
        clientSession.getClusterTime() == null
        clientSession.getOperationTime() == null
        clientSession.getServerSession() != null
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'methods that use the session should throw if the session is closed'() {
        given:
        def options = ClientSessionOptions.builder().build()
        def clientSession = Fixture.getMongoClient().startSession(options)
        clientSession.close()

        when:
        clientSession.getServerSession()

        then:
        thrown(IllegalStateException)

        when:
        clientSession.advanceOperationTime(new BsonTimestamp(42, 0))

        then:
        thrown(IllegalStateException)

        when:
        clientSession.advanceClusterTime(new BsonDocument())

        then:
        thrown(IllegalStateException)

        when:
        clientSession.getMongoClient()

        then:
        thrown(IllegalStateException)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'informational methods should not throw if the session is closed'() {
        given:
        def options = ClientSessionOptions.builder().build()
        def clientSession = Fixture.getMongoClient().startSession(options)
        clientSession.close()

        when:
        clientSession.getOptions()
        clientSession.isCausallyConsistent()
        clientSession.getClusterTime()
        clientSession.getOperationTime()

        then:
        true
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'should apply causally consistent session option to client session'() {
        when:
        def clientSession = Fixture.getMongoClient().startSession(ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent)
                .initialClusterTime(initialClusterTime)
                .initialOperationTime(initialOperationTime)
                .build())

        then:
        clientSession != null
        clientSession.isCausallyConsistent() == causallyConsistent
        clientSession.getClusterTime() == initialClusterTime
        clientSession.getOperationTime() == initialOperationTime

        where:
        [causallyConsistent, initialClusterTime, initialOperationTime] << [
                [true, false],
                [null, new BsonDocument('x', new BsonInt32(1))],
                [null, new BsonTimestamp(42, 1)]
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'client session should have server session with valid identifier'() {
        given:
        def clientSession = Fixture.getMongoClient().startSession(ClientSessionOptions.builder().build())

        when:
        def identifier = clientSession.getServerSession().identifier

        then:
        identifier.size() == 1
        identifier.containsKey('id')
        identifier.get('id').isBinary()
        identifier.getBinary('id').getType() == BsonBinarySubType.UUID_STANDARD.value
        identifier.getBinary('id').data.length == 16
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 5) })
    def 'should use a default session'() {
        given:
        def commandListener = new TestCommandListener()
        def optionsBuilder = MongoClientOptions.builder()
                .addCommandListener(commandListener)
        def client = new MongoClient(getMongoClientURI(optionsBuilder))

        when:
        client.getDatabase('admin').runCommand(new BsonDocument('ping', new BsonInt32(1)))

        then:
        def pingCommandStartedEvent = commandListener.events.get(0)
        (pingCommandStartedEvent as CommandStartedEvent).command.containsKey('lsid')
    }

    @IgnoreIf({ serverVersionAtLeast(3, 5) })
    def 'should not use a default session when sessions are not supported'() {
        given:
        def commandListener = new TestCommandListener()
        def optionsBuilder = MongoClientOptions.builder()
                .addCommandListener(commandListener)
        def client = new MongoClient(getMongoClientURI(optionsBuilder))

        when:
        client.getDatabase('admin').runCommand(new BsonDocument('ping', new BsonInt32(1)))

        then:
        def pingCommandStartedEvent = commandListener.events.get(0)
        !(pingCommandStartedEvent as CommandStartedEvent).command.containsKey('lsid')
    }
}
