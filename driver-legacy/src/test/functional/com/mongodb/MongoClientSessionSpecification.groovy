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

import util.spock.annotations.Slow
import com.mongodb.event.CommandStartedEvent
import com.mongodb.internal.connection.TestCommandListener
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.Assert
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static Fixture.getDefaultDatabaseName
import static Fixture.getMongoClientURI
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getMongoClient

class MongoClientSessionSpecification extends FunctionalSpecification {

    def 'should throw IllegalArgumentException if options are null'() {
        when:
        getMongoClient().startSession(null)

        then:
        thrown(IllegalArgumentException)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should create session with correct defaults'() {
        given:
        def clientSession = getMongoClient().startSession()

        expect:
        clientSession.getOriginator() == getMongoClient()
        clientSession.isCausallyConsistent()
        clientSession.getOptions() == ClientSessionOptions.builder()
                .defaultTransactionOptions(TransactionOptions.builder()
                .readConcern(ReadConcern.DEFAULT)
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .readPreference(ReadPreference.primary())
                .build())
                .build()
        clientSession.getClusterTime() == null
        clientSession.getOperationTime() == null
        clientSession.getServerSession() != null

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6)  })
    def 'cluster time should advance'() {
        given:
        def firstOperationTime = new BsonTimestamp(42, 1)
        def secondOperationTime = new BsonTimestamp(52, 1)
        def thirdOperationTime = new BsonTimestamp(22, 1)
        def firstClusterTime = new BsonDocument('clusterTime', firstOperationTime)
        def secondClusterTime = new BsonDocument('clusterTime', secondOperationTime)
        def olderClusterTime = new BsonDocument('clusterTime', thirdOperationTime)

        when:
        def clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build())

        then:
        clientSession.getClusterTime() == null

        when:
        clientSession.advanceClusterTime(null)

        then:
        clientSession.getClusterTime() == null

        when:
        clientSession.advanceClusterTime(firstClusterTime)

        then:
        clientSession.getClusterTime() == firstClusterTime

        when:
        clientSession.advanceClusterTime(secondClusterTime)

        then:
        clientSession.getClusterTime() == secondClusterTime

        when:
        clientSession.advanceClusterTime(olderClusterTime)

        then:
        clientSession.getClusterTime() == secondClusterTime

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6)  })
    def 'operation time should advance'() {
        given:
        def firstOperationTime = new BsonTimestamp(42, 1)
        def secondOperationTime = new BsonTimestamp(52, 1)
        def olderOperationTime = new BsonTimestamp(22, 1)

        when:
        def clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build())

        then:
        clientSession.getOperationTime() == null

        when:
        clientSession.advanceOperationTime(null)

        then:
        clientSession.getOperationTime() == null

        when:
        clientSession.advanceOperationTime(firstOperationTime)

        then:
        clientSession.getOperationTime() == firstOperationTime

        when:
        clientSession.advanceOperationTime(secondOperationTime)

        then:
        clientSession.getOperationTime() == secondOperationTime

        when:
        clientSession.advanceOperationTime(olderOperationTime)

        then:
        clientSession.getOperationTime() == secondOperationTime

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'methods that use the session should throw if the session is closed'() {
        given:
        def options = ClientSessionOptions.builder().build()
        def clientSession = getMongoClient().startSession(options)
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

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'informational methods should not throw if the session is closed'() {
        given:
        def options = ClientSessionOptions.builder().build()
        def clientSession = getMongoClient().startSession(options)
        clientSession.close()

        when:
        clientSession.getOptions()
        clientSession.isCausallyConsistent()
        clientSession.getClusterTime()
        clientSession.getOperationTime()

        then:
        noExceptionThrown()

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should apply causally consistent session option to client session'() {
        when:
        def clientSession = getMongoClient().startSession(ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent)
                .build())

        then:
        clientSession != null
        clientSession.isCausallyConsistent() == causallyConsistent

        cleanup:
        clientSession?.close()

        where:
        causallyConsistent << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'client session should have server session with valid identifier'() {
        given:
        def clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build())

        when:
        def identifier = clientSession.getServerSession().identifier

        then:
        identifier.size() == 1
        identifier.containsKey('id')
        identifier.get('id').isBinary()
        identifier.getBinary('id').getType() == BsonBinarySubType.UUID_STANDARD.value
        identifier.getBinary('id').data.length == 16

        cleanup:
        clientSession?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should use a default session'() {
        given:
        def commandListener = new TestCommandListener()
        def optionsBuilder = MongoClientOptions.builder()
                .addCommandListener(commandListener)
        def client = new MongoClient(getMongoClientURI(optionsBuilder))

        when:
        client.getDatabase('admin').runCommand(new BsonDocument('ping', new BsonInt32(1)))

        then:
        commandListener.events.size() == 2
        def pingCommandStartedEvent = commandListener.events.get(0) as CommandStartedEvent
        pingCommandStartedEvent.command.containsKey('lsid')

        cleanup:
        client?.close()
    }

    // This test attempts attempts to demonstrate that causal consistency works correctly by inserting a document and then immediately
    // searching for that document on a secondary by its _id and failing the test if the document is not found.  Without causal consistency
    // enabled the expectation is that eventually that test would fail since generally the find will execute on the secondary before
    // the secondary has a chance to replicate the document.
    // This test is inherently racy as it's possible that the server _does_ replicate fast enough and therefore the test passes anyway
    // even if causal consistency was not actually in effect.  For that reason the test iterates a number of times in order to increase
    // confidence that it's really causal consistency that is causing the test to succeed
    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    @Slow
    def 'should find inserted document on a secondary when causal consistency is enabled'() {
        given:
        def collection = getMongoClient().getDatabase(getDefaultDatabaseName()).getCollection(getCollectionName())

        expect:
        def clientSession = getMongoClient().startSession(ClientSessionOptions.builder()
                .causallyConsistent(true)
                .build())
        try {
            for (int i = 0; i < 16; i++) {
                Document document = new Document('_id', i);
                collection.insertOne(clientSession, document)
                Document foundDocument = collection
                        .withReadPreference(ReadPreference.secondaryPreferred()) // read from secondary if available
                        .withReadConcern(readConcern)
                        .find(clientSession, document)
                        .maxTime(30, TimeUnit.SECONDS)  // to avoid the test running forever in case replication is broken
                        .first();
                if (foundDocument == null) {
                    Assert.fail('Should have found recently inserted document on secondary with causal consistency enabled');
                }
            }
        } finally {
            clientSession.close()
        }

        where:
        readConcern << [ReadConcern.DEFAULT, ReadConcern.LOCAL, ReadConcern.MAJORITY]
    }


    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should not use an implicit session for an unacknowledged write'() {
        given:
        def commandListener = new TestCommandListener()
        def optionsBuilder = MongoClientOptions.builder()
                .addCommandListener(commandListener)
        def mongoClientURI = getMongoClientURI(optionsBuilder)
        def client = new MongoClient(mongoClientURI)

        when:
        client.getDatabase(getDatabaseName()).getCollection(getCollectionName())
                .withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                .insertOne(new Document())

        then:
        def insertEvent = commandListener.events.get(0) as CommandStartedEvent
        !insertEvent.command.containsKey('lsid')

        cleanup:
        client?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should throw exception if unacknowledged write used with explicit session'() {
        given:
        def session = getMongoClient().startSession()

        when:
        getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName())
                .withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                .insertOne(session, new Document())

        then:
        thrown(MongoClientException)

        cleanup:
        session?.close()
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 0) || !isDiscoverableReplicaSet() })
    def 'should ignore unacknowledged write concern when in a transaction'() {
        given:
        def collection = getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName())
        collection.insertOne(new Document())

        def session = getMongoClient().startSession()
        session.startTransaction()

        when:
        collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                .insertOne(session, new Document())

        then:
        noExceptionThrown()

        cleanup:
        session.close()
    }
}
