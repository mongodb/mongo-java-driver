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

import com.mongodb.MongoDriverInformation
import com.mongodb.MongoNamespace
import com.mongodb.client.model.IndexModel
import com.mongodb.client.result.InsertOneResult
import com.mongodb.internal.diagnostics.logging.Loggers
import org.bson.BsonInt32
import org.bson.Document
import org.bson.RawBsonDocument
import reactor.core.publisher.Flux
import spock.lang.IgnoreIf

import static Fixture.getMongoClient
import static com.mongodb.ClusterFixture.TIMEOUT_DURATION
import static com.mongodb.ClusterFixture.getConnectionString
import static com.mongodb.reactivestreams.client.Fixture.isReplicaSet
import static com.mongodb.reactivestreams.client.Fixture.serverVersionAtLeast

class SmokeTestSpecification extends FunctionalSpecification {

    private static final LOGGER = Loggers.getLogger('smokeTest')

    def 'should handle common scenarios without error'() {
        given:
        def mongoClient = getMongoClient()
        def database = mongoClient.getDatabase(databaseName)
        def document = new Document('_id', 1)
        def updatedDocument = new Document('_id', 1).append('a', 1)

        when:
        run('clean up old database', mongoClient.getDatabase(databaseName).&drop)
        def names = run('get database names', mongoClient.&listDatabaseNames)

        then: 'Get Database Names'
        !names.contains(null)

        then:
        run('Create a collection and the created database is in the list', database.&createCollection, collectionName) == []

        when:
        def updatedNames = run('get database names', mongoClient.&listDatabaseNames)

        then: 'The database names should contain the database and be one bigger than before'
        updatedNames.contains(databaseName)
        updatedNames.size() == names.size() + 1

        when:
        def collectionNames = run('The collection name should be in the collection names list', database.&listCollectionNames)

        then:
        !collectionNames.contains(null)
        collectionNames.contains(collectionName)

        then:
        run('The count is zero', collection.&countDocuments)[0] == 0

        then:
        run('find first should return nothing if no documents', collection.find().&first) == []

        then:
        run('find should return an empty list', collection.&find) == []

        then:
        run('Insert a document', collection.&insertOne, document)[0] == InsertOneResult.acknowledged(new BsonInt32(1))

        then:
        run('The count is one', collection.&countDocuments)[0] == 1

        then:
        run('find that document', collection.find().&first)[0] == document

        then:
        run('update that document', collection.&updateOne, document, new Document('$set', new Document('a', 1)))[0].wasAcknowledged()

        then:
        run('find the updated document', collection.find().&first)[0] == updatedDocument

        then:
        run('aggregate the collection', collection.&aggregate, [new Document('$match', new Document('a', 1))])[0] == updatedDocument

        then:
        run('remove all documents', collection.&deleteOne, new Document())[0].getDeletedCount() == 1

        then:
        run('The count is zero', collection.&countDocuments)[0] == 0

        then:
        run('create an index', collection.&createIndex, new Document('test', 1))[0] == 'test_1'

        then:
        def indexNames = run('has the newly created index', collection.&listIndexes)*.name

        then:
        indexNames.containsAll('_id_', 'test_1')

        then:
        run('create multiple indexes', collection.&createIndexes, [new IndexModel(new Document('multi', 1))])[0] == 'multi_1'

        then:
        def indexNamesUpdated = run('has the newly created index', collection.&listIndexes)*.name

        then:
        indexNamesUpdated.containsAll('_id_', 'test_1', 'multi_1')

        then:
        run('drop the index', collection.&dropIndex, 'multi_1') == []

        then:
        run('has a single index left "_id" ', collection.&listIndexes).size() == 2

        then:
        run('drop the index', collection.&dropIndex, 'test_1') == []

        then:
        run('has a single index left "_id" ', collection.&listIndexes).size() == 1

        then:
        def newCollectionName = 'new' + collectionName.capitalize()
        run('can rename the collection', collection.&renameCollection, new MongoNamespace(databaseName, newCollectionName)) == []

        then:
        !run('the new collection name is in the collection names list', database.&listCollectionNames).contains(collectionName)
        run('get collection names', database.&listCollectionNames).contains(newCollectionName)

        when:
        collection = database.getCollection(newCollectionName)

        then:
        run('drop the collection', collection.&drop) == []

        then:
        run('there are no indexes', collection.&listIndexes).size() == 0

        then:
        !run('the collection name is no longer in the collectionNames list', database.&listCollectionNames).contains(collectionName)
    }

    @IgnoreIf({ !(serverVersionAtLeast(4, 0) && isReplicaSet()) })
    def 'should commit a transaction'() {
        given:
        run('create collection', database.&createCollection, collection.namespace.collectionName)

        when:
        ClientSession session = run('start a session', getMongoClient().&startSession)[0] as ClientSession
        session.startTransaction()
        run('insert a document', collection.&insertOne, session, new Document('_id', 1))
        run('commit a transaction', session.&commitTransaction)

        then:
        run('The count is one', collection.&countDocuments)[0] == 1

        cleanup:
        session?.close()
    }

    @IgnoreIf({ !(serverVersionAtLeast(4, 0) && isReplicaSet()) })
    def 'should abort a transaction'() {
        given:
        run('create collection', database.&createCollection, collection.namespace.collectionName)

        when:
        ClientSession session = run('start a session', getMongoClient().&startSession)[0] as ClientSession
        session.startTransaction()
        run('insert a document', collection.&insertOne, session, new Document('_id', 1))
        run('abort a transaction', session.&abortTransaction)

        then:
        run('The count is zero', collection.&countDocuments)[0] == 0

        cleanup:
        session?.close()
    }

    def 'should not leak exceptions when a client is closed'() {
        given:
        def mongoClient = MongoClients.create(getConnectionString())

        when:
        mongoClient.close()
        run('get database names', mongoClient.&listDatabaseNames)

        then:
        thrown(IllegalStateException)
    }

    def 'should accept custom MongoDriverInformation'() {
        when:
        def driverInformation = MongoDriverInformation.builder().driverName('test').driverVersion('1.2.0').build()

        then:
        def client = MongoClients.create(getConnectionString(), driverInformation)

        cleanup:
        client?.close()
    }

    @SuppressWarnings('BusyWait')
    def 'should visit all documents from a cursor with multiple batches'() {
        given:
        def total = 1000
        def documents = (1..total).collect { new Document('_id', it) }
        run('Insert 10000 documents', collection.&insertMany, documents)

        when:
        def counted = Flux.from(collection.find(new Document()).sort(new Document('_id', 1)).batchSize(10))
                .collectList().block(TIMEOUT_DURATION).size()

        then:
        counted == documents.size()
    }

    def 'should bulk insert RawBsonDocuments'() {
        given:
        def docs = [RawBsonDocument.parse('{a: 1}'), RawBsonDocument.parse('{a: 2}')]

        when:
        def result = run('Insert RawBsonDocuments', collection.withDocumentClass(RawBsonDocument).&insertMany, docs)

        then:
        result.insertedIds.head() == [0:null, 1:null]
    }

    def run(String log, operation, ... args) {
        LOGGER.debug(log)
        Flux.from(operation.call(args)).collectList().block(TIMEOUT_DURATION)
    }

}
