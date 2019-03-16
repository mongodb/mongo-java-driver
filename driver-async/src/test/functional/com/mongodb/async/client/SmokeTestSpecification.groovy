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

import com.mongodb.MongoNamespace
import org.bson.Document
import spock.lang.IgnoreIf

import static com.mongodb.async.client.Fixture.getMongoClient
import static com.mongodb.async.client.Fixture.isSharded
import static com.mongodb.async.client.TestHelper.run

class SmokeTestSpecification extends FunctionalSpecification {

    def 'should handle common CRUD scenarios without error'() {
        given:
        def mongoClient = getMongoClient()
        def database = mongoClient.getDatabase(databaseName)
        def collection = database.getCollection(collectionName)

        when:
        def document = new Document('_id', 1)
        def updatedDocument = new Document('_id', 1).append('a', 1)

        then: 'The count is zero'
        run(collection.&countDocuments) == 0

        then: 'find first should return null if no documents'
        run(collection.find().&first) == null

        then: 'Insert a document'
        run(collection.&insertOne, document) == null

        then: 'The count is one'
        run(collection.&countDocuments) == 1

        then: 'find that document'
        run(collection.find().&first) == document

        then: 'update that document'
        run(collection.&updateOne, document, new Document('$set', new Document('a', 1))).wasAcknowledged()

        then: 'find the updated document'
        run(collection.find().&first) == updatedDocument

        then: 'aggregate the collection'
        run(collection.aggregate([new Document('$match', new Document('a', 1))]).&first) == updatedDocument

        then: 'remove all documents'
        run(collection.&deleteOne, new Document()).getDeletedCount() == 1

        then: 'The count is zero'
        run(collection.&countDocuments) == 0

        then: 'InsertMany documents'
        run(collection.&insertMany, [new Document('id', 'a'), new Document('id', 'b'), new Document('id', 'c')]) == null

        then: 'Distinct'
        run(collection.distinct('id', String).&into, []) == ['a', 'b', 'c']
    }

    def 'should aggregate to collection'() {
        given:
        def mongoClient = getMongoClient()
        def database = mongoClient.getDatabase(databaseName)
        def collection = database.getCollection(collectionName)

        when:
        def document = new Document('_id', 1).append('a', 1)
        run(collection.&insertOne, document)

        then: 'aggregate the collection to a collection'
        run(collection.aggregate([new Document('$match', new Document('a', 1)), new Document('$out', getClass().getName() + '.out')])
                      .&first) == document
    }

    def 'should handle common administrative scenarios without error'() {
        given:
        def mongoClient = getMongoClient()
        def database = mongoClient.getDatabase(databaseName)
        def collection = database.getCollection(collectionName)

        when: 'clean up old database'
        run(mongoClient.getDatabase(databaseName).&drop) == null
        def names = run(mongoClient.listDatabaseNames().&into, [])

        then: 'Get Database Names'
        !names.contains(null)

        when: 'Create a collection and the created database is in the list'
        run(database.&createCollection, collectionName)
        def updatedNames = run(mongoClient.listDatabaseNames().&into, [])

        then: 'The database names should contain the database and be one bigger than before'
        updatedNames.contains(databaseName)
        updatedNames.size() == names.size() + 1

        when: 'Calling listDatabaseNames batchSize should be ignored'
        def batchCursor = run(mongoClient.listDatabaseNames().batchSize(100).&batchCursor)

        then:
        batchCursor.getBatchSize() == 0
        batchCursor.close()

        when: 'The collection name should be in the collections list'
        def collectionNames = run(database.listCollections().&into, [])

        then:
        collectionNames*.name.contains(collectionName)

        when: 'The collections list should be filterable'
        collectionNames = run(database.listCollections().filter(new Document('name', collectionName)).&into, [])

        then:
        collectionNames*.name == [collectionName]

        when: 'The collection name should be in the collection names list'
        collectionNames = run(database.listCollectionNames().&into, [])

        then:
        !collectionNames.contains(null)
        collectionNames.contains(collectionName)

        then: 'create an index'
        run(collection.&createIndex, new Document('test', 1)) == 'test_1'

        then: 'has the newly created index'
        run(collection.listIndexes().&into, [])*.name.containsAll('_id_', 'test_1')

        then: 'drop the index'
        run(collection.&dropIndex, 'test_1') == null

        then: 'has a single index left "_id" '
        run(collection.listIndexes().&into, []).size == 1

        then: 'drop the collection'
        run(collection.&drop) == null

        then: 'there are no indexes'
        run(collection.listIndexes().&into, []).size == 0

        then: 'the collection name is no longer in the collectionNames list'
        !run(database.listCollectionNames().&into, []).contains(collectionName)
    }

    @IgnoreIf({ isSharded() })   // see JAVA-1757 for why sharded clusters are currently excluded from this test
    def 'should handle rename collection administrative scenario without error'() {
        given:
        def mongoClient = getMongoClient()
        def database = mongoClient.getDatabase(databaseName)
        def collection = database.getCollection(collectionName)
        run(mongoClient.getDatabase(databaseName).&drop) == null

        when: 'Create a collection and the created database is in the list'
        run(database.&createCollection, collectionName)

        then: 'can rename the collection'
        def newCollectionName = 'newCollectionName'
        run(collection.&renameCollection, new MongoNamespace(databaseName, newCollectionName)) == null

        then: 'the new collection name is in the collection names list'
        !run(database.listCollectionNames().&into, []).contains(collectionName)
        run(database.listCollectionNames().&into, []).contains(newCollectionName)
    }
}
