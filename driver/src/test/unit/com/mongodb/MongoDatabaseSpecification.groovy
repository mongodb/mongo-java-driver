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

package com.mongodb

import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CommandWriteOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.ListCollectionsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClient.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def codecRegistry = MongoClient.getDefaultCodecRegistry()
    def readPreference = secondary()
    def writeConcern = WriteConcern.ACKNOWLEDGED

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = new RootCodecRegistry([])
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern,
                executor).withCodecRegistry(newCodecRegistry)

        then:
        database.getCodecRegistry() == newCodecRegistry
        expect database, isTheSameAs(new MongoDatabaseImpl(name, newCodecRegistry, readPreference, writeConcern, executor))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern,
                executor).withReadPreference(newReadPreference)

        then:
        database.getReadPreference() == newReadPreference
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, newReadPreference, writeConcern, executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern,
                executor).withWriteConcern(newWriteConcern)

        then:
        database.getWriteConcern() == newWriteConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, newWriteConcern, executor))
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor)

        when:
        database.executeCommand(command)
        def operation = executor.getWriteOperation() as CommandWriteOperation<Document>

        then:
        operation.command == command

        when:
        database.executeCommand(command, primaryPreferred())
        operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()

        when:
        database.executeCommand(command, BsonDocument)
        operation = executor.getWriteOperation() as CommandWriteOperation<BsonDocument>

        then:
        operation.command == command

        when:
        database.executeCommand(command, primaryPreferred(), BsonDocument)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()
    }

    def 'should use DropDatabaseOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])

        when:
        new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor).dropDatabase()
        def operation = executor.getWriteOperation() as DropDatabaseOperation

        then:
        expect operation, isTheSameAs(new DropDatabaseOperation(name))
    }

    def 'should use ListCollectionsOperation correctly'() {
        given:
        def cursor = {
            Stub(BatchCursor) {
                hasNext() >>> [true, true, false]
                next() >> [new Document('name', 'coll1')]
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor(), cursor(), cursor(), cursor()])
        def database = new MongoDatabaseImpl(name, options, executor)
        def documentOperation = new ListCollectionsOperation(name, new DocumentCodec())
        def bsonOperation = new ListCollectionsOperation(name, new BsonDocumentCodec())

        when:
        database.listCollections().iterator()
        def operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        database.listCollections(BsonDocument).iterator()
        operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        database.listCollections(new Document('filter', 1)).iterator()
        operation = executor.getReadOperation() as ListCollectionsOperation
        executor.getReadPreference() == primary()

        then:
        expect operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        database.listCollections(new Document('filter', 1), BsonDocument).iterator()
        operation = executor.getReadOperation() as ListCollectionsOperation
        executor.getReadPreference() == primary()

        then:
        expect operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when: 'Test setting values via the fluid api'
        database.listCollections().filter(new Document('filter', 2)).batchSize(1).maxTime(1, MILLISECONDS).iterator()
        operation = executor.getReadOperation() as ListCollectionsOperation
        executor.getReadPreference() == primary()

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation(name, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2))).batchSize(1).maxTime(1, MILLISECONDS))
        executor.getReadPreference() == primary()

        when: 'Test getting the names list'
        def names = new MongoDatabaseImpl(name, options, executor).listCollectionNames().into([])
        operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        names == ['coll1']
        expect operation, isTheSameAs(new ListCollectionsOperation(name, new DocumentCodec()))
        executor.getReadPreference() == primary()
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor)

        when:
        database.createCollection(collectionName)
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName))

        when:
        def createCollectionOptions = new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(new Document('wiredTiger', new Document()))

        database.createCollection(collectionName, createCollectionOptions)
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName)
                                              .autoIndex(false)
                                              .capped(true)
                                              .usePowerOf2Sizes(true)
                                              .maxDocuments(100)
                                              .sizeInBytes(1000)
                                              .storageEngineOptions(new BsonDocument('wiredTiger', new BsonDocument())))
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def database = new MongoDatabaseImpl('databaseName', new RootCodecRegistry([]), secondary(), WriteConcern.MAJORITY,
                new TestOperationExecutor([]))

        when:
        def collection = database.getCollection('collectionName')

        then:
        expect collection, isTheSameAs(expectedCollection)

        where:
        expectedCollection = new MongoCollectionImpl<Document>(new MongoNamespace('databaseName', 'collectionName'), Document,
                new RootCodecRegistry([]), secondary(), WriteConcern.MAJORITY, new TestOperationExecutor([]))
    }

}
