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

package com.mongodb.async.client

import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CommandWriteOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.ListCollectionsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def codecRegistry = MongoClientImpl.getDefaultCodecRegistry()
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
        def futureResultCallback = new FutureResultCallback<Document>()

        when:
        database.executeCommand(command, futureResultCallback)
        futureResultCallback.get()

        then:
        def operation = executor.getWriteOperation() as CommandWriteOperation<Document>

        then:
        operation.command == command

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        database.executeCommand(command, primaryPreferred(), futureResultCallback)
        operation = executor.getReadOperation() as CommandReadOperation<Document>
        futureResultCallback.get()

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()

        when:
        futureResultCallback = new FutureResultCallback<BsonDocument>()
        database.executeCommand(command, BsonDocument, futureResultCallback)
        operation = executor.getWriteOperation() as CommandWriteOperation<BsonDocument>
        futureResultCallback.get()

        then:
        operation.command == command

        when:
        futureResultCallback = new FutureResultCallback<BsonDocument>()
        database.executeCommand(command, primaryPreferred(), BsonDocument, futureResultCallback)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>
        futureResultCallback.get()

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()
    }

    def 'should use DropDatabaseOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor).dropDatabase(futureResultCallback)
        def operation = executor.getWriteOperation() as DropDatabaseOperation
        futureResultCallback.get()

        then:
        expect operation, isTheSameAs(new DropDatabaseOperation(name))
    }

    def 'should use ListCollectionNamesOperation correctly'() {
        given:
        def invocationCount = 0
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(invocationCount++ == 0 ? [new Document('name', 'collectionName')] : null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor])
        def futureResultCallback = new FutureResultCallback()

        when:
        new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor)
                .listCollectionNames().into([], futureResultCallback)
        def operation = executor.getReadOperation() as ListCollectionsOperation
        def names = futureResultCallback.get()

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation(name, new DocumentCodec()))
        executor.getReadPreference() == primary()
        names == ['collectionName']
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        database.createCollection(collectionName, futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName))

        when:
        futureResultCallback = new FutureResultCallback<Void>()
        def createCollectionOptions = new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(new Document('wiredTiger', new Document()))

        database.createCollection(collectionName, createCollectionOptions, futureResultCallback)
        futureResultCallback.get()
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
