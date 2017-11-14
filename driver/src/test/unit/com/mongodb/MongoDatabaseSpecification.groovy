/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.client.model.IndexOptionDefaults
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.client.model.ValidationOptions
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.CreateViewOperation
import com.mongodb.operation.DropDatabaseOperation
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.TestHelper.execute
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def codecRegistry = MongoClient.getDefaultCodecRegistry()
    def readPreference = secondary()
    def writeConcern = WriteConcern.ACKNOWLEDGED
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new MongoDatabaseImpl('a.b', codecRegistry, readPreference, writeConcern, false, readConcern, new TestOperationExecutor([]))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw IllegalArgumentException from getCollection if collectionName is invalid'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern,
                new TestOperationExecutor([]))

        when:
        database.getCollection('')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern,
                new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = Stub(CodecRegistry)
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
                .withCodecRegistry(newCodecRegistry)

        then:
        database.getCodecRegistry() == newCodecRegistry
        expect database, isTheSameAs(new MongoDatabaseImpl(name, newCodecRegistry, readPreference, writeConcern, false, readConcern,
                executor))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern , executor)
                .withReadPreference(newReadPreference)

        then:
        database.getReadPreference() == newReadPreference
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, newReadPreference, writeConcern, false, readConcern,
                executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
                .withWriteConcern(newWriteConcern)

        then:
        database.getWriteConcern() == newWriteConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, newWriteConcern, false, readConcern,
                executor))
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
                .withReadConcern(newReadConcern)

        then:
        database.getReadConcern() == newReadConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, newReadConcern,
                executor))
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
        def runCommandMethod = database.&runCommand

        when:
        execute(runCommandMethod, session, command)
        def operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primary()

        when:
        execute(runCommandMethod, session, command, primaryPreferred())
        operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primaryPreferred()

        when:
        execute(runCommandMethod, session, command, BsonDocument)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primary()

        when:
        execute(runCommandMethod, session, command, primaryPreferred(), BsonDocument)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primaryPreferred()

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use DropDatabaseOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
        def dropMethod = database.&drop

        when:
        execute(dropMethod, session)
        def operation = executor.getWriteOperation() as DropDatabaseOperation

        then:
        expect operation, isTheSameAs(new DropDatabaseOperation(name, writeConcern))
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use ListCollectionsOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
        def listCollectionsMethod = database.&listCollections
        def listCollectionNamesMethod = database.&listCollectionNames

        when:
        def listCollectionIterable = execute(listCollectionsMethod, session)

        then:
        expect listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<Document>(session, name, Document, codecRegistry,
                primary(), executor))

        when:
        listCollectionIterable = execute(listCollectionsMethod, session, BsonDocument)

        then:
        expect listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<BsonDocument>(session, name, BsonDocument, codecRegistry,
                primary(), executor))

        when:
        def listCollectionNamesIterable = execute(listCollectionNamesMethod, session)

        then:
        // listCollectionNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listCollectionNamesIterable.getMapped(), isTheSameAs(new ListCollectionsIterableImpl<BsonDocument>(session, name,
                BsonDocument, codecRegistry, primary(), executor))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
        def createCollectionMethod = database.&createCollection

        when:
        execute(createCollectionMethod, session, collectionName)
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern))
        executor.getClientSession() == session

        when:
        def createCollectionOptions = new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(BsonDocument.parse('{ wiredTiger : {}}'))
                .indexOptionDefaults(new IndexOptionDefaults().storageEngine(BsonDocument.parse('{ mmapv1 : {}}')))
                .validationOptions(new ValidationOptions().validator(BsonDocument.parse('{level: {$gte: 10}}'))
                    .validationLevel(ValidationLevel.MODERATE)
                    .validationAction(ValidationAction.WARN))
                .collation(collation)

        execute(createCollectionMethod, session, collectionName, createCollectionOptions)
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern)
                .collation(collation)
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(BsonDocument.parse('{ wiredTiger : {}}'))
                .indexOptionDefaults(BsonDocument.parse('{ storageEngine : { mmapv1 : {}}}'))
                .validator(BsonDocument.parse('{level: {$gte: 10}}'))
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.WARN))
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use CreateViewOperation correctly'() {
        given:
        def viewName = 'view1'
        def viewOn = 'col1'
        def pipeline = [new Document('$match', new Document('x', true))];
        def writeConcern = WriteConcern.JOURNALED
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, executor)
        def createViewMethod = database.&createView

        when:
        execute(createViewMethod, session, viewName, viewOn, pipeline)
        def operation = executor.getWriteOperation() as CreateViewOperation

        then:
        expect operation, isTheSameAs(new CreateViewOperation(name, viewName, viewOn,
                [new BsonDocument('$match', new BsonDocument('x', BsonBoolean.TRUE))], writeConcern))
        executor.getClientSession() == session

        when:
        execute(createViewMethod, session, viewName, viewOn, pipeline, new CreateViewOptions().collation(collation))
        operation = executor.getWriteOperation() as CreateViewOperation

        then:
        expect operation, isTheSameAs(new CreateViewOperation(name, viewName, viewOn,
                [new BsonDocument('$match', new BsonDocument('x', BsonBoolean.TRUE))], writeConcern).collation(collation))
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the createView pipeline data correctly'() {
        given:
        def viewName = 'view1'
        def viewOn = 'col1'
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, readConcern, Stub(OperationExecutor))

        when:
        database.createView(viewName, viewOn, null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.createView(viewName, viewOn, [null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
        def database = new MongoDatabaseImpl('databaseName', codecRegistry, secondary(), WriteConcern.MAJORITY, true,
                ReadConcern.MAJORITY, new TestOperationExecutor([]))

        when:
        def collection = database.getCollection('collectionName')

        then:
        expect collection, isTheSameAs(expectedCollection)

        where:
        expectedCollection = new MongoCollectionImpl<Document>(new MongoNamespace('databaseName', 'collectionName'), Document,
                fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()]), secondary(),
                WriteConcern.MAJORITY, true, ReadConcern.MAJORITY, new TestOperationExecutor([]))
    }

}
