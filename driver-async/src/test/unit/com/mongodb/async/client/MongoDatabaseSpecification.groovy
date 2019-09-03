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
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.SingleResultCallback
import com.mongodb.client.model.AggregationLevel
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.client.model.IndexOptionDefaults
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.model.changestream.ChangeStreamLevel
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.CreateViewOperation
import com.mongodb.operation.DropDatabaseOperation
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.internal.OverridableUuidRepresentationCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.async.client.TestHelper.execute
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def codecRegistry = MongoClients.getDefaultCodecRegistry()
    def readPreference = secondary()
    def writeConcern = WriteConcern.ACKNOWLEDGED
    def readConcern = ReadConcern.DEFAULT
    def uuidRepresentation = UuidRepresentation.JAVA_LEGACY
    def collation = Collation.builder().locale('en').build()

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new MongoDatabaseImpl('a.b', codecRegistry, readPreference, writeConcern, false, true,
                readConcern, uuidRepresentation, new TestOperationExecutor([]))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw IllegalArgumentException from getCollection if collectionName is invalid'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, new TestOperationExecutor([]))

        when:
        database.getCollection('')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = fromProviders(new ValueCodecProvider())
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                JAVA_LEGACY, executor)
                .withCodecRegistry(newCodecRegistry)

        then:
        database.getCodecRegistry() == newCodecRegistry
        expect database, isTheSameAs(new MongoDatabaseImpl(name, newCodecRegistry, readPreference, writeConcern,
                false, true, readConcern, JAVA_LEGACY, executor))

        when:
        database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                STANDARD, executor)
                .withCodecRegistry(newCodecRegistry)

        then:
        database.getCodecRegistry() instanceof OverridableUuidRepresentationCodecRegistry
        (database.getCodecRegistry() as OverridableUuidRepresentationCodecRegistry).uuidRepresentation == STANDARD
        (database.getCodecRegistry() as OverridableUuidRepresentationCodecRegistry).wrapped == newCodecRegistry
        expect database, isTheSameAs(new MongoDatabaseImpl(name, database.getCodecRegistry(), readPreference, writeConcern,
                false, true, readConcern, STANDARD, executor))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
                .withReadPreference(newReadPreference)

        then:
        database.getReadPreference() == newReadPreference
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, newReadPreference, writeConcern, false, true,
                readConcern, uuidRepresentation, executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
                .withWriteConcern(newWriteConcern)

        then:
        database.getWriteConcern() == newWriteConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, newWriteConcern, false, true, readConcern,
                uuidRepresentation, executor))
    }


    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
                .withReadConcern(newReadConcern)

        then:
        database.getReadConcern() == newReadConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, newReadConcern,
                uuidRepresentation, executor))
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
        def listCollectionsMethod = database.&listCollections
        def listCollectionNamesMethod = database.&listCollectionNames

        when:
        def listCollectionIterable = execute(listCollectionsMethod, session)

        then:
        expect listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<Document>(session, name, false, Document, codecRegistry,
                primary(), executor, true))

        when:
        listCollectionIterable = execute(listCollectionsMethod, session, BsonDocument)

        then:
        expect listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<BsonDocument>(session, name, false, BsonDocument,
                codecRegistry, primary(), executor, true))

        when:
        def listCollectionNamesIterable = execute(listCollectionNamesMethod, session)

        then:
        // listCollectionNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listCollectionNamesIterable.getMapped(), isTheSameAs(new ListCollectionsIterableImpl<BsonDocument>(session, name,
                true, BsonDocument, codecRegistry, primary(), executor, true))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
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
        def pipeline = [new Document('$match', new Document('x', true))]
        def writeConcern = WriteConcern.JOURNALED
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern,
                false, true, readConcern, uuidRepresentation, Stub(OperationExecutor))
        def callback = Stub(SingleResultCallback)

        when:
        database.createView(viewName, viewOn, null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        database.createView(viewName, viewOn, [null], callback)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def namespace = new MongoNamespace(name, 'ignored')
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
        def watchMethod = database.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [], Document, ChangeStreamLevel.DATABASE, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], Document, ChangeStreamLevel.DATABASE, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], BsonDocument, ChangeStreamLevel.DATABASE, true), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)

        when:
        database.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.watch([null]).into([]) { result, t -> }

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create AggregateIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)
        def aggregateMethod = database.&aggregate

        when:
        def aggregateIterable = execute(aggregateMethod, session, [])

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(session, name, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [], AggregationLevel.DATABASE, true), ['codec'])

        when:
        aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)])

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(session, name, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)], AggregationLevel.DATABASE, true), ['codec'])

        when:
        aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(session, name, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, [new Document('$match', 1)], AggregationLevel.DATABASE, true),
                ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the AggregationIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, true, readConcern,
                uuidRepresentation, executor)

        when:
        database.aggregate(null, [])

        then:
        thrown(IllegalArgumentException)

        when:
        database.aggregate((List) null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.aggregate([null]).into([]) { result, t -> }

        then:
        thrown(IllegalArgumentException)
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def codecRegistry = fromProviders([new BsonValueCodecProvider()])
        def database = new MongoDatabaseImpl('databaseName', codecRegistry, secondary(), WriteConcern.MAJORITY, true, true,
                ReadConcern.MAJORITY, uuidRepresentation, new TestOperationExecutor([]))
        def expectedCollection = new MongoCollectionImpl<Document>(new MongoNamespace('databaseName', 'collectionName'), Document,
                fromProviders([new BsonValueCodecProvider()]), secondary(), WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY,
                uuidRepresentation, new TestOperationExecutor([]))

        when:
        def collection = database.getCollection('collectionName')

        then:
        expect collection, isTheSameAs(expectedCollection)
    }

    def 'should validate the client session correctly'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern,
                false, true, readConcern, uuidRepresentation, Stub(OperationExecutor))
        def callback = Stub(SingleResultCallback)

        when:
        database.createCollection(null, 'newColl', callback)

        then:
        thrown(IllegalArgumentException)

        when:
        database.createView(null, 'newView', [Document.parse('{$match: {}}')], callback)

        then:
        thrown(IllegalArgumentException)

        when:
        database.drop(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        database.listCollectionNames(null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.listCollections(null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.runCommand(null, Document.parse('{}'), callback)

        then:
        thrown(IllegalArgumentException)
    }

}
