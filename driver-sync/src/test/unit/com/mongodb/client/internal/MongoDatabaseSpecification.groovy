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

package com.mongodb.client.internal

import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.internal.client.model.AggregationLevel
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.client.model.IndexOptionDefaults
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.client.model.ValidationOptions
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel
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
import org.bson.internal.OverridableUuidRepresentationCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.execute
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    def readPreference = secondary()
    def writeConcern = WriteConcern.ACKNOWLEDGED
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new MongoDatabaseImpl('a.b', codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY, new
                TestOperationExecutor([]))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw IllegalArgumentException from getCollection if collectionName is invalid'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                new TestOperationExecutor([]))

        when:
        database.getCollection('')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = fromProviders(new ValueCodecProvider())
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
                .withCodecRegistry(newCodecRegistry)

        then:
        database.getCodecRegistry() == newCodecRegistry
        expect database, isTheSameAs(new MongoDatabaseImpl(name, newCodecRegistry, readPreference, writeConcern, false, false, readConcern,
                JAVA_LEGACY, executor))

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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
                .withReadPreference(newReadPreference)

        then:
        database.getReadPreference() == newReadPreference
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, newReadPreference, writeConcern, false, false, readConcern,
                JAVA_LEGACY, executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
                .withWriteConcern(newWriteConcern)

        then:
        database.getWriteConcern() == newWriteConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, newWriteConcern, false, false, readConcern,
                JAVA_LEGACY, executor))
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
                .withReadConcern(newReadConcern)

        then:
        database.getReadConcern() == newReadConcern
        expect database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, newReadConcern,
                JAVA_LEGACY, executor))
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def runCommandMethod = database.&runCommand

        when:
        TestHelper.execute(runCommandMethod, session, command)
        def operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primary()

        when:
        TestHelper.execute(runCommandMethod, session, command, primaryPreferred())
        operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primaryPreferred()

        when:
        TestHelper.execute(runCommandMethod, session, command, BsonDocument)
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

        then:
        operation.command == command
        executor.getClientSession() == session
        executor.getReadPreference() == primary()

        when:
        TestHelper.execute(runCommandMethod, session, command, primaryPreferred(), BsonDocument)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def dropMethod = database.&drop

        when:
        TestHelper.execute(dropMethod, session)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def listCollectionsMethod = database.&listCollections
        def listCollectionNamesMethod = database.&listCollectionNames

        when:
        def listCollectionIterable = TestHelper.execute(listCollectionsMethod, session)

        then:
        expect listCollectionIterable, isTheSameAs(MongoIterables.listCollectionsOf(session, name, false, Document, codecRegistry,
                primary(), executor, false))

        when:
        listCollectionIterable = TestHelper.execute(listCollectionsMethod, session, BsonDocument)

        then:
        expect listCollectionIterable, isTheSameAs(MongoIterables.listCollectionsOf(session, name, false, BsonDocument,
                codecRegistry, primary(), executor, false))

        when:
        def listCollectionNamesIterable = TestHelper.execute(listCollectionNamesMethod, session)

        then:
        // listCollectionNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listCollectionNamesIterable.getMapped(), isTheSameAs(MongoIterables.listCollectionsOf(session, name, true,
                BsonDocument, codecRegistry, primary(), executor, false))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def createCollectionMethod = database.&createCollection

        when:
        TestHelper.execute(createCollectionMethod, session, collectionName)
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern))
        executor.getClientSession() == session

        when:
        def createCollectionOptions = new CreateCollectionOptions()
                .capped(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
                .storageEngineOptions(BsonDocument.parse('{ wiredTiger : {}}'))
                .indexOptionDefaults(new IndexOptionDefaults().storageEngine(BsonDocument.parse('{ mmapv1 : {}}')))
                .validationOptions(new ValidationOptions().validator(BsonDocument.parse('{level: {$gte: 10}}'))
                    .validationLevel(ValidationLevel.MODERATE)
                    .validationAction(ValidationAction.WARN))
                .collation(collation)

        TestHelper.execute(createCollectionMethod, session, collectionName, createCollectionOptions)
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern)
                .collation(collation)
                .capped(true)
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def createViewMethod = database.&createView

        when:
        TestHelper.execute(createViewMethod, session, viewName, viewOn, pipeline)
        def operation = executor.getWriteOperation() as CreateViewOperation

        then:
        expect operation, isTheSameAs(new CreateViewOperation(name, viewName, viewOn,
                [new BsonDocument('$match', new BsonDocument('x', BsonBoolean.TRUE))], writeConcern))
        executor.getClientSession() == session

        when:
        TestHelper.execute(createViewMethod, session, viewName, viewOn, pipeline, new CreateViewOptions().collation(collation))
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
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false,
                readConcern, JAVA_LEGACY, Stub(OperationExecutor))

        when:
        database.createView(viewName, viewOn, null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.createView(viewName, viewOn, [null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def namespace = new MongoNamespace(name, 'ignored')
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def watchMethod = database.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [], Document, ChangeStreamLevel.DATABASE, false), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], Document, ChangeStreamLevel.DATABASE, false), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], BsonDocument, ChangeStreamLevel.DATABASE, false), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)

        when:
        database.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.watch([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create AggregateIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)
        def aggregateMethod = database.&aggregate

        when:
        def aggregateIterable = execute(aggregateMethod, session, [])

        then:
        expect aggregateIterable, isTheSameAs(MongoIterables.aggregateOf(session, name, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [], AggregationLevel.DATABASE, false), ['codec'])

        when:
        aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)])

        then:
        expect aggregateIterable, isTheSameAs(MongoIterables.aggregateOf(session, name, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)], AggregationLevel.DATABASE, false), ['codec'])

        when:
        aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect aggregateIterable, isTheSameAs(MongoIterables.aggregateOf(session, name, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, [new Document('$match', 1)], AggregationLevel.DATABASE, false),
                ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the AggregationIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false, false, readConcern, JAVA_LEGACY,
                executor)

        when:
        database.aggregate(null, [])

        then:
        thrown(IllegalArgumentException)

        when:
        database.aggregate((List) null)

        then:
        thrown(IllegalArgumentException)

        when:
        database.aggregate([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
        def database = new MongoDatabaseImpl('databaseName', codecRegistry, secondary(), WriteConcern.MAJORITY, true, true,
                ReadConcern.MAJORITY, JAVA_LEGACY, new TestOperationExecutor([]))

        when:
        def collection = database.getCollection('collectionName')

        then:
        expect collection, isTheSameAs(expectedCollection)

        where:
        expectedCollection = new MongoCollectionImpl<Document>(new MongoNamespace('databaseName', 'collectionName'), Document,
                fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()]), secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, JAVA_LEGACY, new TestOperationExecutor([]))
    }

    def 'should validate the client session correctly'() {
        given:
        def database = new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, false,
                false, readConcern, JAVA_LEGACY, Stub(OperationExecutor))

        when:
        database.createCollection(null, 'newColl')

        then:
        thrown(IllegalArgumentException)

        when:
        database.createView(null, 'newView', [Document.parse('{$match: {}}')])

        then:
        thrown(IllegalArgumentException)

        when:
        database.drop(null)

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
        database.runCommand(null, Document.parse('{}'))

        then:
        thrown(IllegalArgumentException)
    }

}
