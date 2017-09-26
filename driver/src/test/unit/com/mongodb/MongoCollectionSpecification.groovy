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

import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.IndexRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.client.ImmutableDocument
import com.mongodb.client.ImmutableDocumentCodecProvider
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.DeleteManyModel
import com.mongodb.client.model.DeleteOneModel
import com.mongodb.client.model.DeleteOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.FindOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.UpdateManyModel
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.client.test.Worker
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexesOperation
import com.mongodb.operation.DropCollectionOperation
import com.mongodb.operation.DropIndexOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.ListIndexesOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.RenameCollectionOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.bulk.BulkWriteResult.acknowledged
import static com.mongodb.bulk.BulkWriteResult.unacknowledged
import static com.mongodb.bulk.WriteRequest.Type.DELETE
import static com.mongodb.bulk.WriteRequest.Type.INSERT
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings('ClassSize')
class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def codecRegistry = MongoClient.getDefaultCodecRegistry()
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()

    def 'should return the correct name from getName'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern,
                new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should behave correctly when using withDefaultClass'() {
        given:
        def newClass = Worker
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
                .withDocumentClass(newClass)

        then:
        collection.getDocumentClass() == newClass
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, newClass, codecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor))
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = Stub(CodecRegistry)
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor).withCodecRegistry(newCodecRegistry)

        then:
        collection.getCodecRegistry() == newCodecRegistry
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, newCodecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor).withReadPreference(newReadPreference)

        then:
        collection.getReadPreference() == newReadPreference
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, newReadPreference, ACKNOWLEDGED,
                readConcern, executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor).withWriteConcern(newWriteConcern)

        then:
        collection.getWriteConcern() == newWriteConcern
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, newWriteConcern,
                readConcern, executor))
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newWReadConcern = ReadConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor).withReadConcern(newWReadConcern)

        then:
        collection.getReadConcern() == newWReadConcern
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                newWReadConcern, executor))
    }

    def <T> T execute(final Closure<T> method, final ClientSession session, ... restOfArgs) {
        if (session == null) {
            method.call(restOfArgs)
        }  else {
            method.call([session, *restOfArgs] as Object[])
        }
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def expectedOperation = new CountOperation(namespace).filter(filter)
        def countMethod = collection.&count

        when:
        execute(countMethod, session)
        def operation = executor.getReadOperation() as CountOperation

        then:
        executor.getClientSession() == session
        expect operation, isTheSameAs(expectedOperation)

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        execute(countMethod, session, filter)
        operation = executor.getReadOperation() as CountOperation

        then:
        executor.getClientSession() == session
        expect operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        def hint = new BsonDocument('hint', new BsonInt32(1))
        execute(countMethod, session, filter, new CountOptions().hint(hint).skip(10).limit(100)
                .maxTime(100, MILLISECONDS).collation(collation))
        operation = executor.getReadOperation() as CountOperation

        then:
        executor.getClientSession() == session
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS)
                .collation(collation))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create DistinctIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def filter = new Document('a', 1)
        def distinctMethod = collection.&distinct

        when:
        def distinctIterable = execute(distinctMethod, session, 'field', String)

        then:
        expect distinctIterable, isTheSameAs(new DistinctIterableImpl(session, namespace, Document, String, codecRegistry, readPreference,
                readConcern, executor, 'field', new BsonDocument()))

        when:
        distinctIterable = execute(distinctMethod, session, 'field', String).filter(filter)

        then:
        expect distinctIterable, isTheSameAs(new DistinctIterableImpl(session, namespace, Document, String, codecRegistry, readPreference,
                readConcern, executor, 'field', filter))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create FindIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def findMethod = collection.&find

        when:
        def findIterable = execute(findMethod, session)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(session, namespace, Document, Document, codecRegistry,
                readPreference, readConcern, executor, new BsonDocument(), new FindOptions()))

        when:
        findIterable = execute(findMethod, session, BsonDocument)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(session, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, executor, new BsonDocument(), new FindOptions()))

        when:
        findIterable = execute(findMethod, session, new Document())

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(session, namespace, Document, Document, codecRegistry,
                readPreference, readConcern, executor, new Document(), new FindOptions()))

        when:
        findIterable = execute(findMethod, session, new Document(), BsonDocument)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(session, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, executor, new Document(), new FindOptions()))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create AggregateIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def aggregateMethod = collection.&aggregate

        when:
        def aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)])

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(session, namespace, Document, Document, codecRegistry,
                readPreference, readConcern,  ACKNOWLEDGED, executor, [new Document('$match', 1)]))

        when:
        aggregateIterable = execute(aggregateMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(session, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern,  ACKNOWLEDGED, executor, [new Document('$match', 1)]))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the aggregation pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)

        when:
        collection.aggregate(null)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.aggregate([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def watchMethod = collection.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [], Document), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], Document), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], BsonDocument), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)

        when:
        collection.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.watch([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create MapReduceIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def mapReduceMethod = collection.&mapReduce

        when:
        def mapReduceIterable = execute(mapReduceMethod, session, 'map', 'reduce')

        then:
        expect mapReduceIterable, isTheSameAs(new MapReduceIterableImpl(session, namespace, Document, Document, codecRegistry,
                readPreference, readConcern,  ACKNOWLEDGED, executor, 'map', 'reduce'))


        when:
        mapReduceIterable = execute(mapReduceMethod, session, 'map', 'reduce', BsonDocument)

        then:
        expect mapReduceIterable, isTheSameAs(new MapReduceIterableImpl(session, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern,  ACKNOWLEDGED, executor, 'map', 'reduce'))

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'bulkWrite should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..3).collect {
            writeConcern.isAcknowledged() ? acknowledged(INSERT, 0, []) : unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, BsonDocument, codecRegistry, readPreference, writeConcern, readConcern,
                executor)
        def expectedOperation = { boolean ordered, WriteConcern wc, Boolean bypassDocumentValidation ->
            new MixedBulkWriteOperation(namespace, [
                    new InsertRequest(BsonDocument.parse('{_id: 1}')),
                    new UpdateRequest(BsonDocument.parse('{a: 2}'), BsonDocument.parse('{a: 200}'), REPLACE)
                            .multi(false).upsert(true).collation(collation),
                    new UpdateRequest(BsonDocument.parse('{a: 3}'), BsonDocument.parse('{$set: {a: 1}}'), UPDATE)
                            .multi(false).upsert(true).collation(collation),
                    new UpdateRequest(BsonDocument.parse('{a: 4}'), BsonDocument.parse('{$set: {a: 1}}'), UPDATE).multi(true),
                    new DeleteRequest(BsonDocument.parse('{a: 5}')).multi(false),
                    new DeleteRequest(BsonDocument.parse('{a: 6}')).multi(true).collation(collation)
            ], ordered, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def updateOptions = new UpdateOptions().upsert(true).collation(collation)
        def deleteOptions = new DeleteOptions().collation(collation)
        def bulkOperations = [new InsertOneModel(BsonDocument.parse('{_id: 1}')),
                              new ReplaceOneModel(BsonDocument.parse('{a: 2}'), BsonDocument.parse('{a: 200}'), updateOptions),
                              new UpdateOneModel(BsonDocument.parse('{a: 3}'), BsonDocument.parse('{$set: {a: 1}}'), updateOptions),
                              new UpdateManyModel(BsonDocument.parse('{a: 4}'), BsonDocument.parse('{$set: {a: 1}}')),
                              new DeleteOneModel(BsonDocument.parse('{a: 5}')),
                              new DeleteManyModel(BsonDocument.parse('{a: 6}'), deleteOptions)]
        def bulkWriteMethod = collection.&bulkWrite

        when:
        def result = execute(bulkWriteMethod, session, bulkOperations)
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, null))

        when:
        result = execute(bulkWriteMethod, session, bulkOperations, new BulkWriteOptions().ordered(true).bypassDocumentValidation(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true))

        when:
        result = execute(bulkWriteMethod, session, bulkOperations, new BulkWriteOptions().ordered(false).bypassDocumentValidation(false))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, false))

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)

        when:
        collection.bulkWrite(null)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.bulkWrite([null])

        then:
        thrown(IllegalArgumentException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])

        then:
        thrown(CodecConfigurationException)
    }

    def 'insertOne should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..3).collect {
            writeConcern.isAcknowledged() ? acknowledged(INSERT, 0, []) : unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern,
                executor)
        def expectedOperation = { WriteConcern wc, Boolean bypassDocumentValidation ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                    true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def insertOneMethod = collection.&insertOne

        when:
        execute(insertOneMethod, session, new Document('_id', 1))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, null))
        executor.getClientSession() == session

        when:
        execute(insertOneMethod, session, new Document('_id', 1), new InsertOneOptions().bypassDocumentValidation(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, true))
        executor.getClientSession() == session

        when:
        execute(insertOneMethod, session, new Document('_id', 1), new InsertOneOptions().bypassDocumentValidation(false))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, false))
        executor.getClientSession() == session

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'insertMany should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..3).collect {
            writeConcern.isAcknowledged() ? acknowledged(INSERT, 0, []) : unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean ordered, WriteConcern wc, Boolean bypassDocumentValidation ->
            new MixedBulkWriteOperation(namespace,
                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                     new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))],
                    ordered, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def insertManyMethod = collection.&insertMany

        when:
        execute(insertManyMethod, session, [new Document('_id', 1), new Document('_id', 2)])
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, null))
        executor.getClientSession() == session

        when:
        execute(insertManyMethod, session, [new Document('_id', 1), new Document('_id', 2)],
                new InsertManyOptions().ordered(true).bypassDocumentValidation(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true))
        executor.getClientSession() == session

        when:
        execute(insertManyMethod, session, [new Document('_id', 1), new Document('_id', 2)],
                new InsertManyOptions().ordered(false).bypassDocumentValidation(false))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, false))
        executor.getClientSession() == session

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'should validate the insertMany data correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern,
                Stub(OperationExecutor))

        when:
        collection.insertMany(null)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.insertMany([null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'deleteOne should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ? acknowledged(DELETE, 1, []) : unacknowledged()
        })
        def expectedResult = writeConcern.isAcknowledged() ? DeleteResult.acknowledged(1) : DeleteResult.unacknowledged()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def deleteOneMethod = collection.&deleteOne

        when:
        def result = execute(deleteOneMethod, session, new Document('_id', 1))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)], true, writeConcern))
        result == expectedResult
        executor.getClientSession() == session

        when:
        result = execute(deleteOneMethod, session, new Document('_id', 1), new DeleteOptions().collation(collation))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false).collation(collation)], true, writeConcern))
        result == expectedResult
        executor.getClientSession() == session

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'deleteOne should translate BulkWriteException correctly'() {
        given:
        def bulkWriteException = new MongoBulkWriteException(acknowledged(0, 0, 1, null, []), [],
                                                             new com.mongodb.bulk.WriteConcernError(100, '', new BsonDocument()),
                                                             new ServerAddress())

        def executor = new TestOperationExecutor([bulkWriteException])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                                                 readConcern, executor)

        when:
        collection.deleteOne(new Document('_id', 1))

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError == bulkWriteException.writeConcernError
        ex.writeResult.wasAcknowledged()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null
    }

    def 'deleteMany should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ? acknowledged(DELETE, 1, []) : unacknowledged()
        })
        def expectedResult = writeConcern.isAcknowledged() ? DeleteResult.acknowledged(1) : DeleteResult.unacknowledged()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def deleteManyMethod = collection.&deleteMany

        when:
        def result = execute(deleteManyMethod, session, new Document('_id', 1))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(true)], true, writeConcern))
        executor.getClientSession() == session
        result == expectedResult

        when:
        result = execute(deleteManyMethod, session, new Document('_id', 1), new DeleteOptions().collation(collation))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(true).collation(collation)], true, writeConcern))
        executor.getClientSession() == session
        result == expectedResult

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'replaceOne should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ?
                    acknowledged(REPLACE, 1, modifiedCount,
                            upsertedId == null ? [] : [new com.mongodb.bulk.BulkWriteUpsert(0, upsertedId)]) :
                    unacknowledged()
        })
        def expectedResult = writeConcern.isAcknowledged() ?
                UpdateResult.acknowledged(1, modifiedCount, upsertedId) : UpdateResult.unacknowledged()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), REPLACE)
                             .collation(collation).upsert(upsert)], true, wc)
                    .bypassDocumentValidation(bypassDocumentValidation)
        }
        def replaceOneMethod = collection.&replaceOne

        when:
        def result = execute(replaceOneMethod, session, new Document('a', 1), new Document('a', 10),
                new UpdateOptions().bypassDocumentValidation(bypassDocumentValidation))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, bypassDocumentValidation, null))
        executor.getClientSession() == session
        result == expectedResult

        when:
        result = execute(replaceOneMethod, session, new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(bypassDocumentValidation).collation(collation))
        executor.getClientSession() == session
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, bypassDocumentValidation, collation))
        result == expectedResult

        where:
        [bypassDocumentValidation, modifiedCount, upsertedId, writeConcern, session] << [
                [null, true, false],
                [null, 1],
                [null, new BsonInt32(42)],
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'replaceOne should translate BulkWriteException correctly'() {
        given:
        def bulkWriteException = new MongoBulkWriteException(bulkWriteResult, [],
                                                             new com.mongodb.bulk.WriteConcernError(100, '', new BsonDocument()),
                                                             new ServerAddress())

        def executor = new TestOperationExecutor([bulkWriteException])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED,
                                                 readConcern, executor)

        when:
        collection.replaceOne(new Document('_id', 1), new Document('_id', 1))

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError == bulkWriteException.writeConcernError
        ex.writeResult.wasAcknowledged() == writeResult.wasAcknowledged()
        ex.writeResult.count == writeResult.count
        ex.writeResult.updateOfExisting == writeResult.updateOfExisting
        ex.writeResult.upsertedId == writeResult.upsertedId

        where:
        bulkWriteResult                                                           | writeResult
        acknowledged(0, 1, 0, 1, [])                                              | WriteConcernResult.acknowledged(1, true, null)
        acknowledged(0, 0, 0, 0,
                     [new com.mongodb.bulk.BulkWriteUpsert(0, new BsonInt32(1))]) | WriteConcernResult.acknowledged(1, false,
                                                                                                                    new BsonInt32(1))
    }

    def 'updateOne should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ? acknowledged(UPDATE, 1, []) : unacknowledged()
        })
        def expectedResult = writeConcern.isAcknowledged() ? UpdateResult.acknowledged(1, 0, null) : UpdateResult.unacknowledged()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), UPDATE)
                            .multi(false).upsert(upsert).collation(collation)], true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def updateOneMethod = collection.&updateOne

        when:
        def result = execute(updateOneMethod, session, new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, null, null))
        executor.getClientSession() == session
        result == expectedResult

        when:
        result = execute(updateOneMethod, session, new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true).collation(collation))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true, collation))
        executor.getClientSession() == session
        result == expectedResult

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'updateMany should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ? acknowledged(UPDATE, 5, 3, []) : unacknowledged()
        })
        def expectedResult = writeConcern.isAcknowledged() ? UpdateResult.acknowledged(5, 3, null) : UpdateResult.unacknowledged()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), UPDATE)
                             .multi(true).upsert(upsert).collation(collation)], true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def updateManyMethod = collection.&updateMany

        when:
        def result = execute(updateManyMethod, session, new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, null, null))
        result == expectedResult

        when:
        result = execute(updateManyMethod, session, new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true).collation(collation))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true, collation))
        result == expectedResult

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'should translate MongoBulkWriteException to MongoWriteException'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        def e = thrown(MongoWriteException)
        e.error == new WriteError(11000, 'oops', new BsonDocument())

        where:
        executor << new TestOperationExecutor([new MongoBulkWriteException(acknowledged(INSERT, 1, []),
                [new com.mongodb.bulk.BulkWriteError(11000, 'oops',
                        new BsonDocument(), 0)],
                null, new ServerAddress())])
    }

    def 'should translate MongoBulkWriteException to MongoWriteConcernException'() {
        given:
        def executor = new TestOperationExecutor([new MongoBulkWriteException(acknowledged(INSERT, 1, []), [],
                new com.mongodb.bulk.WriteConcernError(42, 'oops',
                        new BsonDocument()),
                new ServerAddress())])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        def e = thrown(MongoWriteConcernException)
        e.writeConcernError == new com.mongodb.bulk.WriteConcernError(42, 'oops', new BsonDocument())
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..2).collect {
            writeConcern.isAcknowledged() ? WriteConcernResult.acknowledged(1, true, null) : WriteConcernResult.unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, writeConcern, new DocumentCodec())
                .filter(new BsonDocument('a', new BsonInt32(1)))
        def findOneAndDeleteMethod = collection.&findOneAndDelete

        when:
        execute(findOneAndDeleteMethod, session, new Document('a', 1))
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        execute(findOneAndDeleteMethod, session, new Document('a', 1),
                new FindOneAndDeleteOptions().projection(new Document('projection', 1))
                        .maxTime(100, MILLISECONDS).collation(collation))
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).collation(collation))

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..3).collect {
            writeConcern.isAcknowledged() ? WriteConcernResult.acknowledged(1, true, null) : WriteConcernResult.unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, writeConcern, new DocumentCodec(),
                new BsonDocument('a', new BsonInt32(10))).filter(new BsonDocument('a', new BsonInt32(1)))
        def findOneAndReplaceMethod = collection.&findOneAndReplace

        when:
        execute(findOneAndReplaceMethod, session, new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        execute(findOneAndReplaceMethod, session, new Document('a', 1), new Document('a', 10),
                new FindOneAndReplaceOptions().projection(new Document('projection', 1))
                        .maxTime(100, MILLISECONDS).bypassDocumentValidation(false))
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(false))

        when:
        execute(findOneAndReplaceMethod, session, new Document('a', 1), new Document('a', 10),
                new FindOneAndReplaceOptions().projection(new Document('projection', 1))
                        .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation))
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation))

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def executor = new TestOperationExecutor((1..3).collect {
            writeConcern.isAcknowledged() ? WriteConcernResult.acknowledged(1, true, null) : WriteConcernResult.unacknowledged()
        })
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, writeConcern, new DocumentCodec(),
                new BsonDocument('a', new BsonInt32(10))).filter(new BsonDocument('a', new BsonInt32(1)))
        def findOneAndUpdateMethod = collection.&findOneAndUpdate

        when:
        execute(findOneAndUpdateMethod, session, new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        execute(findOneAndUpdateMethod, session, new Document('a', 1), new Document('a', 10),
                new FindOneAndUpdateOptions().projection(new Document('projection', 1)).maxTime(100, MILLISECONDS)
                        .bypassDocumentValidation(false))
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(false))

        when:
        execute(findOneAndUpdateMethod, session, new Document('a', 1), new Document('a', 10),
                new FindOneAndUpdateOptions().projection(new Document('projection', 1)).maxTime(100, MILLISECONDS)
                        .bypassDocumentValidation(true).collation(collation))
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation))

        where:
        [writeConcern, session] << [
                [ACKNOWLEDGED, UNACKNOWLEDGED],
                [null, Stub(ClientSession)]
        ].combinations()
   }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def expectedOperation = new DropCollectionOperation(namespace, ACKNOWLEDGED)
        def dropMethod = collection.&drop

        when:
        execute(dropMethod, session)
        def operation = executor.getWriteOperation() as DropCollectionOperation
        executor.getClientSession() == session

        then:
        expect operation, isTheSameAs(expectedOperation)

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def createIndexMethod = collection.&createIndex
        def createIndexesMethod = collection.&createIndexes

        when:
        def expectedOperation = new CreateIndexesOperation(namespace,
                [new IndexRequest(new BsonDocument('key', new BsonInt32(1)))], ACKNOWLEDGED)
        def indexName = execute(createIndexMethod, session, new Document('key', 1))
        def operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        indexName == 'key_1'

        when:
        expectedOperation = new CreateIndexesOperation(namespace,
                [new IndexRequest(new BsonDocument('key', new BsonInt32(1))),
                 new IndexRequest(new BsonDocument('key1', new BsonInt32(1)))], ACKNOWLEDGED)
        def indexNames = execute(createIndexesMethod, session, [new IndexModel(new Document('key', 1)),
                                                   new IndexModel(new Document('key1', 1))])
        operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session
        indexNames == ['key_1', 'key1_1']

        when:
        expectedOperation = new CreateIndexesOperation(namespace,
                [new IndexRequest(new BsonDocument('key', new BsonInt32(1)))
                         .background(true)
                         .unique(true)
                         .sparse(true)
                         .name('aIndex')
                         .expireAfter(100, TimeUnit.SECONDS)
                         .version(1)
                         .weights(new BsonDocument('a', new BsonInt32(1000)))
                         .defaultLanguage('es')
                         .languageOverride('language')
                         .textVersion(1)
                         .sphereVersion(2)
                         .bits(1)
                         .min(-180.0)
                         .max(180.0)
                         .bucketSize(200.0)
                         .storageEngine(BsonDocument.parse('{wiredTiger: {configString: "block_compressor=zlib"}}'))
                         .partialFilterExpression(BsonDocument.parse('{status: "active"}'))
                         .collation(collation)
                ], ACKNOWLEDGED)
        indexName = execute(createIndexMethod, session, new Document('key', 1), new IndexOptions()
                .background(true)
                .unique(true)
                .sparse(true)
                .name('aIndex')
                .expireAfter(100, TimeUnit.SECONDS)
                .version(1)
                .weights(new BsonDocument('a', new BsonInt32(1000)))
                .defaultLanguage('es')
                .languageOverride('language')
                .textVersion(1)
                .sphereVersion(2)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .bucketSize(200.0)
                .storageEngine(BsonDocument.parse('{wiredTiger: {configString: "block_compressor=zlib"}}'))
                .partialFilterExpression(BsonDocument.parse('{status: "active"}'))
                .collation(collation))
        operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session
        indexName == 'aIndex'

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the createIndexes data correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern,
                Stub(OperationExecutor))

        when:
        collection.createIndexes(null)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.createIndexes([null])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def listIndexesMethod = collection.&listIndexes

        when:
        execute(listIndexesMethod, session).into([])
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))
        executor.getClientSession() == session

        when:
        def indexes = execute(listIndexesMethod, session, BsonDocument).into([])
        operation = executor.getReadOperation() as ListIndexesOperation
        indexes == []

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new BsonDocumentCodec()))
        executor.getClientSession() == session

        when:
        execute(listIndexesMethod, session).batchSize(10).maxTime(10, MILLISECONDS).iterator()
        operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()).batchSize(10).maxTime(10, MILLISECONDS))
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def dropIndexMethod = collection.&dropIndex

        when:
        def expectedOperation = new DropIndexOperation(namespace, 'indexName', ACKNOWLEDGED)
        execute(dropIndexMethod, session, 'indexName')
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session

        when:
        def keys = new BsonDocument('x', new BsonInt32(1))
        expectedOperation = new DropIndexOperation(namespace, keys, ACKNOWLEDGED)
        execute(dropIndexMethod, session, keys)
        operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*', ACKNOWLEDGED)
        def dropIndexesMethod = collection.&dropIndexes

        when:
        execute(dropIndexesMethod, session)
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should use RenameCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, ACKNOWLEDGED, readConcern, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace, ACKNOWLEDGED)
        def renameCollection = collection.&renameCollection

        when:
        execute(renameCollection, session, newNamespace)
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        executor.getClientSession() == session

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should not expect to mutate the document when inserting'() {
        given:
        def executor = new TestOperationExecutor([null])
        def customCodecRegistry = fromRegistries(fromProviders(new ImmutableDocumentCodecProvider()), codecRegistry)
        def collection = new MongoCollectionImpl(namespace, ImmutableDocument, customCodecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor)
        def document = new ImmutableDocument(['a': 1])

        when:
        collection.insertOne(document)

        then:
        !document.containsKey('_id')

        when:
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation
        def request = operation.writeRequests.get(0) as InsertRequest

        then:
        request.getDocument().containsKey('_id')
    }

    def 'should not expect to mutate the document when bulk writing'() {
        given:
        def executor = new TestOperationExecutor([null])
        def customCodecRegistry = fromRegistries(fromProviders(new ImmutableDocumentCodecProvider()), codecRegistry)
        def collection = new MongoCollectionImpl(namespace, ImmutableDocument, customCodecRegistry, readPreference, ACKNOWLEDGED,
                readConcern, executor)
        def document = new ImmutableDocument(['a': 1])

        when:
        collection.bulkWrite([new InsertOneModel<ImmutableDocument>(document)])

        then:
        !document.containsKey('_id')

        when:
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation
        def request = operation.writeRequests.get(0) as InsertRequest

        then:
        request.getDocument().containsKey('_id')
    }

}
