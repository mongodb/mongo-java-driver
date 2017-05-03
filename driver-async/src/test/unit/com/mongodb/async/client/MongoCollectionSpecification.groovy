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

package com.mongodb.async.client

import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoWriteConcernException
import com.mongodb.MongoWriteException
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.WriteConcernResult
import com.mongodb.WriteError
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.IndexRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteConcernError
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
import com.mongodb.operation.AsyncOperationExecutor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexesOperation
import com.mongodb.operation.DistinctOperation
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
    def codecRegistry = MongoClients.getDefaultCodecRegistry()
    def readPreference = secondary()
    def writeConcern = WriteConcern.ACKNOWLEDGED
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()

    def 'should return the correct name from getName'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern,
                new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should behave correctly when using withDefaultClass'() {
        given:
        def newClass = Worker
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
                .withDocumentClass(newClass)

        then:
        collection.getDocumentClass() == newClass
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, newClass, codecRegistry, readPreference, writeConcern,
                readConcern, executor))
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = Stub(CodecRegistry)
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
                .withCodecRegistry(newCodecRegistry)

        then:
        collection.getCodecRegistry() == newCodecRegistry
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, newCodecRegistry, readPreference, writeConcern,
                readConcern, executor))
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
                .withReadPreference(newReadPreference)

        then:
        collection.getReadPreference() == newReadPreference
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, newReadPreference, writeConcern,
                readConcern, executor))
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
                .withWriteConcern(newWriteConcern)

        then:
        collection.getWriteConcern() == newWriteConcern
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, newWriteConcern,
                readConcern, executor))
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY
        def executor = new TestOperationExecutor([])

        when:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
                .withReadConcern(newReadConcern)

        then:
        collection.getReadConcern() == newReadConcern
        expect collection, isTheSameAs(new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern,
                newReadConcern, executor))
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new CountOperation(namespace).filter(filter)
        def futureResultCallback = new FutureResultCallback<Long>()

        when:
        collection.count(futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Long>()
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.count(filter, futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        futureResultCallback = new FutureResultCallback<Long>()
        def hint = new BsonDocument('hint', new BsonInt32(1))
        collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS).collation(collation),
                futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS)
                .collation(collation))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def codec = codecRegistry.get(String)
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def filter = new BsonDocument('a', new BsonInt32(1))
        def futureResultCallback = new FutureResultCallback<List<String>>()

        when:
        collection.distinct('test', String).into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test', codec).filter(new BsonDocument()))

        when:
        futureResultCallback = new FutureResultCallback<List<String>>()
        collection.distinct('test', String).filter(filter).collation(collation).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test', codec).filter(filter).collation(collation))

        when:
        futureResultCallback = new FutureResultCallback<List<String>>()
        collection.distinct('test', filter, String).maxTime(100, MILLISECONDS).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test', codec).filter(filter).maxTime(100, MILLISECONDS))
    }

    def 'should create DistinctIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def distinctIterable = collection.distinct('field', String)

        then:
        expect distinctIterable, isTheSameAs(new DistinctIterableImpl(namespace, Document, String, codecRegistry, readPreference,
                readConcern, executor, 'field', new BsonDocument()))
    }

    def 'should create FindIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def findIterable = collection.find()

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, readConcern,
                executor, new BsonDocument(), new FindOptions()))

        when:
        findIterable = collection.find(BsonDocument)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, new BsonDocument(), new FindOptions()))

        when:
        findIterable = collection.find(new Document())

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, readConcern,
                executor, new Document(), new FindOptions()))

        when:
        findIterable = collection.find(new Document(), BsonDocument)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl(namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, new Document(), new FindOptions()))
    }

    def 'should use AggregateIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def aggregateIterable = collection.aggregate([new Document('$match', 1)])

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)]))

        when:
        aggregateIterable = collection.aggregate([new Document('$match', 1)], BsonDocument)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl(namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)]))
    }

    def 'should validate the aggregation pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        collection.aggregate(null)

        then:
        thrown(IllegalArgumentException)

        when:
        def results = new FutureResultCallback()
        collection.aggregate([null]).into([], results)
        results.get()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create MapReduceIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def mapReduceIterable = collection.mapReduce('map', 'reduce')

        then:
        expect mapReduceIterable, isTheSameAs(new MapReduceIterableImpl(namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce'))
    }

    def 'bulkWrite should use MixedBulkWriteOperation correctly'() {
        given:
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
        def futureResultCallback = new FutureResultCallback<BulkWriteResult>()

        when:
        collection.bulkWrite(bulkOperations, futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        def expected = expectedOperation(true, writeConcern, null)
        expect operation, isTheSameAs(expected)

        when:
        futureResultCallback = new FutureResultCallback<BulkWriteResult>()
        collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(true).bypassDocumentValidation(true), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true))

        when:
        futureResultCallback = new FutureResultCallback<BulkWriteResult>()
        collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(false).bypassDocumentValidation(false), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, false))

        where:

        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(), unacknowledged(), unacknowledged()])
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        collection.bulkWrite(null, new FutureResultCallback<BulkWriteResult>())

        then:
        thrown(IllegalArgumentException)

        when:
        collection.bulkWrite([null], new FutureResultCallback<BulkWriteResult>())

        then:
        thrown(IllegalArgumentException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new FutureResultCallback<BulkWriteResult>())

        then:
        thrown(CodecConfigurationException)
    }

    def 'insertOne should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { WriteConcern wc, Boolean bypassDocumentValidation ->
            new MixedBulkWriteOperation(namespace,
                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                    true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()

        when:
        collection.insertOne(new Document('_id', 1), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, null))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertOne(new Document('_id', 1), new InsertOneOptions().bypassDocumentValidation(true), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, true))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertOne(new Document('_id', 1), new InsertOneOptions().bypassDocumentValidation(false), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(writeConcern, false))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(), unacknowledged(), unacknowledged()])
    }

    def 'insertMany should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean ordered, WriteConcern wc, Boolean bypassDocumentValidation ->
            new MixedBulkWriteOperation(namespace,
                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                     new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))],
                    ordered, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, null))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)],
                new InsertManyOptions().ordered(true).bypassDocumentValidation(true), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)],
                new InsertManyOptions().ordered(false).bypassDocumentValidation(false), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, false))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(), unacknowledged(), unacknowledged()])
    }

    def 'should validate the insertMany data correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern,
                Stub(AsyncOperationExecutor))
        def callback = Stub(SingleResultCallback)

        when:
        collection.insertMany(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.insertMany([null], callback)

        then:
        thrown(IllegalArgumentException)
    }

    def 'deleteOne should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<DeleteResult>()

        when:
        collection.deleteOne(new Document('_id', 1), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)], true, writeConcern))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<DeleteResult>()
        collection.deleteOne(new Document('_id', 1), new DeleteOptions().collation(collation), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false).collation(collation)], true, writeConcern))
        result == expectedResult

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(DELETE, 1, []),
                                                                 acknowledged(DELETE, 1, [])]) | DeleteResult.acknowledged(1)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])            | DeleteResult.unacknowledged()
    }

    def 'deleteOne should translate BulkWriteException correctly'() {
        given:
        def bulkWriteException = new MongoBulkWriteException(acknowledged(0, 0, 1, null, []), [],
                                                             new WriteConcernError(100, '', new BsonDocument()),
                                                             new ServerAddress());

        def executor = new TestOperationExecutor([bulkWriteException])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, WriteConcern.ACKNOWLEDGED,
                                                 readConcern, executor)
        def futureResultCallback = new FutureResultCallback<DeleteResult>()

        when:
        collection.deleteOne(new Document('_id', 1), futureResultCallback)
        futureResultCallback.get()

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
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<DeleteResult>()

        when:
        collection.deleteMany(new Document('_id', 1), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(true)], true, writeConcern))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<DeleteResult>()
        collection.deleteMany(new Document('_id', 1), new DeleteOptions().collation(collation), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(true).collation(collation)], true, writeConcern))
        result == expectedResult

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(DELETE, 6, []),
                                                                 acknowledged(DELETE, 6, [])]) | DeleteResult.acknowledged(6)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])            | DeleteResult.unacknowledged()
    }

    @SuppressWarnings('LineLength')
    def 'replaceOne should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), REPLACE)
                             .collation(collation).upsert(upsert)], true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.replaceOne(new Document('a', 1), new Document('a', 10),
                new UpdateOptions().bypassDocumentValidation(bypassDocumentValidation), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, bypassDocumentValidation, null))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<UpdateResult>()
        collection.replaceOne(new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(bypassDocumentValidation).collation(collation),
                futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, bypassDocumentValidation, collation))
        result == expectedResult

        where:
        bypassDocumentValidation << [null, true, false, null]
        writeConcern                | executor                                                        | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, null, []),
                                                                 acknowledged(REPLACE, 1, null, [])]) | UpdateResult.acknowledged(1, null, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, 1, []),
                                                                 acknowledged(REPLACE, 1, 1, [])])    | UpdateResult.acknowledged(1, 1, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([
                acknowledged(REPLACE, 1, 1, [new BulkWriteUpsert(0, new BsonInt32(42))]),
                acknowledged(REPLACE, 1, 1, [new BulkWriteUpsert(0, new BsonInt32(42))])])            | UpdateResult.acknowledged(1, 1, new BsonInt32(42))
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])                   | UpdateResult.unacknowledged()
    }

    def 'replaceOne should translate BulkWriteException correctly'() {
        given:
        def bulkWriteException = new MongoBulkWriteException(bulkWriteResult, [],
                                                             new WriteConcernError(100, '', new BsonDocument()),
                                                             new ServerAddress());

        def executor = new TestOperationExecutor([bulkWriteException])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, WriteConcern.ACKNOWLEDGED,
                                                 readConcern, executor)
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.replaceOne(new Document('_id', 1), new Document('_id', 1), futureResultCallback)
        futureResultCallback.get()

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError == bulkWriteException.writeConcernError
        ex.writeResult.wasAcknowledged() == writeResult.wasAcknowledged()
        ex.writeResult.count == writeResult.count
        ex.writeResult.updateOfExisting == writeResult.updateOfExisting
        ex.writeResult.upsertedId == writeResult.upsertedId

        where:
        bulkWriteResult                                                       | writeResult
        acknowledged(0, 1, 0, 1, [])                                          | WriteConcernResult.acknowledged(1, true, null)
        acknowledged(0, 0, 0, 0, [new BulkWriteUpsert(0, new BsonInt32(1))])  | WriteConcernResult.acknowledged(1, false, new BsonInt32(1))
    }


    def 'updateOne should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), UPDATE)
                           .multi(false).upsert(upsert).collation(collation)], true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, null, null))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<UpdateResult>()
        collection.updateOne(new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true).collation(collation), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true, collation))
        result == expectedResult

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(UPDATE, 1, []),
                                                                 acknowledged(UPDATE, 1, [])]) | UpdateResult.acknowledged(1, 0, null)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])            | UpdateResult.unacknowledged()
    }

    def 'should use UpdateOperation correctly for updateMany'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = { boolean upsert, WriteConcern wc, Boolean bypassDocumentValidation, Collation collation ->
            new MixedBulkWriteOperation(namespace,
                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)), new BsonDocument('a', new BsonInt32(10)), UPDATE)
                             .multi(true).upsert(upsert).collation(collation)], true, wc).bypassDocumentValidation(bypassDocumentValidation)
        }
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false, writeConcern, null, null))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<UpdateResult>()
        collection.updateMany(new Document('a', 1), new Document('a', 10),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true).collation(collation), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true, writeConcern, true, collation))
        result == expectedResult

        where:
        writeConcern                | executor                                                    | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(UPDATE, 5, 3, []),
                                                                 acknowledged(UPDATE, 5, 3, [])]) | UpdateResult.acknowledged(5, 3, null)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])               | UpdateResult.unacknowledged()
    }

    def 'write operation should translate MongoBulkWriteException to MongoWriteException'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<Void>()
        collection.insertOne(new Document('_id', 1), futureResultCallback)

        when:
        futureResultCallback.get()

        then:
        def e = thrown(MongoWriteException)
        e.error == new WriteError(11000, 'oops', new BsonDocument())

        where:
        executor << new TestOperationExecutor([new MongoBulkWriteException(acknowledged(INSERT, 1, []),
                [new BulkWriteError(11000, 'oops', new BsonDocument(), 0)],
                null, new ServerAddress())])
    }

    def 'write operation should translate MongoBulkWriteException to MongoWriteConcernException'() {
        given:
        def executor = new TestOperationExecutor([new MongoBulkWriteException(acknowledged(INSERT, 1, []), [],
                new WriteConcernError(42, 'oops', new BsonDocument()),
                new ServerAddress())])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<Void>()
        collection.insertOne(new Document('_id', 1), futureResultCallback)

        when:
        futureResultCallback.get()

        then:
        def e = thrown(MongoWriteConcernException)
        e.writeConcernError == new WriteConcernError(42, 'oops', new BsonDocument())
    }

    def 'write operation should pass other exception through'() {
        given:
        def exception = new MongoSocketReadException('oops', new ServerAddress())
        def executor = new TestOperationExecutor([exception]);
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<Void>()
        collection.insertOne(new Document('_id', 1), futureResultCallback)

        when:
        futureResultCallback.get()

        then:
        def e = thrown(MongoSocketReadException)
        e.is(exception)
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, writeConcern, new DocumentCodec())
                .filter(new BsonDocument('a', new BsonInt32(1)))

        def futureResultCallback = new FutureResultCallback<Document>()

        when:
        collection.findOneAndDelete(new Document('a', 1), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1))
                .maxTime(100, MILLISECONDS).collation(collation), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).collation(collation))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, writeConcern, new DocumentCodec(),
                new BsonDocument('a', new BsonInt32(10))).filter(new BsonDocument('a', new BsonInt32(1)))

        def futureResultCallback = new FutureResultCallback<Document>()

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                new FindOneAndReplaceOptions().projection(new Document('projection', 1)).maxTime(100, MILLISECONDS)
                        .bypassDocumentValidation(false), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(false))

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                new FindOneAndReplaceOptions().projection(new Document('projection', 1)).maxTime(100, MILLISECONDS)
                        .bypassDocumentValidation(true).collation(collation), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, writeConcern, new DocumentCodec(),
                new BsonDocument('a', new BsonInt32(10))).filter(new BsonDocument('a', new BsonInt32(1)))

        def futureResultCallback = new FutureResultCallback<Document>()

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                new FindOneAndUpdateOptions().projection(new Document('projection', 1))
                        .maxTime(100, MILLISECONDS).bypassDocumentValidation(false), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(false))

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                new FindOneAndUpdateOptions().projection(new Document('projection', 1))
                        .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(100, MILLISECONDS).bypassDocumentValidation(true).collation(collation))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new DropCollectionOperation(namespace, writeConcern)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.drop(futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def expectedOperation = new CreateIndexesOperation(namespace,
                [new IndexRequest(new BsonDocument('key', new BsonInt32(1)))], writeConcern)
        def futureResultCallback = new FutureResultCallback<String>()
        collection.createIndex(new Document('key', 1), futureResultCallback)
        def indexName = futureResultCallback.get()
        def operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        indexName == 'key_1'

        when:
        expectedOperation = new CreateIndexesOperation(namespace,
                [new IndexRequest(new BsonDocument('key', new BsonInt32(1))),
                 new IndexRequest(new BsonDocument('key1', new BsonInt32(1)))], writeConcern)
        futureResultCallback = new FutureResultCallback<List<String>>()
        collection.createIndexes([new IndexModel(new Document('key', 1)),
                                  new IndexModel(new Document('key1', 1))], futureResultCallback)
        def indexNames = futureResultCallback.get()
        operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
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
                         .collation(collation)],
                writeConcern)
        futureResultCallback = new FutureResultCallback<String>()
        collection.createIndex(new Document('key', 1), new IndexOptions()
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
                .collation(collation), futureResultCallback)
        indexName = futureResultCallback.get()
        operation = executor.getWriteOperation() as CreateIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
        indexName == 'aIndex'
    }

    def 'should validate the createIndexes data correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern,
                Stub(AsyncOperationExecutor))
        def callback = Stub(SingleResultCallback)

        when:
        collection.createIndexes(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        collection.createIndexes([null], callback)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.listIndexes().into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))

        when:
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        collection.listIndexes(BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new BsonDocumentCodec()))
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)

        when:
        def expectedOperation = new DropIndexOperation(namespace, 'indexName', writeConcern)
        def futureResultCallback = new FutureResultCallback<Void>()
        collection.dropIndex('indexName', futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        def keys = new BsonDocument('x', new BsonInt32(1))
        expectedOperation = new DropIndexOperation(namespace, keys, writeConcern)
        futureResultCallback = new FutureResultCallback<Void>()
        collection.dropIndex(keys, futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*', writeConcern)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.dropIndexes(futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use RenameCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, codecRegistry, readPreference, writeConcern, readConcern, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace, writeConcern)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.renameCollection(newNamespace, futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should not expect to mutate the document when inserting'() {
        given:
        def executor = new TestOperationExecutor([null])
        def customCodecRegistry = fromRegistries(fromProviders(new ImmutableDocumentCodecProvider()), codecRegistry)
        def collection = new MongoCollectionImpl(namespace, ImmutableDocument, customCodecRegistry, readPreference, writeConcern,
                readConcern, executor)
        def document = new ImmutableDocument(['a': 1])
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.insertOne(document, futureResultCallback)
        futureResultCallback.get()

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
        def collection = new MongoCollectionImpl(namespace, ImmutableDocument, customCodecRegistry, readPreference, writeConcern,
                readConcern, executor)
        def document = new ImmutableDocument(['a': 1])
        def futureResultCallback = new FutureResultCallback<BulkWriteResult>()

        when:
        collection.bulkWrite([new InsertOneModel<ImmutableDocument>(document)], futureResultCallback)
        futureResultCallback.get()

        then:
        !document.containsKey('_id')

        when:
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation
        def request = operation.writeRequests.get(0) as InsertRequest

        then:
        request.getDocument().containsKey('_id')
    }

}
