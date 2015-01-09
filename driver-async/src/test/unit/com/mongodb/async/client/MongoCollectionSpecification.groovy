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

import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoWriteConcernException
import com.mongodb.MongoWriteException
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.WriteConcernResult
import com.mongodb.WriteError
import com.mongodb.async.FutureResultCallback
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteConcernError
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DistinctOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.MapReduceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.DropCollectionOperation
import com.mongodb.operation.DropIndexOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.ListIndexesOperation
import com.mongodb.operation.MapReduceStatistics
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.RenameCollectionOperation
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.bulk.BulkWriteResult.acknowledged
import static com.mongodb.bulk.BulkWriteResult.unacknowledged
import static com.mongodb.bulk.WriteRequest.Type.DELETE
import static com.mongodb.bulk.WriteRequest.Type.INSERT
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def options = OperationOptions.builder()
                                  .writeConcern(WriteConcern.ACKNOWLEDGED)
                                  .readPreference(secondary())
                                  .codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()
    def getOptions = { WriteConcern writeConcern -> options.withWriteConcern(writeConcern) }

    def 'should return the correct name from getName'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should return the correct options'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getOptions() == options
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
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
        collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray([new BsonString('a')]), new BsonArray([new BsonString('b')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<Object>>()

        when:
        collection.distinct('test', filter, futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(new BsonDocument()))

        when:
        futureResultCallback = new FutureResultCallback<List<Object>>()
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.distinct('test', filter, new DistinctOptions().maxTime(100, MILLISECONDS), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(filter).maxTime(100, MILLISECONDS))
    }

    def 'should handle exceptions in distinct correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure'),
                                                  new BsonArray([new BsonString('no document codec')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<Object>>()

        when: 'A failed operation'
        collection.distinct('test', filter, futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(MongoException)

        when: 'An unexpected result'
        futureResultCallback = new FutureResultCallback<List<Object>>()
        collection.distinct('test', filter, futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(MongoException)

        when: 'A missing codec should throw immediately'
        futureResultCallback = new FutureResultCallback<List<Object>>()
        collection.distinct('test', new Document(), futureResultCallback)

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use FindOperation correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new FindOperation(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def bsonOperation = new FindOperation(namespace, new BsonDocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.find().into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.find(BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.find(new Document('filter', 1)).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.find(new Document('filter', 1), BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))
    }

    def 'should use AggregateOperation correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.aggregate([new Document('$match', 1)]).into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()))

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.aggregate([new Document('$match', 1)], new AggregateOptions().maxTime(100, MILLISECONDS)).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()).maxTime(100, MILLISECONDS))

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.aggregate([new Document('$match', 1)], BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new BsonDocumentCodec()))
    }

    def 'aggregate should use AggregateToCollectionOperation correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([null, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def outCollectionName = 'outColl'
        def futureResultCallback = new FutureResultCallback<List<Document>>()
        def pipeline = [new Document('$match', 1), new Document('$out', outCollectionName)]
        def bsonPipeline = new BsonArray([new BsonDocument('$match', new BsonInt32(1)),
                                          new BsonDocument('$out', new BsonString(outCollectionName))])
        when:
        collection.aggregate(pipeline).into([], futureResultCallback)
        futureResultCallback.get()
        def aggregateOperation = executor.getWriteOperation() as AggregateToCollectionOperation
        def findOperation = executor.getReadOperation() as FindOperation

        then:
        expect aggregateOperation, isTheSameAs(new AggregateToCollectionOperation(namespace, bsonPipeline))
        expect findOperation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.getDatabaseName(), outCollectionName),
                                                            options.codecRegistry.get(Document))
                                                  .filter(new BsonDocument()))
    }

    def 'aggregateToCollection should use AggregateToCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def outCollectionName = 'outColl'
        def pipeline = [new Document('$match', 1), new Document('$out', outCollectionName)]
        def bsonPipeline = new BsonArray([new BsonDocument('$match', new BsonInt32(1)),
                                          new BsonDocument('$out', new BsonString(outCollectionName))])
        when:
        def futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.aggregateToCollection(pipeline, futureResultCallback)
        futureResultCallback.get()
        def aggregateOperation = executor.getWriteOperation() as AggregateToCollectionOperation

        then:
        expect aggregateOperation, isTheSameAs(new AggregateToCollectionOperation(namespace, bsonPipeline))

        when:
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.aggregateToCollection(pipeline, new AggregateOptions().allowDiskUse(true), futureResultCallback)
        futureResultCallback.get()
        aggregateOperation = executor.getWriteOperation() as AggregateToCollectionOperation

        then:
        expect aggregateOperation, isTheSameAs(new AggregateToCollectionOperation(namespace, bsonPipeline).allowDiskUse(true))
    }

    def 'aggregateToCollection should throw IllegalArgumentException when last state is not $out'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        def futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.aggregateToCollection([new Document('$match', 1)], futureResultCallback)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should handle exceptions in aggregate correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()

        when: 'The operation fails with an exception'
        collection.aggregate([new BsonDocument('$match', new BsonInt32(1))], BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.aggregate([new Document('$match', 1)])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MapReduceWithInlineResultsOperation correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                        new DocumentCodec()).verbose(true)
        def bsonOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                    new BsonDocumentCodec()).verbose(true)
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.mapReduce('map', 'reduce').into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        def mapReduceOptions = new MapReduceOptions().finalizeFunction('final')
        futureResultCallback = new FutureResultCallback<List<Document>>()
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation.finalizeFunction(new BsonJavaScript('final')))

        when:
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        collection.mapReduce('map', 'reduce', BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation.finalizeFunction(new BsonJavaScript('final')))
    }

    def 'mapReduce should use MapReduceToCollectionOperation correctly'() {
        given:
        def stats = Stub(MapReduceStatistics)
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([stats, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedAggregateOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'),
                                                                            new BsonJavaScript('reduce'), 'collectionName')
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('final'))
                .verbose(true)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedAggregateOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new DocumentCodec())
                                              .filter(new BsonDocument()))
   }

    def 'mapReduceToCollection should use MapReduceToCollectionOperation correctly'() {
        given:
        def stats = Stub(MapReduceStatistics)
        def executor = new TestOperationExecutor([stats])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.mapReduceToCollection('map', 'reduce', mapReduceOptions, futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'),
                                                                         new BsonJavaScript('reduce'), 'collectionName')
                                              .filter(new BsonDocument('filter', new BsonInt32(1)))
                                              .finalizeFunction(new BsonJavaScript('final'))
                                              .verbose(true))
    }

    def 'mapReduceToCollection should throw IllegalArgumentException if inline'() {
        given:
        def executor = new TestOperationExecutor([])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.mapReduceToCollection('map', 'reduce', new MapReduceOptions(), new FutureResultCallback<List<Document>>())

        then:
        thrown(IllegalArgumentException)
    }

    def 'should handle exceptions in mapReduce correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()

        when: 'The operation fails with an exception'
        collection.mapReduce('map', 'reduce', BsonDocument).into([], futureResultCallback)
        futureResultCallback.get()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        collection.mapReduce('map', 'reduce').into([], futureResultCallback)

        then:
        thrown(CodecConfigurationException)
    }

    def 'bulkWrite should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                        ordered, writeConcern)
        }
        def futureResultCallback = new FutureResultCallback<BulkWriteResult>()

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        futureResultCallback = new FutureResultCallback<BulkWriteResult>()
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        futureResultCallback = new FutureResultCallback<BulkWriteResult>()
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(false), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(false))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(), unacknowledged(), unacknowledged()])
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new FutureResultCallback<BulkWriteResult>())

        then:
        thrown(CodecConfigurationException)
    }

    def 'insertOne should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                                            true, writeConcern)
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()

        when:
        collection.insertOne(new Document('_id', 1), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 1, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged()])
    }

    def 'insertMany should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { ordered ->
            new MixedBulkWriteOperation(namespace,
                                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))],
                                ordered, writeConcern)
        }
        def futureResultCallback = new FutureResultCallback<WriteConcernResult>()

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true),
                              futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        futureResultCallback = new FutureResultCallback<WriteConcernResult>()
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(false),
                              futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, []),
                                                                 acknowledged(INSERT, 0, [])])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(), unacknowledged(), unacknowledged()])
    }

    def 'deleteOne should use MixedBulkWriteOperationperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def futureResultCallback = new FutureResultCallback<DeleteResult>()

        when:
        collection.deleteOne(new Document('_id', 1), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace,
                                                                  [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))
                                                                           .multi(false)],
                                                                  true, writeConcern))
        result == expectedResult

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(DELETE, 1, [])]) | DeleteResult.acknowledged(1)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged()])            | DeleteResult.unacknowledged()
    }

    def 'deleteMany should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def futureResultCallback = new FutureResultCallback<DeleteResult>()

        when:
        collection.deleteMany(new Document('_id', 1), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace, [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))
                                                                                      .multi(true)],
                                                                  true, writeConcern))
        result == expectedResult

        where:
        writeConcern                | executor                                                 | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(DELETE, 6, [])]) | DeleteResult.acknowledged(6)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged()])            | DeleteResult.unacknowledged()
    }

    @SuppressWarnings('LineLength')
    def 'replaceOne should use MixedBulkWriteOperationperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.replaceOne(new Document('a', 1), new Document('a', 10), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                                                new BsonDocument('a', new BsonInt32(10)),
                                                                                                REPLACE)],
                                                                  true, writeConcern))
        result == expectedResult

        where:
        writeConcern                | executor                                                        | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, null, [])]) | UpdateResult.acknowledged(1, null, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, 1, [])])    | UpdateResult.acknowledged(1, 1, null)
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(REPLACE, 1, 1,
                                                                              [new BulkWriteUpsert(0, new BsonInt32( 42))])]) | UpdateResult.acknowledged(1, 1, new BsonInt32(42))
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged()])                   | UpdateResult.unacknowledged()
    }

    def 'updateOne should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean upsert ->
            new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                      new BsonDocument('a', new BsonInt32(10)),
                                                                      UPDATE).multi(false).upsert(upsert)],
                                        true, writeConcern)
        }
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<UpdateResult>()
        collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
        result == expectedResult

        where:
        writeConcern                | executor                                                   | expectedResult
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([acknowledged(UPDATE, 1, []),
                                                                 acknowledged(UPDATE, 1, [])])   | UpdateResult.acknowledged(1, 0, null)
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([unacknowledged(),
                                                                 unacknowledged()])              | UpdateResult.unacknowledged()
    }

    def 'should use UpdateOperation correctly for updateMany'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean upsert ->
            new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                      new BsonDocument('a', new BsonInt32(10)),
                                                                      UPDATE)
                                                            .multi(true).upsert(upsert)],
                                        true, writeConcern)
        }
        def futureResultCallback = new FutureResultCallback<UpdateResult>()

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10), futureResultCallback)
        def result = futureResultCallback.get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))
        result == expectedResult

        when:
        futureResultCallback = new FutureResultCallback<UpdateResult>()
        collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true), futureResultCallback)
        result = futureResultCallback.get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
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
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
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
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
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
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
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
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, new DocumentCodec()).filter(new BsonDocument('a', new BsonInt32(1)))
        def futureResultCallback = new FutureResultCallback<Document>()

        when:
        collection.findOneAndDelete(new Document('a', 1), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Document>()
        collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1)),
                                    futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))
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
                                     new FindOneAndReplaceOptions().projection(new Document('projection', 1)), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))
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
                                    new FindOneAndUpdateOptions().projection(new Document('projection', 1)), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))

        where:
        writeConcern                | executor
        WriteConcern.ACKNOWLEDGED   | new TestOperationExecutor([WriteConcernResult.acknowledged(1, true, null),
                                                                 WriteConcernResult.acknowledged(1, true, null)])
        WriteConcern.UNACKNOWLEDGED | new TestOperationExecutor([WriteConcernResult.unacknowledged(),
                                                                 WriteConcernResult.unacknowledged()])
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropCollectionOperation(namespace)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.dropCollection(futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CreateIndexOperation(namespace, new BsonDocument('key', new BsonInt32(1)))
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.createIndex(new Document('key', 1), futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        futureResultCallback = new FutureResultCallback<Void>()
        collection.createIndex(new Document('key', 1), new CreateIndexOptions().background(true), futureResultCallback)
        futureResultCallback.get()
        operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation.background(true))
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def asyncCursor = Stub(AsyncBatchCursor) {
            next(_) >> { args -> args[0].onResult(null, null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def futureResultCallback = new FutureResultCallback<List<Document>>()

        when:
        collection.getIndexes(futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))

        when:
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        collection.getIndexes(BsonDocument, futureResultCallback)
        futureResultCallback.get()
        operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new BsonDocumentCodec()))
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, 'indexName')
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.dropIndex('indexName', futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*')
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
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace)
        def futureResultCallback = new FutureResultCallback<Void>()

        when:
        collection.renameCollection(newNamespace, futureResultCallback)
        futureResultCallback.get()
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

}
