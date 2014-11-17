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

package com.mongodb.async.rx.client;

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.async.MongoAsyncCursor
import com.mongodb.async.SingleResultFuture
import com.mongodb.async.client.TestOperationExecutor
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
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
import com.mongodb.connection.AcknowledgedWriteConcernResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexOperation
import com.mongodb.operation.DeleteOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.DropCollectionOperation
import com.mongodb.operation.DropIndexOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.ListIndexesOperation
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.RenameCollectionOperation
import com.mongodb.operation.UpdateOperation
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

import static com.mongodb.ReadPreference.secondary
import static com.mongodb.async.client.CustomMatchers.isTheSameAs
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect
import static com.mongodb.async.rx.client.Helpers.get

class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def options = OperationOptions.builder()
                                  .writeConcern(WriteConcern.ACKNOWLEDGED)
                                  .readPreference(secondary())
                                  .codecRegistry(com.mongodb.async.client.MongoClientImpl.getDefaultCodecRegistry()).build()

    def 'should return the correct name from getName'() {
        given:
        def collection = getCollection(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should return the correct options'() {
        given:
        def collection = getCollection(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getOptions() == options
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1, 2, 3])
        def filter = new BsonDocument()
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new CountOperation(namespace).filter(filter)

        when:
        get(collection.count())
        def operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        get(collection.count(filter))
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        def hint = new BsonDocument('hint', new BsonInt32(1))
        get(collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS)))
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray([new BsonString('a')]), new BsonArray([new BsonString('b')])])
        def filter = new BsonDocument()
        def collection = getCollection(namespace, Document, options, executor)

        when:
        get(collection.distinct('test', filter))
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(new BsonDocument()))

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        get(collection.distinct('test', filter, new DistinctOptions().maxTime(100, MILLISECONDS)))
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
        def collection = getCollection(namespace, Document, options, executor)

        when: 'A failed operation'
        get(collection.distinct('test', filter))

        then:
        thrown(MongoException)

        when: 'An unexpected result'
        get(collection.distinct('test', filter))

        then:
        thrown(MongoException)

        when: 'A missing codec should throw immediately'
        get(collection.distinct('test', new Document()))

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use FindOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = getCollection(namespace, Document, options, executor)
        def documentOperation = new FindOperation(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def bsonOperation = new FindOperation(namespace, new BsonDocumentCodec()).filter(new BsonDocument()).slaveOk(true)

        when:
        get(collection.find().toObservable())
        def operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        get(collection.find(BsonDocument).toObservable())
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        get(collection.find(new Document('filter', 1)).toObservable())
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        get(collection.find(new Document('filter', 1), BsonDocument).toObservable())
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))
    }

    def 'should use AggregateOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor])
        def collection = getCollection(namespace, Document, options, executor)

        when:
        get(collection.aggregate([new Document('$match', 1)]).toObservable())
        def operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()))

        when:
        get(collection.aggregate([new Document('$match', 1)], new AggregateOptions().maxTime(100, MILLISECONDS)).toObservable())
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()).maxTime(100, MILLISECONDS))

        when:
        get(collection.aggregate([new Document('$match', 1)], BsonDocument).toObservable())
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                           new BsonDocumentCodec()))
    }

    def 'should handle exceptions in aggregate correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = getCollection(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        get(collection.aggregate([new BsonDocument('$match', new BsonInt32(1))], BsonDocument).toObservable())

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.aggregate([new Document('$match', 1)])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MapReduceWithInlineResultsOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = getCollection(namespace, Document, options, executor)
        def documentOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                        new DocumentCodec()).verbose(true)
        def bsonOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                    new BsonDocumentCodec()).verbose(true)

        when:
        get(collection.mapReduce('map', 'reduce').toObservable())
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        def mapReduceOptions = new MapReduceOptions().finalizeFunction('final')
        get(collection.mapReduce('map', 'reduce', mapReduceOptions).toObservable())
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation.finalizeFunction(new BsonJavaScript('final')))

        when:
        get(collection.mapReduce('map', 'reduce', BsonDocument).toObservable())
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        get(collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).toObservable())
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation.finalizeFunction(new BsonJavaScript('final')))

    }

    def 'should use MapReduceToCollectionOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                   'collectionName').filter(new BsonDocument('filter', new BsonInt32(1)))
                                                                                    .finalizeFunction(new BsonJavaScript('final'))
                                                                                    .verbose(true)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')

        when:
        get(collection.mapReduce('map', 'reduce', mapReduceOptions).toObservable())
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new DocumentCodec())
                                            .filter(new BsonDocument()))

        when:
        get(collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).toObservable())
        operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'),
                                                        new BsonDocumentCodec())
                                            .filter(new BsonDocument()))
    }

    def 'should handle exceptions in mapReduce correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = getCollection(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        get(collection.mapReduce('map', 'reduce', BsonDocument).toObservable())

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.mapReduce('map', 'reduce').toObservable()

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                        ordered, WriteConcern.ACKNOWLEDGED)
        }

        when:
        get(collection.bulkWrite([new InsertOneModel(new Document('_id', 1))]))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        get(collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true)))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = getCollection(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        get(collection.bulkWrite([new InsertOneModel(new Document('_id', 1))]))

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use InsertOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])

        when:
        get(collection.insertOne(new Document('_id', 1)))
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use InsertManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = { ordered ->
            new InsertOperation(namespace, ordered, WriteConcern.ACKNOWLEDGED,
                                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])
        }

        when:
        get(collection.insertMany([new Document('_id', 1), new Document('_id', 2)]))
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        get(collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true)))
        operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use DeleteOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)])

        when:
        get(collection.deleteOne(new Document('_id', 1)))
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DeleteManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])


        when:
        get(collection.deleteMany(new Document('_id', 1)))
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for replaceOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                       new BsonDocument('a', new BsonInt32(10)),
                                                                       WriteRequest.Type.REPLACE)])

        when:
        get(collection.replaceOne(new Document('a', 1), new Document('a', 10)))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for updateOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE).multi(false).upsert(upsert)])
        }

        when:
        get(collection.updateOne(new Document('a', 1), new Document('a', 10)))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        get(collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)))
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use UpdateOperation correctly for updateMany'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE)
                                         .multi(true).upsert(upsert)])
        }

        when:
        get(collection.updateMany(new Document('a', 1), new Document('a', 10)))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        get(collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)))
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, new DocumentCodec()).filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        get(collection.findOneAndDelete(new Document('a', 1)))
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        get(collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1))))
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        get(collection.findOneAndReplace(new Document('a', 1), new Document('a', 10)))
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        get(collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                                         new FindOneAndReplaceOptions().projection(new Document('projection', 1))))
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        get(collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10)))
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        get(collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                                        new FindOneAndUpdateOptions().projection(new Document('projection', 1))))
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new DropCollectionOperation(namespace)

        when:
        get(collection.dropCollection())
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new CreateIndexOperation(namespace, new BsonDocument('key', new BsonInt32(1)))
        when:
        get(collection.createIndex(new Document('key', 1)))
        def operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        get(collection.createIndex(new Document('key', 1), new CreateIndexOptions().background(true)))
        operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation.background(true))
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([[]])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new ListIndexesOperation(namespace, new DocumentCodec())

        when:
        get(collection.getIndexes())
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, 'indexName')

        when:
        get(collection.dropIndex('indexName'))
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = getCollection(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*')

        when:
        get(collection.dropIndexes())
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use RenameCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = getCollection(namespace, Document, options, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace)

        when:
        get(collection.renameCollection(newNamespace))
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def getCollection(namespace, clazz, options, executor) {
        new MongoCollectionImpl(new com.mongodb.async.client.MongoCollectionImpl(namespace, clazz, options, executor))
    }

}
