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

import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.client.MongoCollectionOptions
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
import com.mongodb.connection.AcknowledgedWriteConcernResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.BatchCursor
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

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClient.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.secondary
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def options = MongoCollectionOptions.builder()
                                        .writeConcern(WriteConcern.ACKNOWLEDGED)
                                        .readPreference(secondary())
                                        .codecRegistry(getDefaultCodecRegistry()).build()


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

        when:
        collection.count()
        def operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.count(filter)
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        def hint = new BsonDocument('hint', new BsonInt32(1))
        collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
        operation = executor.getReadOperation() as CountOperation

        then:
        expect operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray([new BsonString('a')]), new BsonArray([new BsonString('b')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.distinct('test', filter)
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(new BsonDocument()))

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.distinct('test', filter, new DistinctOptions().maxTime(100, MILLISECONDS))
        operation = executor.getReadOperation() as DistinctOperation

        then:
        expect operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(filter).maxTime(100, MILLISECONDS))
    }

    def 'should handle exceptions in distinct correctly'() {
        given:
        def options = MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'A failed operation'
        collection.distinct('test', filter)

        then:
        thrown(MongoException)

        when: 'A missing codec'
        collection.distinct('test', new Document())

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use FindOperation correctly'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor, batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new FindOperation(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def bsonOperation = new FindOperation(namespace, new BsonDocumentCodec()).filter(new BsonDocument()).slaveOk(true)

        when:
        collection.find().into([])
        def operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        collection.find(BsonDocument).into([])
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        collection.find(new Document('filter', 1)).into([])
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        collection.find(new Document('filter', 1), BsonDocument).into([])
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))
    }

    def 'should use AggregateOperation correctly'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.aggregate([new Document('$match', 1)]).into([])
        def operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()))

        when:
        collection.aggregate([new Document('$match', 1)], new AggregateOptions().maxTime(100, MILLISECONDS)).into([])
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new DocumentCodec()).maxTime(100, MILLISECONDS))

        when:
        collection.aggregate([new Document('$match', 1)], BsonDocument).into([])
        operation = executor.getReadOperation() as AggregateOperation

        then:
        expect operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                             new BsonDocumentCodec()))
    }

    def 'should handle exceptions in aggregate correctly'() {
        given:
        def options = MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        collection.aggregate([new BsonDocument('$match', new BsonInt32(1))], BsonDocument).into([])

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.aggregate([new Document('$match', 1)])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MapReduceWithInlineResultsOperation correctly'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor, batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                        new DocumentCodec()).verbose(true)
        def bsonOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                    new BsonDocumentCodec()).verbose(true)

        when:
        collection.mapReduce('map', 'reduce').into([])
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation)

        when:
        def mapReduceOptions = new MapReduceOptions().finalizeFunction('final')
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([])
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(documentOperation.finalizeFunction(new BsonJavaScript('final')))

        when:
        collection.mapReduce('map', 'reduce', BsonDocument).into([])
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation)

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).into([])
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        expect operation, isTheSameAs(bsonOperation.finalizeFunction(new BsonJavaScript('final')))

    }

    def 'should use MapReduceToCollectionOperation correctly'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor, batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                   'collectionName').filter(new BsonDocument('filter', new BsonInt32(1)))
                                                                                    .finalizeFunction(new BsonJavaScript('final'))
                                                                                    .verbose(true)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([])
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        expect operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new DocumentCodec())
                                              .filter(new BsonDocument()))

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).into([])
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
        def options = MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        collection.mapReduce('map', 'reduce', BsonDocument).into([])

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.mapReduce('map', 'reduce').into([])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                        ordered, WriteConcern.ACKNOWLEDGED)
        }

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def options = MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use InsertOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])

        when:
        collection.insertOne(new Document('_id', 1))
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use InsertManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { ordered ->
            new InsertOperation(namespace, ordered, WriteConcern.ACKNOWLEDGED,
                                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])
        }

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)])
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true))
        operation = executor.getWriteOperation() as InsertOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use DeleteOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)])

        when:
        collection.deleteOne(new Document('_id', 1))
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DeleteManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])


        when:
        collection.deleteMany(new Document('_id', 1))
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for replaceOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                       new BsonDocument('a', new BsonInt32(10)),
                                                                       WriteRequest.Type.REPLACE)])

        when:
        collection.replaceOne(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for updateOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE).multi(false).upsert(upsert)])
        }

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true))
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use UpdateOperation correctly for updateMany'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE)
                                         .multi(true).upsert(upsert)])
        }

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true))
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, new DocumentCodec()).filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndDelete(new Document('a', 1))
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1)))
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                                     new FindOneAndReplaceOptions().projection(new Document('projection', 1)))
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                                    new FindOneAndUpdateOptions().projection(new Document('projection', 1)))
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        expect operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropCollectionOperation(namespace)

        when:
        collection.dropCollection()
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CreateIndexOperation(namespace, new BsonDocument('key', new BsonInt32(1)))
        when:
        collection.createIndex(new Document('key', 1))
        def operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)

        when:
        collection.createIndex(new Document('key', 1), new CreateIndexOptions().background(true))
        operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation.background(true))
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([[], []])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.getIndexes()
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))

        when:
        collection.getIndexes(BsonDocument)
        operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new BsonDocumentCodec()))
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, 'indexName')

        when:
        collection.dropIndex('indexName')
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*')

        when:
        collection.dropIndexes()
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

        when:
        collection.renameCollection(newNamespace)
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

}
