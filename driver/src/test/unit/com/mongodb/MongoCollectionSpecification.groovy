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
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.BatchCursor
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
import static com.mongodb.MongoClient.getDefaultCodecRegistry
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
    def options = MongoCollectionOptions.builder()
                                        .writeConcern(WriteConcern.ACKNOWLEDGED)
                                        .readPreference(secondary())
                                        .codecRegistry(getDefaultCodecRegistry()).build()
    def getOptions = { WriteConcern writeConcern ->
        MongoCollectionOptions.builder().writeConcern(writeConcern).build().withDefaults(options)
    }


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

    def 'bulkWrite should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                        ordered, writeConcern)
        }

        when:
        def result = collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        result = collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        result = collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(false))
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
        def options = MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])

        then:
        thrown(CodecConfigurationException)
    }

    def 'insertOne should use MixedBulkWriteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                                            true, writeConcern)

        when:
        collection.insertOne(new Document('_id', 1))
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

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)])
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true))
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(true))

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(false))
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

        when:
        def result = collection.deleteOne(new Document('_id', 1))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
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

        when:
        def result = collection.deleteMany(new Document('_id', 1))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        result.wasAcknowledged() == writeConcern.isAcknowledged()
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

        when:
        def result = collection.replaceOne(new Document('a', 1), new Document('a', 10))
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
                                                                              [new com.mongodb.bulk.BulkWriteUpsert(0, new BsonInt32( 42))])]) | UpdateResult.acknowledged(1, 1, new BsonInt32(42))
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

        when:
        def result = collection.updateOne(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))
        result == expectedResult

        when:
        result = collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true))
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

    def 'updateMany should use MixedBulkWriteOperationOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
        def expectedOperation = { boolean upsert ->
            new MixedBulkWriteOperation(namespace, [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                      new BsonDocument('a', new BsonInt32(10)),
                                                                      UPDATE)
                                                            .multi(true).upsert(upsert)],
                                        true, writeConcern)
        }

        when:
        def result = collection.updateMany(new Document('a', 1), new Document('a', 10))
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        expect operation, isTheSameAs(expectedOperation(false))
        result == expectedResult

        when:
        result = collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true))
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

    def 'should translate MongoBulkWriteException to MongoWriteException'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

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
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        def e = thrown(MongoWriteConcernException)
        e.writeConcernError == new com.mongodb.bulk.WriteConcernError(42, 'oops', new BsonDocument())
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, getOptions(writeConcern), executor)
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
        def batchCursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([batchCursor, batchCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.getIndexes()
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        expect operation, isTheSameAs(new ListIndexesOperation(namespace, new DocumentCodec()))

        when:
        def indexes = collection.getIndexes(BsonDocument)
        operation = executor.getReadOperation() as ListIndexesOperation
        indexes == []

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
