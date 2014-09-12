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

import com.mongodb.client.MongoCollectionOptions
import com.mongodb.client.model.AggregateModel
import com.mongodb.client.model.BulkWriteModel
import com.mongodb.client.model.CountModel
import com.mongodb.client.model.DeleteManyModel
import com.mongodb.client.model.DeleteOneModel
import com.mongodb.client.model.DistinctModel
import com.mongodb.client.model.FindModel
import com.mongodb.client.model.FindOneAndDeleteModel
import com.mongodb.client.model.FindOneAndReplaceModel
import com.mongodb.client.model.FindOneAndUpdateModel
import com.mongodb.client.model.InsertManyModel
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.UpdateManyModel
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.codecs.DocumentCodec
import com.mongodb.codecs.DocumentCodecProvider
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.CountOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.FindAndRemoveOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.QueryOperation
import com.mongodb.operation.RemoveOperation
import com.mongodb.operation.ReplaceOperation
import com.mongodb.operation.UpdateOperation
import com.mongodb.protocol.AcknowledgedWriteResult
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.codecs.configuration.RootCodecRegistry
import org.bson.types.ObjectId
import org.mongodb.Document
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ReadPreference.secondary

class NewMongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def collection;
    def options = MongoCollectionOptions.builder().writeConcern(WriteConcern.JOURNALED)
                                        .readPreference(secondary())
                                        .codecRegistry(new RootCodecRegistry([new DocumentCodecProvider()]))
                                        .build()

    def 'should get namespace'() {
        given:
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, new TestOperationExecutor([]))

        expect:
        collection.namespace == namespace
    }

    def 'should get options'() {
        given:
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, new TestOperationExecutor([]))

        expect:
        collection.options == options
    }

    def 'insertOne should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        def operation = executor.getWriteOperation() as InsertOperation
        operation.insertRequests[0].document == new BsonDocument('_id', new BsonInt32(1))
    }

    def 'insert should add _id to document'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)


        def document = new Document()
        when:
        collection.insertOne(document)

        then:
        document.containsKey('_id')
        document.get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
    }

    def 'insertMany should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(2, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.insertMany(new InsertManyModel<Document>([new Document('_id', 1), new Document('_id', 2)]));

        then:
        def operation = executor.getWriteOperation() as InsertOperation
        operation.insertRequests[0].document == new BsonDocument('_id', new BsonInt32(1))
        operation.insertRequests[1].document == new BsonDocument('_id', new BsonInt32(2))
    }

    def 'insertMany should add _id to documents'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(2, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def documents = [new Document(), new Document()]
        when:
        collection.insertMany(new InsertManyModel<Document>(documents));

        then:
        documents[0].containsKey('_id')
        documents[0].get('_id') instanceof ObjectId
        documents[1].containsKey('_id')
        documents[1].get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
    }

    def 'replace should use ReplaceOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.replaceOne(new ReplaceOneModel<>(new Document('_id', 1),
                                                                 new Document('color', 'blue'))
                                                   .upsert(true))

        then:
        def operation = executor.getWriteOperation() as ReplaceOperation

        def replaceRequest = operation.replaceRequests[0]
        !replaceRequest.multi
        replaceRequest.upsert
        replaceRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        replaceRequest.replacement == new BsonDocument('color', new BsonString('blue'))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'updateOne should use UpdateOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.updateOne(new UpdateOneModel<>(new Document('_id', 1),
                                                               new Document('$set', new Document('color', 'blue')))
                                                  .upsert(true))

        then:
        def operation = executor.getWriteOperation() as UpdateOperation

        def updateRequest = operation.updateRequests[0]
        !updateRequest.multi
        updateRequest.upsert
        updateRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        updateRequest.updateOperations == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'updateMany should use UpdateOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.updateMany(new UpdateManyModel<>(new Document('_id', 1),
                                                                 new Document('$set', new Document('color', 'blue')))
                                                   .upsert(true))

        then:
        def operation = executor.getWriteOperation() as UpdateOperation

        def updateRequest = operation.updateRequests[0]
        updateRequest.multi
        updateRequest.upsert
        updateRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        updateRequest.updateOperations == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'deleteOne should use RemoveOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.deleteOne(new DeleteOneModel<>(new Document('_id', 1)));

        then:
        def operation = executor.getWriteOperation() as RemoveOperation

        def removeRequest = operation.removeRequests[0]
        !removeRequest.multi
        removeRequest.criteria == new BsonDocument('_id', new BsonInt32(1))

        result.deletedCount == 1
    }

    def 'deleteMany should use RemoveOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.deleteMany(new DeleteManyModel<>(new Document('_id', 1)));

        then:
        def operation = executor.getWriteOperation() as RemoveOperation

        def updateRequest = operation.removeRequests[0]
        updateRequest.multi
        updateRequest.criteria == new BsonDocument('_id', new BsonInt32(1))

        result.deletedCount == 1
    }

    def 'find should use FindOperation properly'() {
        given:
        def document = new Document('_id', 1)
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        cursor.next() >> document
        def executor = new TestOperationExecutor([cursor, cursor])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def model = new FindModel().criteria(new Document('cold', true))
                                   .batchSize(4)
                                   .maxTime(1, TimeUnit.SECONDS)
                                   .skip(5)
                                   .limit(100)
                                   .modifiers(new Document('$hint', 'i1'))
                                   .projection(new Document('x', 1))
                                   .sort(new Document('y', 1))

        def result = collection.find(model).into([])

        then:
        def operation = executor.getReadOperation() as QueryOperation
        operation.with {
            criteria == new BsonDocument('cold', BsonBoolean.TRUE)
            batchSize == 4
            getMaxTime(TimeUnit.SECONDS) == 1
            skip == 5
            limit == 100
            modifiers == new BsonDocument('$hint', new BsonString('i1'))
            projection == new BsonDocument('x', new BsonInt32(1))
            sort == new BsonDocument('y', new BsonInt32(1))
            cursorFlags == EnumSet.noneOf(CursorFlag)
        }
        executor.readPreference == secondary()
        result == [document]

        when: 'all the boolean properties are enabled'
        model = new FindModel().awaitData(true)
                               .exhaust(true)
                               .noCursorTimeout(true)
                               .partial(true)
                               .tailable(true)
                               .oplogReplay(true)

        collection.find(model).into([])

        then: 'cursor flags contains all flags'
        def operation2 = executor.getReadOperation() as QueryOperation
        operation2.cursorFlags == EnumSet.of(CursorFlag.AWAIT_DATA, CursorFlag.EXHAUST, CursorFlag.NO_CURSOR_TIMEOUT, CursorFlag.PARTIAL,
                                             CursorFlag.TAILABLE, CursorFlag.OPLOG_REPLAY);
    }

    def 'count should use CountOperation properly'() {
        given:
        def executor = new TestOperationExecutor([42L])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new CountModel().criteria(new Document('cold', true))
                                    .maxTime(1, TimeUnit.SECONDS)
                                    .skip(5)
                                    .limit(100)
                                    .hint(new Document('x', 1))

        when:
        def result = collection.count(model)

        then:
        def operation = executor.getReadOperation() as CountOperation
        operation.criteria == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.skip == 5
        operation.limit == 100
        operation.hint == new BsonDocument('x', new BsonInt32(1))
        executor.readPreference == secondary()
        result == 42
    }

    def 'count should use CountOperation properly with hint string'() {
        given:
        def executor = new TestOperationExecutor([42L])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new CountModel().hintString('idx1')

        when:
        collection.count(model)

        then:
        def operation = executor.getReadOperation() as CountOperation
        operation.hint == new BsonString('idx1')
    }

    def 'distinct should use DistinctOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray(Arrays.asList(new BsonString('foo'), new BsonInt32(42)))])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new DistinctModel('fieldName1').criteria(new Document('cold', true))
                                                   .maxTime(1, TimeUnit.SECONDS)

        when:
        def result = collection.distinct(model)

        then:
        def operation = executor.getReadOperation() as DistinctOperation
        operation.criteria == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        executor.readPreference == secondary()
        result == ['foo', 42]
    }

    def 'bulk insert should use BulkWriteOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new com.mongodb.protocol.AcknowledgedBulkWriteResult(1, 0, 0, 0, [])])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def document = new Document();

        when:
        collection.bulkWrite(new BulkWriteModel<>([new InsertOneModel<>(document),
                                                   new UpdateOneModel<>(new Document('_id', 1),
                                                                        new Document('$set', new Document('color', 'blue')))
                                                           .upsert(true),
                                                   new UpdateManyModel<>(new Document('_id', 1),
                                                                         new Document('$set', new Document('color', 'blue')))
                                                           .upsert(true),
                                                   new ReplaceOneModel<>(new Document('_id', 1),
                                                                         new Document('color', 'blue'))
                                                           .upsert(true),
                                                   new DeleteOneModel<>(new Document('_id', 1)),
                                                   new DeleteManyModel<>(new Document('_id', 1))])
                                     .ordered(false))


        then:
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        def insertRequest = operation.writeRequests[0] as com.mongodb.operation.InsertRequest
        insertRequest.document == new BsonDocument('_id', new BsonObjectId(document.getObjectId('_id')))

        def updateOneRequest = operation.writeRequests[1] as com.mongodb.operation.UpdateRequest
        !updateOneRequest.multi
        updateOneRequest.upsert
        updateOneRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        updateOneRequest.updateOperations == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        def updateManyRequest = operation.writeRequests[2] as com.mongodb.operation.UpdateRequest
        updateManyRequest.multi
        updateManyRequest.upsert
        updateManyRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        updateManyRequest.updateOperations == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        def replaceRequest = operation.writeRequests[3] as com.mongodb.operation.ReplaceRequest
        !replaceRequest.multi
        replaceRequest.upsert
        replaceRequest.criteria == new BsonDocument('_id', new BsonInt32(1))
        replaceRequest.replacement == new BsonDocument('color', new BsonString('blue'))

        def deleteOneRequest = operation.writeRequests[4] as com.mongodb.operation.RemoveRequest
        !deleteOneRequest.multi
        deleteOneRequest.criteria == new BsonDocument('_id', new BsonInt32(1))

        def deleteManyRequest = operation.writeRequests[5] as com.mongodb.operation.RemoveRequest
        deleteManyRequest.multi
        deleteManyRequest.criteria == new BsonDocument('_id', new BsonInt32(1))

        document.containsKey('_id')
    }

    def 'bulk insert should generate _id'() {
        given:
        def executor = new TestOperationExecutor([new com.mongodb.protocol.AcknowledgedBulkWriteResult(1, 0, 0, 0, [])])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def document = new Document();

        when:
        collection.bulkWrite(new BulkWriteModel<>([new InsertOneModel<>(document),]))


        then:
        document.containsKey('_id')
    }

    def 'aggregate should use AggregationOperation properly'() {
        given:
        def document = new Document('_id', 1)
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        cursor.next() >> document
        def executor = new TestOperationExecutor([cursor])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new AggregateModel([new Document('$match', new Document('job', 'plumber'))])
                .allowDiskUse(true)
                .batchSize(10)
                .maxTime(1, TimeUnit.SECONDS)
                .useCursor(true)

        when:
        def result = collection.aggregate(model).into([])

        then:
        def operation = executor.getReadOperation() as AggregateOperation
        operation.pipeline == [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber')))]
        operation.batchSize == 10
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.useCursor
        executor.readPreference == secondary()
        result == [document]
    }

    def 'aggregate should use AggregationToCollectionOperation properly'() {
        given:
        def document = new Document('_id', 1)
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        cursor.next() >> document
        def executor = new TestOperationExecutor([null, cursor])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new AggregateModel([new Document('$match', new Document('job', 'plumber')),
                                        new Document('$out', 'outCollection')])
                .allowDiskUse(true)
                .batchSize(10)
                .maxTime(1, TimeUnit.SECONDS)
                .useCursor(true)

        when:
        def result = collection.aggregate(model).into([])

        then:
        def aggregateToCollectionOperation = executor.getWriteOperation() as AggregateToCollectionOperation
        aggregateToCollectionOperation != null
        aggregateToCollectionOperation.pipeline == [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString('outCollection'))]
        aggregateToCollectionOperation.getMaxTime(TimeUnit.SECONDS) == 1

        def queryOperation = executor.getReadOperation() as QueryOperation
        queryOperation != null
        queryOperation.namespace == new MongoNamespace(namespace.getDatabaseName(), 'outCollection')
        queryOperation.decoder instanceof DocumentCodec

        executor.readPreference == secondary()
        result == [document]
    }

    def 'findOneAndDelete should use FindAndDeleteOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def model = new FindOneAndDeleteModel(new Document('cold', true))
                .projection(new Document('field', 1))
                .sort(new Document('sort', -1))

        when:
        def result = collection.findOneAndDelete(model)

        then:
        def operation = executor.getWriteOperation() as FindAndRemoveOperation
        operation.getCriteria() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))

        result == returnedDocument
    }

    def 'findOneAndReplace should use FindOneAndReplaceOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument, returnedDocument])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def model = new FindOneAndReplaceModel(new Document('cold', true), new Document('hot', false))
                .projection(new Document('field', 1))
                .sort(new Document('sort', -1))

        when:
        def result = collection.findOneAndReplace(model)

        then:
        def operation = executor.getWriteOperation() as FindAndReplaceOperation
        operation.getCriteria() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getReplacement() == new BsonDocument('hot', new BsonBoolean(false))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))
        !operation.isUpsert()
        !operation.isReturnReplaced()

        result == returnedDocument

        when:
        model.upsert(true).returnReplaced(true)
        collection.findOneAndReplace(model)

        then:
        def operation2 = executor.getWriteOperation() as FindAndReplaceOperation
        operation2.isUpsert()
        operation2.isReturnReplaced()
    }

    def 'findOneAndUpdate should use FindOneAndUpdateOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument, returnedDocument])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def model = new FindOneAndUpdateModel(new Document('cold', true), new Document('hot', false))
                .projection(new Document('field', 1))
                .sort(new Document('sort', -1))

        when:
        def result = collection.findOneAndUpdate(model)

        then:
        def operation = executor.getWriteOperation() as FindAndUpdateOperation
        operation.getCriteria() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getUpdate() == new BsonDocument('hot', new BsonBoolean(false))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))
        !operation.isUpsert()
        !operation.isReturnUpdated()

        result == returnedDocument

        when:
        model.upsert(true).returnUpdated(true)
        collection.findOneAndUpdate(model)

        then:
        def operation2 = executor.getWriteOperation() as FindAndUpdateOperation
        operation2.isUpsert()
        operation2.isReturnUpdated()
    }

}
