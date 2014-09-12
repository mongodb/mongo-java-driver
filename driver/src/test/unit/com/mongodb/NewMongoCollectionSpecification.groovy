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
import com.mongodb.client.model.DistinctModel
import com.mongodb.client.model.FindModel
import com.mongodb.client.model.FindOneAndRemoveModel
import com.mongodb.client.model.FindOneAndReplaceModel
import com.mongodb.client.model.FindOneAndUpdateModel
import com.mongodb.client.model.InsertManyModel
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.ReplaceOneModel
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
import com.mongodb.operation.QueryOperation
import com.mongodb.operation.ReplaceOperation
import com.mongodb.protocol.AcknowledgedWriteResult
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
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

    def 'insertOne should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteResult(1, false, null)])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        executor.getWriteOperation() as InsertOperation
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
        executor.getWriteOperation() as InsertOperation
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
                                                                 new Document('_id', 1).append('color', 'blue')))

        then:
        executor.getWriteOperation() as ReplaceOperation
        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'find should use FindOperation properly'() {
        given:
        def document = new Document('_id', 1)
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        cursor.next() >> document
        def executor = new TestOperationExecutor([cursor])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new FindModel<>().criteria(new Document('cold', true))
                                     .batchSize(4)
                                     .cursorFlags(EnumSet.of(CursorFlag.PARTIAL, CursorFlag.NO_CURSOR_TIMEOUT))
                                     .maxTime(1, TimeUnit.SECONDS)
                                     .skip(5)
                                     .limit(100)
                                     .modifiers(new Document('$hint', 'i1'))
                                     .projection(new Document('x', 1))
                                     .sort(new Document('y', 1))

        when:
        def result = collection.find(model).into([])

        then:
        def operation = executor.getReadOperation() as QueryOperation
        operation.criteria == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.batchSize == 4
        operation.cursorFlags == EnumSet.of(CursorFlag.PARTIAL, CursorFlag.NO_CURSOR_TIMEOUT)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.skip == 5
        operation.limit == 100
        operation.modifiers == new BsonDocument('$hint', new BsonString('i1'))
        operation.projection == new BsonDocument('x', new BsonInt32(1))
        operation.sort == new BsonDocument('y', new BsonInt32(1))
        executor.readPreference == secondary()
        result == [document]
    }

    def 'count should use CountOperation properly'() {
        given:
        def executor = new TestOperationExecutor([42L])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def model = new CountModel<>().criteria(new Document('cold', true))
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

        def model = new CountModel<>().hintString('idx1')

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

        def model = new DistinctModel<>('fieldName1').criteria(new Document('cold', true))
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

    def 'bulk insert should generate _id'() {
        given:
        def executor = new TestOperationExecutor([new com.mongodb.protocol.AcknowledgedBulkWriteResult(1, 0, 0, 0, [])])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def document = new Document();

        when:
        collection.bulkWrite(new BulkWriteModel<>([new InsertOneModel<>(document)]))

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

        def model = new AggregateModel<>([new Document('$match', new Document('job', 'plumber'))])
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

        def model = new AggregateModel<>([new Document('$match', new Document('job', 'plumber')),
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

    def 'findOneAndRemove should use FindAndRemoveOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument])
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)
        def model = new FindOneAndRemoveModel<>(new Document('cold', true))
                .projection(new Document('field', 1))
                .sort(new Document('sort', -1))

        when:
        def result = collection.findOneAndRemove(model)

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
        def model = new FindOneAndReplaceModel<>(new Document('cold', true), new Document('hot', false))
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
        def model = new FindOneAndUpdateModel<>(new Document('cold', true), new Document('hot', false))
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
