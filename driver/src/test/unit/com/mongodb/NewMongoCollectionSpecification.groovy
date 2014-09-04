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
import com.mongodb.client.model.CountModel
import com.mongodb.client.model.DistinctModel
import com.mongodb.client.model.FindModel
import com.mongodb.client.model.InsertManyModel
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.codecs.DocumentCodecProvider
import com.mongodb.operation.CountOperation
import com.mongodb.operation.DistinctOperation
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
        def executor = new TestOperationExecutor(new AcknowledgedWriteResult(1, false, null))
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.insertOne(new Document('_id', 1))

        then:
        executor.getWriteOperation() as InsertOperation
        !result.insertedId
        result.insertedCount == 1
    }

    def 'insert should add _id to document'() {
        given:
        def executor = new TestOperationExecutor(new AcknowledgedWriteResult(1, false, null))
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)


        def document = new Document()
        when:
        def result = collection.insertOne(document)

        then:
        document.containsKey('_id')
        document.get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
        !result.insertedId
        result.insertedCount == 1
    }

    def 'insertMany should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor(new AcknowledgedWriteResult(2, false, null))
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.insertMany(new InsertManyModel<Document>([new Document('_id', 1), new Document('_id', 2)]));

        then:
        executor.getWriteOperation() as InsertOperation
        !result.insertedIds
        result.insertedCount == 2
    }

    def 'insertMany should add _id to documents'() {
        given:
        def executor = new TestOperationExecutor(new AcknowledgedWriteResult(2, false, null))
        collection = new NewMongoCollectionImpl<Document>(namespace, Document, options, executor)

        def documents = [new Document(), new Document()]
        when:
        def result = collection.insertMany(new InsertManyModel<Document>(documents));

        then:
        documents[0].containsKey('_id')
        documents[0].get('_id') instanceof ObjectId
        documents[1].containsKey('_id')
        documents[1].get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
        !result.insertedIds
        result.insertedCount == 2
    }

    def 'replace should use ReplaceOperation properly'() {
        given:
        def executor = new TestOperationExecutor(new AcknowledgedWriteResult(1, false, null))
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
        def executor = new TestOperationExecutor(cursor)
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
        def executor = new TestOperationExecutor(42L)
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
        def executor = new TestOperationExecutor(42L)
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
        def executor = new TestOperationExecutor(new BsonArray(Arrays.asList(new BsonString('foo'), new BsonInt32(42))))
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

}
