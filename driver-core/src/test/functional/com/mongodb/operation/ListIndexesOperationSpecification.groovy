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

package com.mongodb.operation

import category.Async
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.bulk.IndexRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListIndexesOperationSpecification extends OperationFunctionalSpecification {

    def 'should return empty list for nonexistent collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        def cursor = operation.execute(getBinding())

        then:
        !cursor.hasNext()
    }

    @Category(Async)
    def 'should return empty list for nonexistent collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        AsyncBatchCursor cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get() == null
    }


    def 'should return default index on Collection that exists'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        BatchCursor<Document> indexes = operation.execute(getBinding())

        then:
        def firstBatch = indexes.next()
        firstBatch.size() == 1
        firstBatch[0].name == '_id_'
        !indexes.hasNext()
    }

    @Category(Async)
    def 'should return default index on Collection that exists asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def indexes = callback.get()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should return created indexes on Collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexesOperation(namespace, [new IndexRequest(new BsonDocument('unique', new BsonInt32(1))).unique(true)])
                .execute(getBinding())

        when:
        BatchCursor cursor = operation.execute(getBinding())

        then:
        def indexes = cursor.next()
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
        !cursor.hasNext()
    }

    @Category(Async)
    def 'should return created indexes on Collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexesOperation(namespace, [new IndexRequest(new BsonDocument('unique', new BsonInt32(1))).unique(true)])
                .execute(getBinding())

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def indexes = callback.get()

        then:
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
    }

    def 'should use the set batchSize of collections'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).batchSize(2)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection2', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection3', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection4', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection5', new BsonInt32(1)))

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()

        then:
        collections.size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.hasNext()
        cursor.getBatchSize() == 2

        when:
        collections = cursor.next()

        then:
        collections.size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.hasNext()
        cursor.getBatchSize() == 2
    }

    @Category(Async)
    def 'should use the set batchSize of collections asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).batchSize(2)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection2', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection3', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection4', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection5', new BsonInt32(1)))

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get().size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.getBatchSize() == 2

        when:
        callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get().size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.getBatchSize() == 2
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).maxTime(1000, MILLISECONDS)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).maxTime(1000, MILLISECONDS)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))

        enableMaxTimeFailPoint()

        when:
        executeAsync(operation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

}
