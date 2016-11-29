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

package com.mongodb.operation

import category.Async
import com.mongodb.MongoException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast

class DropIndexOperationSpecification extends OperationFunctionalSpecification {

    def 'should not error when dropping non-existent index on non-existent collection'() {
        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').execute(getBinding())

        then:
        getIndexes().size() == 0
    }

    @Category(Async)
    def 'should not error when dropping non-existent index on non-existent collection asynchronously'() {
        when:
        executeAsync(new DropIndexOperation(getNamespace(), 'made_up_index_1'))

        then:
        getIndexes().size() == 0
    }

    def 'should error when dropping non-existent index on existing collection'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').execute(getBinding())

        then:
        thrown(MongoException)
    }

    @Category(Async)
    def 'should error when dropping non-existent index  on existing collection asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        executeAsync(new DropIndexOperation(getNamespace(), 'made_up_index_1'))

        then:
        thrown(MongoException)
    }

    def 'should drop existing index by name'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))

        when:
        new DropIndexOperation(getNamespace(), 'theField_1').execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop existing index by keys'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)

        when:
        new DropIndexOperation(getNamespace(), keys).execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should drop existing index asynchronously'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)
        def operation = new DropIndexOperation(getNamespace(), keys);

        when:
        executeAsync(operation)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }


    def 'should drop existing index by key when using BsonInt64'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)

        when:
        new DropIndexOperation(getNamespace(), new BsonDocument('theField', new BsonInt64(1))).execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should drop existing index by key when using BsonInt64 asynchronously'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)
        def operation = new DropIndexOperation(getNamespace(), new BsonDocument('theField', new BsonInt64(1)));

        when:
        executeAsync(operation)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop all indexes when passed *'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('theOtherField', new BsonInt32(1)))

        when:
        new DropIndexOperation(getNamespace(), '*').execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should drop all indexes when passed * asynchronously'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('theOtherField', new BsonInt32(1)))

        when:
        executeAsync(new DropIndexOperation(getNamespace(), '*'))
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        def operation = new DropIndexOperation(getNamespace(), 'theField_1', new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def getIndexes() {
        def indexes = []
        def cursor = new ListIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next())
        }
        indexes
    }

}
