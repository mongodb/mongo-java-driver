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
import com.mongodb.DuplicateKeyException
import com.mongodb.MongoCommandException
import com.mongodb.MongoException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.bulk.IndexRequest
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

class CreateIndexesOperationSpecification extends OperationFunctionalSpecification {
    def x1 = ['x': 1] as Document
    def field1Index = ['field': 1]
    def field2Index = ['field2': 1]
    def xyIndex = ['x.y': 1]


    def 'should get index names'() {
        when:

        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field1', new BsonInt32(1))),
                                                               new IndexRequest(new BsonDocument('field2', new BsonInt32(-1))),
                                                               new IndexRequest(new BsonDocument('field3', new BsonInt32(1))
                                                                                        .append('field4', new BsonInt32(-1))),
                                                               new IndexRequest(new BsonDocument('field5', new BsonInt32(-1)))
                                                                       .name('customName')
                                                              ])
        then:
        createIndexOperation.indexNames == ['field1_1', 'field2_-1', 'field3_1_field4_-1', 'customName']

    }

    def 'should be able to create a single index'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }

    @Category(Async)
    def 'should be able to create a single index asynchronously'() {

        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        executeAsync(createIndexOperation)

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }


    def 'should be able to create a single index with a BsonInt64'() {
        given:
        def keys = new BsonDocument('field', new BsonInt64(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }

    @Category(Async)
    def 'should be able to create a single index with a BsonInt64 asynchronously'() {

        given:
        def keys = new BsonDocument('field', new BsonInt64(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        executeAsync(createIndexOperation)

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create multiple indexes'() {
        given:
        def keysForFirstIndex = new BsonDocument('field', new BsonInt32(1))
        def keysForSecondIndex = new BsonDocument('field2', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keysForFirstIndex),
                                                                               new IndexRequest(keysForSecondIndex)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [field1Index, field2Index]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create multiple indexes asynchronously'() {
        given:
        def keysForFirstIndex = new BsonDocument('field', new BsonInt32(1))
        def keysForSecondIndex = new BsonDocument('field2', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keysForFirstIndex),
                                                                               new IndexRequest(keysForSecondIndex)])

        when:
        executeAsync(createIndexOperation)

        then:
        getUserCreatedIndexes('key') == [field1Index, field2Index]
    }

    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should fail to create multiple indexes with server that does not support createIndexesCommand'() {
        given:
        def keysForFirstIndex = new BsonDocument('field', new BsonInt32(1))
        def keysForSecondIndex = new BsonDocument('field2', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keysForFirstIndex),
                                                                               new IndexRequest(keysForSecondIndex)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(MongoException)
    }

    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should fail to create multiple indexes with server that does not support createIndexesCommand asynchronously'() {
        given:
        def keysForFirstIndex = new BsonDocument('field', new BsonInt32(1))
        def keysForSecondIndex = new BsonDocument('field2', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keysForFirstIndex),
                                                                               new IndexRequest(keysForSecondIndex)])

        when:
        executeAsync(createIndexOperation)

        then:
        thrown(MongoException)
    }

    def 'should be able to create a single index on a nested field'() {
        given:
        def keys = new BsonDocument('x.y', new BsonInt32(1))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [xyIndex]
    }

    def 'should be able to handle duplicate key errors when indexing'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).unique(true)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(DuplicateKeyException)
    }

    @Category(Async)
    def 'should be able to handle duplicate key errors when indexing asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).unique(true)])

        when:
        executeAsync(createIndexOperation)

        then:
        thrown(DuplicateKeyException)
    }

    @IgnoreIf({ serverVersionAtLeast(asList(3, 0, 0)) })
    def 'should drop duplicates'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('x', new BsonInt32(1)))
                                                                       .unique(true).dropDups(true)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getCollectionHelper().count() == 1
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw when trying to build an invalid index'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(new BsonDocument())])

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(MongoCommandException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    @Category(Async)
    def 'should throw when trying to build an invalid index asynchronously'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(new BsonDocument())])

        when:
        executeAsync(createIndexOperation)

        then:
        thrown(MongoCommandException)
    }

    def 'should be able to create a unique index'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('unique').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        new CreateIndexesOperation(getNamespace(),
                                   [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).unique(true)]).execute(getBinding())

        then:
        getUserCreatedIndexes('unique').size() == 1
    }

    def 'should be able to create a sparse index'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('sparse').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        new CreateIndexesOperation(getNamespace(),
                                   [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).sparse(true)])
                .execute(getBinding())

        then:
        getUserCreatedIndexes('sparse').size() == 1
    }

    def 'should be able to create a TTL indexes'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        new CreateIndexesOperation(getNamespace(),
                                   [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).expireAfter(100, SECONDS)])
                .execute(getBinding())

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 1
        getUserCreatedIndexes('expireAfterSeconds') == [100]
    }

    def 'should be able to create a 2d indexes'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('2d')))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]

        when:
        getCollectionHelper().drop(getNamespace())
        new CreateIndexesOperation(getNamespace(),
                                   [new IndexRequest(new BsonDocument('field', new BsonString('2d'))).bits(2).min(1.0).max(2.0)])
                .execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]
        getUserCreatedIndexes('bits') == [2]
        getUserCreatedIndexes('min') == [1.0]
        getUserCreatedIndexes('max') == [2.0]
    }

    def 'should be able to create a geoHaystack indexes'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('geoHaystack'))
                                                                                        .append('field1', new BsonInt32(1)))
                                                                       .bucketSize(10.0)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [[field: 'geoHaystack', field1: 1]]
        getUserCreatedIndexes('bucketSize') == [10.0]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 4, 0)) })
    def 'should be able to create a 2dSphereIndex'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('2dsphere')))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field' :'2dsphere']]
        if (serverVersionAtLeast(asList(2, 6, 0))) { getUserCreatedIndexes('2dsphereIndexVersion') == [2] }
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create a 2dSphereIndex with version 1'() {
            given:
            def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                                  [new IndexRequest(new BsonDocument('field', new BsonString('2dsphere')))
                                                                           .sphereVersion(1)])

            when:
            createIndexOperation.execute(getBinding())

            then:
            getUserCreatedIndexes('key') == [['field' :'2dsphere']]
            getUserCreatedIndexes('2dsphereIndexVersion') == [1]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 4, 0)) })
    def 'should be able to create a textIndex'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('text')))
                                                                       .defaultLanguage('es')
                                                                       .languageOverride('language')
                                                                       .weights(new BsonDocument('field', new BsonInt32(100)))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes().size() == 1
        getUserCreatedIndexes('weights') == [['field': 100]]
        getUserCreatedIndexes('default_language') == ['es']
        getUserCreatedIndexes('language_override') == ['language']
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 4, 0)) })
    def 'should be able to create a textIndexVersion'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('text')))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes().size() == 1
        if (serverVersionAtLeast(asList(2, 6, 0))) { getUserCreatedIndexes('textIndexVersion') == [2] }
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create a textIndexVersion with version 1'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('field', new BsonString('text')))
                                                                       .textVersion(1)])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('textIndexVersion') == [1]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 0, 0)) })
    def 'should pass through storage engine options'() {
        given:
        def storageEngineOptions = new Document('wiredTiger', new Document('configString', 'block_compressor=zlib'))
                .append('mmapv1', new Document())
        def operation = new CreateIndexesOperation(getNamespace(),
                                                   [new IndexRequest(new BsonDocument('a', new BsonInt32(1)))
                                                            .storageEngine(new BsonDocumentWrapper(storageEngineOptions,
                                                                                                   new DocumentCodec()))])

        when:
        operation.execute(getBinding())

        then:
        getIndex('a_1').get('storageEngine') == storageEngineOptions
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should be able to create a partially filtered index'() {
        given:
        def partialFilterExpression = new Document('a', new Document('$gte', 10))
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))
                         .partialFilterExpression(new BsonDocumentWrapper(partialFilterExpression, new DocumentCodec()))])

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('partialFilterExpression').head() == partialFilterExpression
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 8)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)], new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    Document getIndex(final String indexName) {
        getIndexes().find {
            it -> it.getString('name') == indexName
        }
    }

    def List<Document> getIndexes() {
        def indexes = []
        def cursor = new ListIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next())
        }
        indexes
    }

    def List<Document> getUserCreatedIndexes() {
        getIndexes().findAll { it.key != [_id: 1] }
    }

    def List<Document> getUserCreatedIndexes(String keyname) {
        getUserCreatedIndexes()*.get(keyname).findAll { it != null }
    }

}
