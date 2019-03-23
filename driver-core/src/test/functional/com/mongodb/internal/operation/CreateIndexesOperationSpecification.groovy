/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation

import com.mongodb.DuplicateKeyException
import com.mongodb.MongoCommandException
import com.mongodb.MongoExecutionTimeoutException
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
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
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
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [field1Index]

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)]).maxTime(30, SECONDS)

        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }

    def 'should be able to create a single index with a BsonInt64'() {
        given:
        def keys = new BsonDocument('field', new BsonInt64(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [field1Index]

        where:
        async << [true, false]
    }

    def 'should be able to create multiple indexes'() {
        given:
        def keysForFirstIndex = new BsonDocument('field', new BsonInt32(1))
        def keysForSecondIndex = new BsonDocument('field2', new BsonInt32(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keysForFirstIndex),
                                                                    new IndexRequest(keysForSecondIndex)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [field1Index, field2Index]

        where:
        async << [true, false]
    }

    def 'should be able to create a single index on a nested field'() {
        given:
        def keys = new BsonDocument('x.y', new BsonInt32(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [xyIndex]

        where:
        async << [true, false]
    }

    def 'should be able to handle duplicate key errors when indexing'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).unique(true)])

        when:
        execute(operation, async)

        then:
        thrown(DuplicateKeyException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionAtLeast(3, 0) })
    def 'should drop duplicates'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).unique(true).dropDups(true)])

        when:
        execute(operation, async)

        then:
        getCollectionHelper().count() == 1

        where:
        async << [true, false]
    }

    def 'should throw when trying to build an invalid index'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(new BsonDocument())])

        when:
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        where:
        async << [true, false]
    }

    def 'should be able to create a unique index'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('unique').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).unique(true)])
        execute(operation, async)

        then:
        getUserCreatedIndexes('unique').size() == 1

        where:
        async << [true, false]
    }

    def 'should be able to create a sparse index'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('sparse').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).sparse(true)])
        execute(operation, async)

        then:
        getUserCreatedIndexes('sparse').size() == 1

        where:
        async << [true, false]
    }

    def 'should be able to create a TTL indexes'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1))).expireAfter(100, SECONDS)])
        execute(operation, async)

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 1
        getUserCreatedIndexes('expireAfterSeconds') == [100]

        where:
        async << [true, false]
    }

    def 'should be able to create a 2d indexes'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('2d')))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]

        when:
        getCollectionHelper().drop(getNamespace())
        operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('2d'))).bits(2).min(1.0).max(2.0)])
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]
        getUserCreatedIndexes('bits') == [2]
        getUserCreatedIndexes('min') == [1.0]
        getUserCreatedIndexes('max') == [2.0]

        where:
        async << [true, false]
    }

    def 'should be able to create a geoHaystack indexes'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('geoHaystack')).append('field1', new BsonInt32(1)))
                         .bucketSize(10.0)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [[field: 'geoHaystack', field1: 1]]
        getUserCreatedIndexes('bucketSize') == [10.0]

        where:
        async << [true, false]
    }

    def 'should be able to create a 2dSphereIndex'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('2dsphere')))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [['field' :'2dsphere']]

        where:
        async << [true, false]
    }

    def 'should be able to create a 2dSphereIndex with version 1'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('2dsphere'))).sphereVersion(1)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key') == [['field' :'2dsphere']]
        getUserCreatedIndexes('2dsphereIndexVersion') == [1]

        where:
        async << [true, false]
    }

    def 'should be able to create a textIndex'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('text')))
                         .defaultLanguage('es')
                         .languageOverride('language')
                         .weights(new BsonDocument('field', new BsonInt32(100)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes().size() == 1
        getUserCreatedIndexes('weights') == [['field': 100]]
        getUserCreatedIndexes('default_language') == ['es']
        getUserCreatedIndexes('language_override') == ['language']

        where:
        async << [true, false]
    }

    def 'should be able to create a textIndexVersion'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('text')))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes().size() == 1

        where:
        async << [true, false]
    }

    def 'should be able to create a textIndexVersion with version 1'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonString('text'))).textVersion(1)])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('textIndexVersion') == [1]

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 0) })
    def 'should pass through storage engine options'() {
        given:
        def storageEngineOptions = new Document('wiredTiger', new Document('configString', 'block_compressor=zlib'))
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('a', new BsonInt32(1)))
                         .storageEngine(new BsonDocumentWrapper(storageEngineOptions, new DocumentCodec()))])

        when:
        execute(operation, async)

        then:
        getIndex('a_1').get('storageEngine') == storageEngineOptions

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should be able to create a partially filtered index'() {
        given:
        def partialFilterExpression = new Document('a', new Document('$gte', 10))
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('field', new BsonInt32(1)))
                         .partialFilterExpression(new BsonDocumentWrapper(partialFilterExpression, new DocumentCodec()))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('partialFilterExpression').head() == partialFilterExpression

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def operation = new CreateIndexesOperation(getNamespace(), [new IndexRequest(keys)], new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(), requests)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        [async, requests] << [
                [true, false],
                [[new IndexRequest(BsonDocument.parse('{field: 1}}')).collation(defaultCollation)],
                 [new IndexRequest(BsonDocument.parse('{field: 1}}')),
                  new IndexRequest(BsonDocument.parse('{field: 2}}')).collation(defaultCollation)]]
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should be able to create an index with collation'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('a', new BsonInt32(1))).collation(defaultCollation)])

        when:
        execute(operation, async)
        def indexCollation = new BsonDocumentWrapper<Document>(getIndex('a_1').get('collation'), new DocumentCodec())
        indexCollation.remove('version')

        then:
        indexCollation == defaultCollation.asDocument()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 1) })
    def 'should be able to create wildcard indexes'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('$**', new BsonInt32(1))),
                 new IndexRequest(new BsonDocument('tags.$**', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key').contains(['$**': 1])
        getUserCreatedIndexes('key').contains(['tags.$**': 1])

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 1) })
    def 'should be able to create wildcard index with projection'() {
        given:
        def operation = new CreateIndexesOperation(getNamespace(),
                [new IndexRequest(new BsonDocument('$**', new BsonInt32(1)))
                        .wildcardProjection(new BsonDocument('a', new BsonInt32(1)).append('b.c', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getUserCreatedIndexes('key').contains(['$**': 1])
        getUserCreatedIndexes('wildcardProjection').contains(['a': 1, 'b.c': 1])

        where:
        async << [true, false]
    }

    Document getIndex(final String indexName) {
        getIndexes().find {
            it -> it.getString('name') == indexName
        }
    }

    List<Document> getIndexes() {
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
