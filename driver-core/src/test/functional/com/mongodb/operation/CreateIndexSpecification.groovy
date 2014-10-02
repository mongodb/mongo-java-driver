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
import com.mongodb.CommandFailureException
import com.mongodb.DuplicateKeyException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList

class CreateIndexSpecification extends OperationFunctionalSpecification {
    def x1 = ['x': 1] as Document
    def field1Index = ['field': 1]
    def field2Index = ['field2': 1]
    def xyIndex = ['x.y': 1]

    def 'should set its options correctly'() {
        when:
        def operation = new CreateIndexOperation(getNamespace(), new BsonDocument('a', new BsonInt32(1)))

        then:
        operation.getKey() == new BsonDocument('a', new BsonInt32(1))
        !operation.isBackground()
        !operation.isUnique()
        !operation.isSparse()
        operation.getName() == null
        operation.getExpireAfterSeconds() == null
        operation.getVersion() == null
        operation.getWeights() == null
        operation.getDefaultLanguage() == null
        operation.getLanguageOverride() == null
        operation.getTextIndexVersion() == null
        operation.getTwoDSphereIndexVersion() == null
        operation.getBits() == null
        operation.getMin() == null
        operation.getMax() == null
        operation.getBucketSize() == null

        when:
        def operation2 = new CreateIndexOperation(getNamespace(), new BsonDocument('a', new BsonInt32(1)))
                .background(true)
                .unique(true)
                .sparse(true)
                .name('aIndex')
                .expireAfterSeconds(100)
                .version(1)
                .weights(new BsonDocument('a', new BsonInt32(1000)))
                .defaultLanguage('es')
                .languageOverride('language')
                .textIndexVersion(1)
                .twoDSphereIndexVersion(1)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .bucketSize(200.0)

        then:
        operation2.getKey() == new BsonDocument('a', new BsonInt32(1))
        operation2.isBackground()
        operation2.isUnique()
        operation2.isSparse()
        operation2.getName() == 'aIndex'
        operation2.getExpireAfterSeconds() == 100
        operation2.getVersion() == 1
        operation2.getWeights() == new BsonDocument('a', new BsonInt32(1000))
        operation2.getDefaultLanguage() == 'es'
        operation2.getLanguageOverride() == 'language'
        operation2.getTextIndexVersion() == 1
        operation2.getTwoDSphereIndexVersion() == 1
        operation2.getBits() == 1
        operation2.getMin() == -180.0
        operation2.getMax() == 180.0
        operation2.getBucketSize() == 200.0
    }

    def 'should be able to create a single index'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }

    @Category(Async)
    def 'should be able to create a single index asynchronously'() {

        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.executeAsync(getAsyncBinding()).get()

        then:
        getUserCreatedIndexes('key') == [field1Index]
    }

    def 'should be able to create a single index on a nested field'() {
        given:
        def keys = new BsonDocument('x.y', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [xyIndex]
    }

    def 'should be able to handle duplicate key errors when indexing'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1))).unique(true)

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(DuplicateKeyException)
    }

    @Category(Async)
    def 'should be able to handle duplicate key errors when indexing asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1)
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1))).unique(true)

        when:
        createIndexOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(DuplicateKeyException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw when trying to build an invalid index'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument())

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(CommandFailureException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    @Category(Async)
    def 'should throw when trying to build an invalid index asynchronously'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument())

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(CommandFailureException)
    }

    def 'should be able to create a unique index'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1)))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('unique').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        createIndexOperation.unique(true).execute(getBinding())

        then:
        getUserCreatedIndexes('unique').size() == 1
    }

    def 'should be able to create a sparse index'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1)))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('sparse').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        createIndexOperation.sparse(true).execute(getBinding())

        then:
        getUserCreatedIndexes('sparse').size() == 1
    }

    def 'should be able to create a TTL indexes'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1)))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 0

        when:
        getCollectionHelper().drop(getNamespace())
        createIndexOperation.expireAfterSeconds(100).execute(getBinding())

        then:
        getUserCreatedIndexes('expireAfterSeconds').size() == 1
        getUserCreatedIndexes('expireAfterSeconds') == [100]
    }

    def 'should be able to create a 2d indexes'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('2d')))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]

        when:
        getCollectionHelper().drop(getNamespace())
        createIndexOperation.bits(2).min(1.0).max(2.0).execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field': '2d']]
        getUserCreatedIndexes('bits') == [2]
        getUserCreatedIndexes('min') == [1.0]
        getUserCreatedIndexes('max') == [2.0]
    }

    def 'should be able to create a geoHaystack indexes'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('geoHaystack'))
                .append('field1', new BsonInt32(1)))

        when:
        createIndexOperation.bucketSize(10.0).execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [[field: 'geoHaystack', field1: 1]]
        getUserCreatedIndexes('bucketSize') == [10.0]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 4, 0)) })
    def 'should be able to create a 2dSphereIndex'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('2dsphere')))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('key') == [['field' :'2dsphere']]
        getUserCreatedIndexes('2dsphereIndexVersion') == [2]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create a 2dSphereIndex with version 1'() {
            given:
            def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('2dsphere')))
                    .twoDSphereIndexVersion(1)

            when:
            createIndexOperation.execute(getBinding())

            then:
            getUserCreatedIndexes('key') == [['field' :'2dsphere']]
            getUserCreatedIndexes('2dsphereIndexVersion') == [1]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 4, 0)) })
    def 'should be able to create a textIndex'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('text')))
            .defaultLanguage('es')
            .languageOverride('language')
            .weights(new BsonDocument('field', new BsonInt32(100)))

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
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('text')))

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('textIndexVersion') == [2]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to create a textIndexVersion with version 1'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonString('text')))
                .textIndexVersion(1)

        when:
        createIndexOperation.execute(getBinding())

        then:
        getUserCreatedIndexes('textIndexVersion') == [1]
    }

    def List<Document> getIndexes() {
        new GetIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
    }

    def List<Document> getUserCreatedIndexes() {
        getIndexes().findAll { it.key != [_id: 1] }
    }

    def List<Document> getUserCreatedIndexes(String keyname) {
        getUserCreatedIndexes()*.get(keyname).findAll { it != null }
    }

}
