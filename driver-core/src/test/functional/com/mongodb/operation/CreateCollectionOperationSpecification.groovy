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
import com.mongodb.MongoServerException
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList

class CreateCollectionOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        when:
        CreateCollectionOperation operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())

        then:
        !operation.isCapped()
        operation.sizeInBytes == 0
        operation.isAutoIndex()
        operation.getMaxDocuments() == 0
        operation.usePowerOf2Sizes == null
    }

    def 'should set optional values correctly'(){
        when:
        CreateCollectionOperation operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
            .autoIndex(false)
            .capped(true)
            .sizeInBytes(1000)
            .maxDocuments(1000)
            .usePowerOf2Sizes(true)

        then:
        operation.isCapped()
        operation.sizeInBytes == 1000
        !operation.isAutoIndex()
        operation.getMaxDocuments() == 1000
        operation.usePowerOf2Sizes == true
    }

    def 'should create a collection'() {
        given:
        assert !collectionNameExists(getCollectionName())

        when:
        new CreateCollectionOperation(getDatabaseName(), getCollectionName()).execute(getBinding())

        then:
        collectionNameExists(getCollectionName())
    }

    @Category(Async)
    def 'should create a collection asynchronously'() {
        given:
        assert !collectionNameExists(getCollectionName())

        when:
        executeAsync(new CreateCollectionOperation(getDatabaseName(), getCollectionName()))

        then:
        collectionNameExists(getCollectionName())
    }

    def 'should error when creating a collection that already exists'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
        operation.execute(getBinding())

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoServerException)
        !collectionNameExists('nonExistingCollection')
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 0, 0)) })
    def 'should pass through storage engine options'() {
        given:
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .storageEngineOptions(new BsonDocument('wiredTiger',
                                                       new BsonDocument('configString', new BsonString('block_compressor=zlib')))
                                              .append('mmapv1', new BsonDocument()))

        when:
        operation.execute(getBinding())

        then:
        new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec()).execute(getBinding()).next().find {
            it -> it.getString('name').value == getCollectionName()
        }.getDocument('options').getDocument('storageEngine') == operation.storageEngineOptions
    }

    @Category(Async)
    def 'should error when creating a collection that already exists asynchronously'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
        operation.execute(getBinding())

        when:
        executeAsync(operation)

        then:
        thrown(MongoServerException)
        !collectionNameExists('nonExistingCollection')
    }

    def 'should create capped collection'() {
        given:
        assert !collectionNameExists(getCollectionName())

        when:
        new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .capped(true)
                .autoIndex(false)
                .maxDocuments(100)
                .sizeInBytes(40 * 1024)
                .usePowerOf2Sizes(true)
                .execute(getBinding())

        then:
        collectionNameExists(getCollectionName())

        when:
        def stats = new CommandWriteOperation<Document>(getDatabaseName(),
                                                        new BsonDocument('collStats', new BsonString(getCollectionName())),
                                                        new DocumentCodec()).execute(getBinding())
        then:
        stats.getBoolean('capped')
        stats.getInteger('max') == 100
        if (serverVersionAtLeast([2, 4, 0])) {
            stats.getInteger('storageSize') == 40 * 1024
        } else {
            stats.getInteger('storageSize') >= 40 * 1024 && stats.getInteger('storageSize') <= 41 * 1024
        }
    }

    def collectionNameExists(String collectionName) {
        def cursor = new ListCollectionsOperation(databaseName, new DocumentCodec()).execute(getBinding())
        if (!cursor.hasNext()) {
            return false
        }
        cursor.next()*.get('name').contains(collectionName)
    }
}
