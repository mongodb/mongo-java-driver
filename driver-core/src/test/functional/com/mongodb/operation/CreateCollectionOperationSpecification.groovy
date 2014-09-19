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
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast

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
        new CreateCollectionOperation(getDatabaseName(), getCollectionName()).executeAsync(getAsyncBinding()).get()

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

    @Category(Async)
    def 'should error when creating a collection that already exists asynchronously'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
        operation.execute(getBinding())

        when:
        operation.executeAsync(getAsyncBinding()).get()

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
        new GetCollectionNamesOperation(databaseName).execute(getBinding()).contains(collectionName);
    }

}
