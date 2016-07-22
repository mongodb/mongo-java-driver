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
import com.mongodb.MongoServerException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.WriteConcernException
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList

class CreateCollectionOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        when:
        CreateCollectionOperation operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())

        then:
        !operation.isCapped()
        operation.getSizeInBytes() == 0
        operation.isAutoIndex()
        operation.getMaxDocuments() == 0
        operation.isUsePowerOf2Sizes() == null
        operation.getStorageEngineOptions() == null
        operation.getIndexOptionDefaults() == null
        operation.getValidator() == null
        operation.getValidationLevel() == null
        operation.getValidationAction() == null
    }

    def 'should set optional values correctly'(){
        given:
        def storageEngineOptions = BsonDocument.parse('{ mmapv1 : {}}')
        def indexOptionDefaults = BsonDocument.parse('{ storageEngine: { mmapv1 : {} }}')
        def validator = BsonDocument.parse('{ level: { $gte : 10 }}')

        when:
        CreateCollectionOperation operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
            .autoIndex(false)
            .capped(true)
            .sizeInBytes(1000)
            .maxDocuments(1000)
            .usePowerOf2Sizes(true)
            .storageEngineOptions(storageEngineOptions)
            .indexOptionDefaults(indexOptionDefaults)
            .validator(validator)
            .validationLevel(ValidationLevel.MODERATE)
            .validationAction(ValidationAction.WARN)

        then:
        operation.isCapped()
        operation.sizeInBytes == 1000
        !operation.isAutoIndex()
        operation.getMaxDocuments() == 1000
        operation.isUsePowerOf2Sizes() == true
        operation.getStorageEngineOptions() == storageEngineOptions
        operation.getIndexOptionDefaults() == indexOptionDefaults
        operation.getValidator() == validator
        operation.getValidationLevel() == ValidationLevel.MODERATE
        operation.getValidationAction() == ValidationAction.WARN
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

    def 'should create collection in respect to the autoIndex option'() {
        given:
        assert !collectionNameExists(getCollectionName())

        when:
        new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .autoIndex(autoIndex)
                .execute(getBinding())

        then:
        new CommandWriteOperation<Document>(getDatabaseName(),
                new BsonDocument('collStats', new BsonString(getCollectionName())),
                new DocumentCodec()).execute(getBinding())
                .getInteger('nindexes') == expectedNumberOfIndexes

        where:
        autoIndex | expectedNumberOfIndexes
        true | 1
        false | 0
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should allow indexOptionDefaults'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def indexOptionDefaults = BsonDocument.parse('{ storageEngine: { mmapv1 : {} }}')

        when:
        new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .indexOptionDefaults(indexOptionDefaults)
                .execute(getBinding())

        then:
        getCollectionInfo(getCollectionName()).get('options').get('indexOptionDefaults') == indexOptionDefaults
    }


    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should allow validator'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def validator = BsonDocument.parse('{ level: { $gte : 10 }}')

        when:
        new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .validator(validator)
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.ERROR)
                .execute(getBinding())

        then:
        def options = getCollectionInfo(getCollectionName()).get('options')
        options.get('validator') == validator
        options.get('validationLevel') == new BsonString(ValidationLevel.MODERATE.getValue())
        options.get('validationAction') == new BsonString(ValidationAction.ERROR.getValue())

        when:
        getCollectionHelper().insertDocuments(BsonDocument.parse('{ level: 8}'))

        then:
        WriteConcernException writeConcernException = thrown()
        writeConcernException.getErrorCode() == 121
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 8)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName(), new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def getCollectionInfo(String collectionName) {
        new ListCollectionsOperation(databaseName, new BsonDocumentCodec()).filter(new BsonDocument('name',
                new BsonString(collectionName))).execute(getBinding()).tryNext()?.head()
    }

    def collectionNameExists(String collectionName) {
        getCollectionInfo(collectionName) != null
    }
}
