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
import spock.lang.IgnoreIf

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
        operation.getCollation() == null
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
            .collation(defaultCollation)

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
        operation.getCollation() == defaultCollation
    }

    def 'should create a collection'() {
        given:
        assert !collectionNameExists(getCollectionName())

        when:
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
        execute(operation, async)

        then:
        collectionNameExists(getCollectionName())

        where:
        async << [true, false]
    }

    def 'should error when creating a collection that already exists'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
        execute(operation, async)

        when:
        execute(operation, async)

        then:
        thrown(MongoServerException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 0, 0)) })
    def 'should pass through storage engine options'() {
        given:
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .storageEngineOptions(new BsonDocument('wiredTiger',
                                                       new BsonDocument('configString', new BsonString('block_compressor=zlib')))
                                              .append('mmapv1', new BsonDocument()))

        when:
        execute(operation, async)

        then:
        new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec()).execute(getBinding()).next().find {
            it -> it.getString('name').value == getCollectionName()
        }.getDocument('options').getDocument('storageEngine') == operation.storageEngineOptions

        where:
        async << [true, false]
    }

    def 'should set flags for use power of two sizes'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .usePowerOf2Sizes(true)

        when:
        execute(operation, async)

        then:
        new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec()).execute(getBinding()).next().find {
            it -> it.getString('name').value == getCollectionName()
        }.getDocument('options').getNumber('flags').intValue() == 1

        where:
        async << [true, false]
    }

    def 'should create capped collection'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .capped(true)
                .autoIndex(false)
                .maxDocuments(100)
                .sizeInBytes(40 * 1024)

        when:
        execute(operation, async)

        then:
        collectionNameExists(getCollectionName())

        when:
        def stats = new CommandWriteOperation<Document>(getDatabaseName(),
                                                        new BsonDocument('collStats', new BsonString(getCollectionName())),
                                                        new DocumentCodec()).execute(getBinding())
        then:
        stats.getBoolean('capped')
        stats.getInteger('max') == 100
        // Starting in 3.0, the size in bytes moved from storageSize to maxSize
        stats.getInteger('maxSize') == 40 * 1024 || stats.getInteger('storageSize') == 40 * 1024

        where:
        async << [true, false]
    }

    def 'should create collection in respect to the autoIndex option'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .autoIndex(autoIndex)

        when:
        execute(operation, async)

        then:
        new CommandWriteOperation<Document>(getDatabaseName(),
                new BsonDocument('collStats', new BsonString(getCollectionName())),
                new DocumentCodec()).execute(getBinding())
                .getInteger('nindexes') == expectedNumberOfIndexes

        where:
        autoIndex | expectedNumberOfIndexes | async
        true      | 1                       | true
        true      | 1                       | false
        false     | 0                       | true
        false     | 0                       | false
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should allow indexOptionDefaults'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def indexOptionDefaults = BsonDocument.parse('{ storageEngine: { mmapv1 : {} }}')
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .indexOptionDefaults(indexOptionDefaults)

        when:
        execute(operation, async)

        then:
        getCollectionInfo(getCollectionName()).get('options').get('indexOptionDefaults') == indexOptionDefaults

        where:
        async << [true, false]
    }


    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should allow validator'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def validator = BsonDocument.parse('{ level: { $gte : 10 }}')
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName())
                .validator(validator)
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.ERROR)

        when:
        execute(operation, async)

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

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 8)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName(), new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should throw an exception when passing an unsupported collation'() {
        given:
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName()).collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by server version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 10)) })
    def 'should be able to create a collection with a collation'() {
        given:
        def operation = new CreateCollectionOperation(getDatabaseName(), getCollectionName()).collation(defaultCollation)

        when:
        execute(operation, async)
        def collectionCollation = getCollectionInfo(getCollectionName()).get('options').get('collation')

        then:
        defaultCollation.asDocument().each { assert collectionCollation.get(it.key) == it.value }

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
