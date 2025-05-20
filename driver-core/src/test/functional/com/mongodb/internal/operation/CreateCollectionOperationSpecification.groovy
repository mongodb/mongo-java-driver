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

import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static java.util.Collections.singletonList

class CreateCollectionOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        when:
        CreateCollectionOperation operation = createOperation()

        then:
        !operation.isCapped()
        operation.getSizeInBytes() == 0
        operation.isAutoIndex()
        operation.getMaxDocuments() == 0
        operation.getStorageEngineOptions() == null
        operation.getIndexOptionDefaults() == null
        operation.getValidator() == null
        operation.getValidationLevel() == null
        operation.getValidationAction() == null
        operation.getCollation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def storageEngineOptions = BsonDocument.parse('{ wiredTiger : {}}')
        def indexOptionDefaults = BsonDocument.parse('{ storageEngine: { wiredTiger : {} }}')
        def validator = BsonDocument.parse('{ level: { $gte : 10 }}')

        when:
        CreateCollectionOperation operation = createOperation()
            .autoIndex(false)
            .capped(true)
            .sizeInBytes(1000)
            .maxDocuments(1000)
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
        def operation = createOperation()
        execute(operation, async)

        then:
        collectionNameExists(getCollectionName())

        where:
        async << [true, false]
    }

    def 'should pass through storage engine options'() {
        given:
        def storageEngineOptions = new BsonDocument('wiredTiger', new BsonDocument('configString', new BsonString('block_compressor=zlib')))
        def operation = createOperation()
                .storageEngineOptions(storageEngineOptions)

        when:
        execute(operation, async)

        then:
        new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec())
                .execute(getBinding()).next().find { it -> it.getString('name').value == getCollectionName() }
                .getDocument('options').getDocument('storageEngine') == operation.storageEngineOptions

        where:
        async << [true, false]
    }

    def 'should pass through storage engine options- zstd compression'() {
        given:
        def storageEngineOptions = new BsonDocument('wiredTiger', new BsonDocument('configString', new BsonString('block_compressor=zstd')))
        def operation = createOperation()
                .storageEngineOptions(storageEngineOptions)

        when:
        execute(operation, async)

        then:
        new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec())
                .execute(getBinding()).next().find { it -> it.getString('name').value == getCollectionName() }
                .getDocument('options').getDocument('storageEngine') == operation.storageEngineOptions
        where:
        async << [true, false]
    }

    def 'should create capped collection'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = createOperation()
                .capped(true)
                .maxDocuments(100)
                .sizeInBytes(40 * 1024)

        when:
        execute(operation, async)

        then:
        collectionNameExists(getCollectionName())

        when:
        def stats = storageStats()

        then:
        stats.getBoolean('capped').getValue()
        stats.getNumber('max').intValue() == 100
        // Starting in 3.0, the size in bytes moved from storageSize to maxSize
        stats.getNumber('maxSize', new BsonInt32(0)).intValue() == 40 * 1024 ||
                stats.getNumber('storageSize', new BsonInt32(0)).intValue() == 40 * 1024

        where:
        async << [true, false]
    }

    def 'should allow indexOptionDefaults'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def indexOptionDefaults = BsonDocument.parse('{ storageEngine: { wiredTiger : {} }}')
        def operation = createOperation()
                .indexOptionDefaults(indexOptionDefaults)

        when:
        execute(operation, async)

        then:
        getCollectionInfo(getCollectionName()).get('options').get('indexOptionDefaults') == indexOptionDefaults

        where:
        async << [true, false]
    }


    def 'should allow validator'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def validator = BsonDocument.parse('{ level: { $gte : 10 }}')
        def operation = createOperation()
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
        MongoBulkWriteException writeConcernException = thrown()
        writeConcernException.getWriteErrors().get(0).getCode() == 121

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        assert !collectionNameExists(getCollectionName())
        def operation = createOperation(new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should be able to create a collection with a collation'() {
        given:
        def operation = createOperation().collation(defaultCollation)

        when:
        execute(operation, async)
        def collectionCollation = getCollectionInfo(getCollectionName()).get('options').get('collation')
        collectionCollation.remove('version')

        then:
        collectionCollation == defaultCollation.asDocument()

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


    BsonDocument storageStats() {
        if (serverVersionLessThan(6, 2)) {
            return new CommandReadOperation<>(getDatabaseName(),
                    new BsonDocument('collStats', new BsonString(getCollectionName())),
                    new BsonDocumentCodec()).execute(getBinding())
        }
        BatchCursor<BsonDocument> cursor = new AggregateOperation(

                getNamespace(),
                singletonList(new BsonDocument('$collStats', new BsonDocument('storageStats', new BsonDocument()))),
                new BsonDocumentCodec()).execute(getBinding())
        try {
            return cursor.next().first().getDocument('storageStats')
        } finally {
            cursor.close()
        }
    }

    def createOperation() {
        createOperation(null)
    }

    def createOperation(WriteConcern writeConcern) {
        new CreateCollectionOperation(getDatabaseName(), getCollectionName(), writeConcern)
    }
}
