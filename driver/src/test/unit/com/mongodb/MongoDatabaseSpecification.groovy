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

package com.mongodb

import com.mongodb.client.MongoCollectionOptions
import com.mongodb.client.MongoDatabaseOptions
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.test.Worker
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.GetCollectionNamesOperation
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary

class MongoDatabaseSpecification extends Specification {

    def database;
    def options = MongoDatabaseOptions.builder().codecRegistry(new RootCodecRegistry([new DocumentCodecProvider()]))
                                      .writeConcern(WriteConcern.ACKNOWLEDGED)
                                      .readPreference(primary())
                                      .build()
    def collectionOptions = MongoCollectionOptions.builder().build().withDefaults(options)
    def customCollectionOptions = MongoCollectionOptions.builder()
                                                        .writeConcern(WriteConcern.JOURNALED)
                                                        .readPreference(secondary())
                                                        .build().withDefaults(options)

    def 'should get name'() {
        when:
        database = new MongoDatabaseImpl('name', options, new TestOperationExecutor([]))

        then:
        database.getName() == 'name'
    }

    def 'should get options'() {
        when:
        database = new MongoDatabaseImpl('name', options, new TestOperationExecutor([]))

        then:
        database.getOptions() == options
    }

    def 'should get collection'() {
        given:
        def myCollectionOptions;
        def namespace = new MongoNamespace('databaseName', 'collectionName')
        database = new MongoDatabaseImpl('databaseName', options, new TestOperationExecutor([]))


        when:
        def defaultCollection = database.getCollection('collectionName')
        myCollectionOptions = defaultCollection.getOptions()

        then:
        defaultCollection.getNamespace() == namespace
        myCollectionOptions.getReadPreference() == collectionOptions.getReadPreference()
        myCollectionOptions.getWriteConcern() == collectionOptions.getWriteConcern()

        when:
        def customCollection = database.getCollection('collectionName', customCollectionOptions)
        myCollectionOptions = customCollection.getOptions()

        then:
        customCollection.getNamespace() == namespace
        myCollectionOptions.getReadPreference() == customCollectionOptions.getReadPreference()
        myCollectionOptions.getWriteConcern() == customCollectionOptions.getWriteConcern()

        when:
        def workerCollection = database.getCollection('collectionName', Worker)
        myCollectionOptions = workerCollection.getOptions()

        then:
        workerCollection.getNamespace() == namespace
        myCollectionOptions.getReadPreference() == collectionOptions.getReadPreference()
        myCollectionOptions.getWriteConcern() == collectionOptions.getWriteConcern()
        workerCollection.clazz == Worker

        when:
        def workerCollectionCustomOptions = database.getCollection('collectionName', Worker, customCollectionOptions)
        myCollectionOptions = workerCollectionCustomOptions.getOptions()

        then:
        workerCollectionCustomOptions.getNamespace() == namespace
        myCollectionOptions.getReadPreference() == customCollectionOptions.getReadPreference()
        myCollectionOptions.getWriteConcern() == customCollectionOptions.getWriteConcern()
        workerCollectionCustomOptions.clazz == Worker
    }

    def 'should use DropDatabaseOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null])
        database = new MongoDatabaseImpl('name', options, executor)

        when:
        database.dropDatabase()

        then:
        def operation = executor.getWriteOperation() as DropDatabaseOperation
        operation.databaseName == 'name'
    }

    def 'should use GetCollectionNamesOperation properly'() {
        given:
        def executor = new TestOperationExecutor([['collection1', 'collection2']])
        database = new MongoDatabaseImpl('name', options, executor)

        when:
        def result = database.getCollectionNames()

        then:
        def operation = executor.getReadOperation() as GetCollectionNamesOperation
        operation.databaseName == 'name'
        result == ['collection1', 'collection2']
    }

    def 'should use CreateCollectionOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        database = new MongoDatabaseImpl('databaseName', options, executor)

        when:
        database.createCollection('collectionName')

        then:
        def operation = executor.getWriteOperation() as CreateCollectionOperation
        operation.databaseName == 'databaseName'
        operation.collectionName == 'collectionName'
        operation.isAutoIndex()
        !operation.isCapped()
        operation.isUsePowerOf2Sizes() == null
        operation.getMaxDocuments() == 0
        operation.getSizeInBytes() == 0

        when:
        database.createCollection('collectionName', new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1024)
        )

        then:
        def operation2 = executor.getWriteOperation() as CreateCollectionOperation
        operation2.databaseName == 'databaseName'
        operation2.collectionName == 'collectionName'
        !operation2.isAutoIndex()
        operation2.isCapped()
        operation2.isUsePowerOf2Sizes()
        operation2.getMaxDocuments() == 100
        operation2.getSizeInBytes() == 1024
    }
}
