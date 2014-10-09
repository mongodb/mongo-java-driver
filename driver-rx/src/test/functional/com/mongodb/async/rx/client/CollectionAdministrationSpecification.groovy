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

package com.mongodb.async.rx.client

import com.mongodb.MongoNamespace
import org.bson.Document

import static Fixture.get
import static Fixture.getAsList
import static Fixture.getMongoClient

class CollectionAdministrationSpecification extends FunctionalSpecification {

    def idIndex = ['_id': 1]
    def field1Index = ['field': 1]
    def index1 = ['field': 1] as Document
    def index2 = ['field2': 1] as Document

    def 'Drop should drop the collection'() {
        when:
        def client = getMongoClient()
        def database = client.getDatabase(databaseName)
        get(database.tools().createCollection(collectionName))

        then:
        getAsList(database.tools().getCollectionNames()).contains(collectionName)

        when:
        get(collection.tools().drop())

        then:
        !getAsList(database.tools().getCollectionNames()).contains(collectionName)
    }

    def 'getIndexes should not error for a nonexistent collection'() {
        when:
        def database = getMongoClient().getDatabase(databaseName)

        then:
        !getAsList(database.tools().getCollectionNames()).contains(collectionName)
        getAsList(collection.tools().getIndexes()) == []
    }

    @SuppressWarnings(['FactoryMethodName'])
    def 'createIndex should add indexes to the collection'() {
        when:
        get(collection.tools().createIndex(index1))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)
    }

    def 'dropIndex should drop index'() {
        when:
        get(collection.tools().createIndex(index1))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)

        when:
        get(collection.tools().dropIndex('field_1'))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') == [idIndex]
    }

    def 'dropIndexes should drop all indexes apart from _id'() {
        when:
        get(collection.tools().createIndex(index1))
        get(collection.tools().createIndex(index2))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)

        when:
        get(collection.tools().dropIndexes())

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') == [idIndex]
    }

    def 'rename collection should rename the collection name'() {

        given:
        def newCollectionName = 'NewCollection1234'
        def client = getMongoClient()
        def database = client.getDatabase(databaseName)

        when:
        get(database.tools().createCollection(collectionName))

        then:
        getAsList(database.tools().getCollectionNames()).contains(collectionName)

        when:
        get(collection.tools().renameCollection(new MongoNamespace(databaseName, newCollectionName)))

        then:
        !getAsList(database.tools().getCollectionNames()).contains(collectionName)
        getAsList(database.tools().getCollectionNames()).contains(newCollectionName)
    }

}
