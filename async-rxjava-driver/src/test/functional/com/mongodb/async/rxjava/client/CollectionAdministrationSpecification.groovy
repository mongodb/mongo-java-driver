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

package com.mongodb.async.rxjava.client

import org.mongodb.Index

import static Fixture.get
import static Fixture.getAsList
import static Fixture.getMongoClient
import static org.mongodb.OrderBy.ASC

class CollectionAdministrationSpecification extends FunctionalSpecification {

    def idIndex = ['_id': 1]
    def field1Index = ['field': 1]
    def index1 = Index.builder().addKey('field', ASC).build()
    def index2 = Index.builder().addKey('field2', ASC).build()

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
    def 'createIndexes should add indexes to the collection'() {
        when:
        get(collection.tools().createIndexes([index1]))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)
    }

    def 'dropIndex should drop index'() {
        when:
        get(collection.tools().createIndexes([index1]))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)

        when:
        get(collection.tools().dropIndex(index1))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') == [idIndex]
    }

    def 'dropIndexes should drop all indexes apart from _id'() {
        when:
        get(collection.tools().createIndexes([index1, index2]))

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') containsAll(idIndex, field1Index)

        when:
        get(collection.tools().dropIndexes())

        then:
        getAsList(collection.tools().getIndexes()) *.get('key') == [idIndex]
    }

}
