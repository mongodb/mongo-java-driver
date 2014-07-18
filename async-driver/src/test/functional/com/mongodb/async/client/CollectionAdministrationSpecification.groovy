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

package com.mongodb.async.client

import org.mongodb.Index

import static org.mongodb.OrderBy.ASC

class CollectionAdministrationSpecification extends FunctionalSpecification {

    def idIndex = ['_id': 1]
    def field1Index = ['field': 1]
    def index1 = Index.builder().addKey('field', ASC).build()
    def index2 = Index.builder().addKey('field2', ASC).build()

    def 'Drop should drop the collection'() {
        when:
        def client = Fixture.getMongoClient()
        def database = client.getDatabase(databaseName)
        database.tools().createCollection(collectionName).get()

        then:
        database.tools().getCollectionNames().get().contains(collectionName)

        when:
        collection.tools().drop().get()

        then:
        !database.tools().getCollectionNames().get().contains(collectionName)
    }

    def 'getIndexes should not error for a nonexistent collection'() {
        when:
        def database = Fixture.getMongoClient().getDatabase(databaseName)

        then:
        !database.tools().getCollectionNames().get().contains(collectionName)
        collection.tools().getIndexes().get() == []
    }

    @SuppressWarnings(['FactoryMethodName'])
    def 'createIndexes should add indexes to the collection'() {
        when:
        collection.tools().createIndexes([index1]).get()

        then:
        collection.tools().getIndexes().get()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'dropIndex should drop index'() {
        when:
        collection.tools().createIndexes([index1]).get()

        then:
        collection.tools().getIndexes().get()*.get('key') containsAll(idIndex, field1Index)

        when:
        collection.tools().dropIndex(index1).get()

        then:
        collection.tools().getIndexes().get()*.get('key') == [idIndex]
    }

    def 'dropIndexes should drop all indexes apart from _id'() {
        when:
        collection.tools().createIndexes([index1, index2]).get()

        then:
        collection.tools().getIndexes().get()*.get('key') containsAll(idIndex, field1Index)

        when:
        collection.tools().dropIndexes().get()

        then:
        collection.tools().getIndexes().get()*.get('key') == [idIndex]
    }

}
