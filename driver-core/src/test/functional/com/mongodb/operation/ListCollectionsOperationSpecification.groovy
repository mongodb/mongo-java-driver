/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation

import category.Async
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.FutureResultCallback
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class ListCollectionsOperationSpecification extends OperationFunctionalSpecification {

    def madeUpDatabase = 'MadeUpDatabase'

    def 'should return empty set if database does not exist'() {
        given:
        def operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec())

        when:
        def cursor = operation.execute(getBinding())

        then:
        !cursor.hasNext()

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }

    @Category(Async)
    def 'should return empty cursor if database does not exist asynchronously'() {
        given:
        def operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec())

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        !callback.get()

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }

    def 'should return collection names if a collection exists'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()
        def names = collections*.get('name')

        then:
        names.containsAll([collectionName, 'collection2'])
        !names.contains(null)
        names.findAll { it.contains('$')}.isEmpty()
    }

    @Category(Async)
    def 'should return collection names if a collection exists asynchronously'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def names = callback.get()*.get('name')

        then:
        names.containsAll([collectionName, 'collection2'])
        !names.contains(null)
        names.findAll { it.contains('$')}.isEmpty()
    }
}
