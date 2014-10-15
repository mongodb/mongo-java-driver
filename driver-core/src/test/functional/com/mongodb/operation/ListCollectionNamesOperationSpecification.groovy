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
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class ListCollectionNamesOperationSpecification extends OperationFunctionalSpecification {

    def madeUpDatabase = 'MadeUpDatabase'

    def 'should return empty set if database does not exist'() {
        given:
        def operation = new ListCollectionNamesOperation(madeUpDatabase)

        when:
        List<String> names = operation.execute(getBinding())

        then:
        names.isEmpty()

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }

    @Category(Async)
    def 'should return empty set if database does not exist asynchronously'() {
        given:
        def operation = new ListCollectionNamesOperation(madeUpDatabase)

        when:
        List<String> names = operation.executeAsync(getAsyncBinding()).get()

        then:
        names.isEmpty()

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }

    def 'should return default system.index and collection names if a Collection exists'() {
        given:
        def operation = new ListCollectionNamesOperation(databaseName)
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        List<String> names = operation.execute(getBinding())

        then:
        names.size() == 3
        names.containsAll(['system.indexes', collectionName, 'collection2'])
    }

    @Category(Async)
    def 'should return default system.index and collection names if a Collection exists asynchronously'() {
        given:
        def operation = new ListCollectionNamesOperation(databaseName)
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        List<String> names = operation.executeAsync(getAsyncBinding()).get()

        then:
        names.size() == 3
        names.containsAll(['system.indexes', collectionName, 'collection2'])
    }

}
