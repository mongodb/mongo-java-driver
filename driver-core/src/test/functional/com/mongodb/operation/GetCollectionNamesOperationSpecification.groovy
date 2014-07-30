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
import com.mongodb.OperationFunctionalSpecification
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class GetCollectionNamesOperationSpecification extends OperationFunctionalSpecification {

    def 'should return empty set if database does not exist'() {
        given:
        def operation = new GetCollectionNamesOperation('MadeUpDatabase')

        when:
        List<String> names = operation.execute(getBinding())

        then:
        names.isEmpty()
    }

    @Category(Async)
    def 'should return empty set if database does not exist asynchronously'() {
        given:
        def operation = new GetCollectionNamesOperation('MadeUpDatabase')

        when:
        List<String> names = operation.executeAsync(getAsyncBinding()).get()

        then:
        names.isEmpty()
    }

    def 'should return default system.index and collection names if a Collection exists'() {
        given:
        def operation = new GetCollectionNamesOperation(databaseName)
        getCollectionHelper().insertDocuments(new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<String> names = operation.execute(getBinding())

        then:
        names.containsAll(['system.indexes', collectionName])
    }

    @Category(Async)
    def 'should return default system.index and collection names if a Collection exists asynchronously'() {
        given:
        def operation = new GetCollectionNamesOperation(databaseName)
        getCollectionHelper().insertDocuments(new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<String> names = operation.executeAsync(getAsyncBinding()).get()

        then:
        names.containsAll(['system.indexes', collectionName])
    }

}
