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
import com.mongodb.DuplicateKeyException
import com.mongodb.MongoServerException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.operation.OrderBy.ASC

class CreateIndexesSpecification extends OperationFunctionalSpecification {
    def idIndex = ['_id': 1]
    def x1 = ['x': 1]
    def field1Index = ['field': 1]
    def field2Index = ['field2': 1]
    def xyIndex = ['x.y': 1]

    def 'should be able to create a single index'() {
        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to create a single index on a nested field'() {
        given:
        def index = Index.builder().addKey('x.y', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, xyIndex)
    }

    @Category(Async)
    def 'should be able to create a single index asynchronously'() {

        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.executeAsync(getAsyncBinding()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to create multiple indexes'() {
        given:
        def index1 = Index.builder().addKey('field', ASC).build()
        def index2 = Index.builder().addKey('field2', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index1, index2])


        when:
        createIndexesOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index, field2Index)
    }

    @Category(Async)
    def 'should be able to create multiple indexes asynchronously'() {

        given:
        def index1 = Index.builder().addKey('field', ASC).build()
        def index2 = Index.builder().addKey('field2', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index1, index2])

        when:
        createIndexesOperation.executeAsync(getAsyncBinding()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index, field2Index)
    }

    // Todo remove once 2.7 has fixed SERVER-14920
    @IgnoreIf({ serverVersionAtLeast([2, 7, 0]) })
    def 'should be able to handle duplicated indexes in the same array'() {
        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index, index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    @Category(Async)
    // Todo remove once 2.7 has fixed SERVER-14920
    @IgnoreIf({ serverVersionAtLeast([2, 7, 0]) })
    def 'should be able to handle duplicated indexes asynchronously in the same array'() {
        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index, index])

        when:
        createIndexesOperation.executeAsync(getAsyncBinding()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to handle duplicate key errors when indexing'() {
        given:
        getCollectionHelper().insertDocuments(x1 as Document, x1 as Document)
        def index = Index.builder().addKey('field', ASC).unique().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        thrown(DuplicateKeyException)
    }

    @Category(Async)
    def 'should be able to handle duplicate key errors when indexing asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(x1 as Document, x1 as Document)
        def index = Index.builder().addKey('field', ASC).unique().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(DuplicateKeyException)
    }

    def 'should throw when trying to build an invalid index'() {
        given:
        def index = Index.builder().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        thrown(MongoServerException)
    }

    @Category(Async)
    def 'should throw when trying to build an invalid index asynchronously'() {
        given:
        def index = Index.builder().build()
        def createIndexesOperation = new CreateIndexesOperation(getNamespace(), [index])

        when:
        createIndexesOperation.execute(getBinding())

        then:
        thrown(MongoServerException)
    }

    def getIndexes() {
        new GetIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
    }

}
