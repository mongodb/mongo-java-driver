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

package org.mongodb.operation
import org.mongodb.Fixture
import org.mongodb.FunctionalSpecification
import org.mongodb.Index

import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getSession
import static org.mongodb.OrderBy.ASC

class CreateIndexesSpecification extends FunctionalSpecification {
    def idIndex = ['_id': 1] 
    def field1Index = ['field': 1]
    def field2Index = ['field2': 1]
    
    def 'should be able to create a single index'() {
        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index], getNamespace())

        when:
        createIndexesOperation.execute(getSession())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to create a single index asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index], getNamespace())

        when:
        createIndexesOperation.executeAsync(getSession()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to create multiple indexes'() {
        given:
        def index1 = Index.builder().addKey('field', ASC).build()
        def index2 = Index.builder().addKey('field2', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index1, index2], getNamespace())


        when:
        createIndexesOperation.execute(getSession())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index, field2Index)
    }

    def 'should be able to create multiple indexes asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def index1 = Index.builder().addKey('field', ASC).build()
        def index2 = Index.builder().addKey('field2', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index1, index2], getNamespace())

        when:
        createIndexesOperation.executeAsync(getSession()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index, field2Index)
    }

    def 'should be able to handle duplicated indexes'() {
        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index, index], getNamespace())

        when:
        createIndexesOperation.execute(getSession())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to handle duplicated indexes asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def index = Index.builder().addKey('field', ASC).build()
        def createIndexesOperation = new CreateIndexesOperation([index, index], getNamespace())

        when:
        createIndexesOperation.executeAsync(getSession()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def getIndexes() {
        new GetIndexesOperation(getNamespace()).execute(getSession())
    }

}
