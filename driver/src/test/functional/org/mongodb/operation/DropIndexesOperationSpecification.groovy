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
import org.mongodb.Document
import org.mongodb.Fixture
import org.mongodb.FunctionalSpecification
import org.mongodb.Index
import org.mongodb.MongoException

import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getSession
import static org.mongodb.OrderBy.ASC

class DropIndexesOperationSpecification extends FunctionalSpecification {

    def 'should not error when dropping non-existent index on non-existent collection'() {
        given:
        def operation = new DropIndexOperation(getNamespace(), 'made_up_index_1')

        when:
        operation.execute(getSession())

        then:
        getIndexes().size() == 0
    }

    def 'should not error when dropping non-existent index on non-existent collection asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def operation = new DropIndexOperation(getNamespace(), 'made_up_index_1')

        when:
        operation.executeAsync(getSession()).get()

        then:
        getIndexes().size() == 0
    }

    def 'should error when dropping non-existent index on existing collection'() {
        given:
        def operation = new DropIndexOperation(getNamespace(), 'made_up_index_1')
        getCollectionHelper().insertDocuments(new Document('documentThat', 'forces creation of the Collection'))

        when:
        operation.execute(getSession())

        then:
        thrown(MongoException)
    }

    def 'should error when dropping non-existent index  on existing collection asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def operation = new DropIndexOperation(getNamespace(), 'made_up_index_1')
        getCollectionHelper().insertDocuments(new Document('documentThat', 'forces creation of the Collection'))

        when:
        operation.executeAsync(getSession()).get()

        then:
        thrown(MongoException)
    }

    def 'should drop existing index'() {
        given:
        def operation = new DropIndexOperation(getNamespace(), 'theField_1');
        createIndexes(Index.builder().addKey('theField', ASC).build())

        when:
        operation.execute(getSession())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop existing index asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def operation = new DropIndexOperation(getNamespace(), 'theField_1');
        createIndexes(Index.builder().addKey('theField', ASC).build())

        when:
        operation.executeAsync(getSession()).get()
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop all indexes when passed *'() {
        given:
        def operation = new DropIndexOperation(getNamespace(), '*');
        createIndexes(Index.builder().addKey('theField', ASC).build(),
                Index.builder().addKey('theOtherField', ASC).build())

        when:
        operation.execute(getSession())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop all indexes when passed * asynchronously'() {
        assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def operation = new DropIndexOperation(getNamespace(), '*');
        createIndexes(Index.builder().addKey('theField', ASC).build(),
                      Index.builder().addKey('theOtherField', ASC).build())

        when:
        operation.executeAsync(getSession()).get()
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @SuppressWarnings('FactoryMethodName')
    def createIndexes(Index[] indexes) {
        new CreateIndexesOperation(indexes.toList(), getNamespace()).execute(getSession())
    }

    def getIndexes() {
        new GetIndexesOperation(getNamespace()).execute(getSession())
    }

}
