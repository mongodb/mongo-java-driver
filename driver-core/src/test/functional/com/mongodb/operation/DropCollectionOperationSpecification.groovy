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

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class DropCollectionOperationSpecification extends OperationFunctionalSpecification {

    def 'should drop a collection that exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert collectionNameExists(getCollectionName())

        when:
        new DropCollectionOperation(getNamespace()).execute(getBinding())

        then:
        !collectionNameExists(getCollectionName())
    }

    @Category(Async)
    def 'should drop a collection that exists asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert collectionNameExists(getCollectionName())

        when:
        executeAsync(new DropCollectionOperation(getNamespace()))

        then:
        !collectionNameExists(getCollectionName())
    }

    def 'should not error when dropping a collection that does not exist'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'nonExistingCollection')

        when:
        new DropCollectionOperation(namespace).execute(getBinding())

        then:
        !collectionNameExists('nonExistingCollection')
    }

    @Category(Async)
    def 'should not error when dropping a collection that does not exist asynchronously'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'nonExistingCollection')

        when:
        executeAsync(new DropCollectionOperation(namespace))

        then:
        !collectionNameExists('nonExistingCollection')
    }

    def collectionNameExists(String collectionName) {
        def cursor = new ListCollectionsOperation(databaseName, new DocumentCodec()).execute(getBinding())
        if (!cursor.hasNext()) {
            return false
        }
        cursor.next()*.get('name').contains(collectionName)
    }

}
